use anyhow::{Context, bail};
use bytes::Bytes;
use futures::StreamExt;
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde_json::Value;

use crate::config::ProviderCompat;
use crate::llm::StreamEvent;
use crate::provider::UpstreamResponse;

/// OpenAI chat-completions transport — passthrough with compat shaping.
pub struct OpenAiCompat {
    pub name: String,
    pub base_url: String,
    client: reqwest::Client,
    auth: Option<HeaderValue>,
    headers: Vec<(HeaderName, HeaderValue)>,
    compat: ProviderCompat,
}

impl OpenAiCompat {
    pub fn new(
        name: &str,
        base_url: &str,
        key: Option<String>,
        headers: &std::collections::HashMap<String, String>,
        compat: ProviderCompat,
    ) -> anyhow::Result<Self> {
        let auth = key
            .map(|k| {
                HeaderValue::from_str(&format!("Bearer {k}"))
                    .context("provider api_key contains invalid header characters")
            })
            .transpose()?;
        let headers = headers
            .iter()
            .map(|(k, v)| {
                Ok((
                    HeaderName::from_bytes(k.as_bytes())
                        .with_context(|| format!("invalid header name '{k}'"))?,
                    HeaderValue::from_str(v)
                        .with_context(|| format!("invalid header value for '{k}'"))?,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            name: name.to_string(),
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
            auth,
            headers,
            compat,
        })
    }

    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let req = match &self.auth {
            Some(a) => req.header(AUTHORIZATION, a.clone()),
            None => req,
        };
        self.headers
            .iter()
            .fold(req, |r, (k, v)| r.header(k.clone(), v.clone()))
    }

    pub async fn forward(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Bytes,
    ) -> anyhow::Result<UpstreamResponse> {
        // Compat shaping applies to chat-completions payloads only.
        let shaped: Option<Value> =
            if method == reqwest::Method::POST && path == "/chat/completions" {
                serde_json::from_slice::<Value>(&body).ok().map(|mut v| {
                    if self.compat.inband_tools {
                        crate::provider::inband::render_tools(&mut v);
                    }
                    crate::provider::compat::apply(&mut v, &self.compat);
                    v
                })
            } else {
                None
            };
        let body = match &shaped {
            Some(v) => Bytes::from(v.to_string()),
            None => body,
        };
        let url = format!("{}{}", self.base_url, path);
        let send = |body: Bytes| {
            let method = method.clone();
            let url = url.clone();
            async move {
                self.authed(self.client.request(method, &url))
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await
            }
        };
        let mut resp = send(body.clone()).await.context("upstream request failed")?;

        // Strict-tools fallback: a 400 mentioning "strict" on a request that
        // carried strict tools retries once without the field.
        if resp.status() == reqwest::StatusCode::BAD_REQUEST {
            if let Some(v) = &shaped {
                let has_strict = v["tools"].as_array().is_some_and(|tools| {
                    tools.iter().any(|t| t["strict"].as_bool() == Some(true))
                });
                if has_strict {
                    let status = resp.status();
                    let err = resp.text().await.unwrap_or_default();
                    if err.contains("strict") {
                        let mut retry = v.clone();
                        if let Some(tools) = retry["tools"].as_array_mut() {
                            for t in tools.iter_mut() {
                                t.as_object_mut().map(|o| o.remove("strict"));
                            }
                        }
                        resp = send(Bytes::from(retry.to_string()))
                            .await
                            .context("upstream request failed")?;
                    } else {
                        // Rebuild a response carrying the buffered error body.
                        return Ok(UpstreamResponse {
                            status,
                            content_type: "application/json".into(),
                            stream: Box::pin(futures::stream::once(async move {
                                Ok(Bytes::from(err))
                            })),
                        });
                    }
                }
            }
        }

        Ok(UpstreamResponse {
            status: resp.status(),
            content_type: resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string(),
            stream: Box::pin(
                resp.bytes_stream()
                    .map(|chunk| chunk.map_err(std::io::Error::other)),
            ),
        })
    }

    pub async fn chat_stream(
        &self,
        body: Value,
    ) -> anyhow::Result<
        Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send + Unpin>,
    > {
        let mut body = body;
        body["stream"] = Value::Bool(true);
        let up = self
            .forward(
                reqwest::Method::POST,
                "/chat/completions",
                Bytes::from(body.to_string()),
            )
            .await?;
        if !up.status.is_success() {
            let buf = crate::provider::collect_stream(up.stream).await?;
            bail!("upstream {}: {}", up.status, String::from_utf8_lossy(&buf));
        }
        // In-band tools: buffer the whole stream so <tool_call> blocks can be
        // extracted from the complete text before emitting events.
        if self.compat.inband_tools {
            let events: Vec<StreamEvent> = sse_events(up.stream)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<anyhow::Result<_>>()?;
            return Ok(Box::new(futures::stream::iter(
                crate::provider::inband::events_with_tool_calls(events)
                    .into_iter()
                    .map(Ok),
            )));
        }
        Ok(Box::new(sse_events(up.stream)))
    }

    pub async fn chat(&self, mut body: Value) -> anyhow::Result<Value> {
        body["stream"] = Value::Bool(false);
        let up = self
            .forward(
                reqwest::Method::POST,
                "/chat/completions",
                Bytes::from(body.to_string()),
            )
            .await?;
        let buf = crate::provider::collect_stream(up.stream).await?;
        if !up.status.is_success() {
            bail!("upstream {}: {}", up.status, String::from_utf8_lossy(&buf));
        }
        let mut v: Value = serde_json::from_slice(&buf)?;
        if self.compat.inband_tools {
            crate::provider::inband::response_with_tool_calls(&mut v);
        }
        Ok(v)
    }

    pub async fn list_models(&self) -> anyhow::Result<Value> {
        let up = self
            .forward(reqwest::Method::GET, "/models", Bytes::new())
            .await?;
        let buf = crate::provider::collect_stream(up.stream).await?;
        Ok(serde_json::from_slice(&buf)?)
    }
}

/// Parse an SSE byte stream into normalized StreamEvents. Shared by
/// adapters that emit OpenAI-shaped SSE (openai-compat) — others translate.
///
/// Bytes are buffered raw and decoded only at `\n` boundaries, so a
/// multi-byte UTF-8 char split across TCP chunks is never lossy-decoded.
/// `data:` lines accumulate until the blank-line event boundary (SSE
/// permits multi-line data, joined with `\n`).
pub fn sse_events(
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>>,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send>> {
    // finish carries the last-seen finish_reason into the terminal Done.
    // pending queues extra events from a multi-tool-call chunk.
    Box::pin(futures::stream::unfold(
        (
            stream,
            Vec::<u8>::new(),
            Vec::<String>::new(),
            std::collections::VecDeque::<anyhow::Result<StreamEvent>>::new(),
            false,
            crate::llm::StopReason::Other,
        ),
        |(mut stream, mut buf, mut data_lines, mut pending, mut done, mut finish)| async move {
            // Drain queued events from a multi-event chunk first.
            if let Some(res) = pending.pop_front() {
                if matches!(res, Ok(StreamEvent::Done(_))) {
                    done = true;
                }
                return Some((res, (stream, buf, data_lines, pending, done, finish)));
            }
            if done {
                return None;
            }
            loop {
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    // buf[..pos] is a complete UTF-8 boundary: \n (0x0A)
                    // never appears inside a multi-byte sequence.
                    let line = String::from_utf8_lossy(&buf[..pos])
                        .trim_end_matches('\r')
                        .to_string();
                    buf.drain(..=pos);
                    if line.is_empty() {
                        match flush_event(&mut data_lines, &mut finish) {
                            Some(Ok(evs)) => {
                                let mut it = evs.into_iter();
                                if let Some(first) = it.next() {
                                    for ev in it {
                                        pending.push_back(Ok(ev));
                                    }
                                    if matches!(first, StreamEvent::Done(_)) {
                                        done = true;
                                    }
                                    return Some((
                                        Ok(first),
                                        (stream, buf, data_lines, pending, done, finish),
                                    ));
                                }
                            }
                            Some(Err(e)) => {
                                return Some((
                                    Err(e),
                                    (stream, buf, data_lines, pending, done, finish),
                                ))
                            }
                            None => {}
                        }
                        continue;
                    }
                    if let Some(data) = line.strip_prefix("data:") {
                        data_lines.push(data.trim().to_string());
                    }
                    continue;
                }
                match stream.next().await {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(e)) => {
                        done = true;
                        return Some((
                            Err(e.into()),
                            (stream, buf, data_lines, pending, done, finish),
                        ));
                    }
                    None => {
                        // Flush a trailing partial line, then any pending
                        // event, then emit Done.
                        if !buf.is_empty() {
                            let line = String::from_utf8_lossy(&buf)
                                .trim_end_matches('\r')
                                .to_string();
                            buf.clear();
                            if let Some(data) = line.strip_prefix("data:") {
                                data_lines.push(data.trim().to_string());
                            }
                        }
                        match flush_event(&mut data_lines, &mut finish) {
                            Some(Ok(evs)) => {
                                let mut it = evs.into_iter();
                                if let Some(first) = it.next() {
                                    for ev in it {
                                        pending.push_back(Ok(ev));
                                    }
                                    if matches!(first, StreamEvent::Done(_)) {
                                        done = true;
                                    }
                                    return Some((
                                        Ok(first),
                                        (stream, buf, data_lines, pending, done, finish),
                                    ));
                                }
                            }
                            Some(Err(e)) => {
                                return Some((
                                    Err(e),
                                    (stream, buf, data_lines, pending, done, finish),
                                ))
                            }
                            None => {}
                        }
                        done = true;
                        return Some((
                            Ok(StreamEvent::Done(finish)),
                            (stream, buf, data_lines, pending, done, finish),
                        ));
                    }
                }
            }
        },
    ))
}

/// Flush one complete SSE event (the accumulated `data:` lines joined
/// with `\n`). `Some(Ok(evs))` = emit these events; `None` = absorbed
/// (finish_reason update or a skippable chunk).
fn flush_event(
    data_lines: &mut Vec<String>,
    finish: &mut crate::llm::StopReason,
) -> Option<anyhow::Result<Vec<StreamEvent>>> {
    let data = data_lines.join("\n");
    data_lines.clear();
    if data.is_empty() {
        return None;
    }
    if data == "[DONE]" {
        return Some(Ok(vec![StreamEvent::Done(*finish)]));
    }
    match parse_chunk(&data) {
        Ok(parsed) => {
            if let Some(r) = parsed.finish {
                *finish = r;
            }
            if parsed.events.is_empty() {
                None
            } else {
                Some(Ok(parsed.events))
            }
        }
        Err(e) => Some(Err(e)),
    }
}

/// What a parsed SSE chunk produced: events plus an optional
/// finish_reason update (a chunk can carry both usage and a finish).
#[derive(Default)]
struct Parsed {
    events: Vec<StreamEvent>,
    finish: Option<crate::llm::StopReason>,
}

fn parse_chunk(data: &str) -> anyhow::Result<Parsed> {
    use crate::llm::StopReason;
    let v: Value = serde_json::from_str(data).context("invalid SSE JSON")?;
    let mut out = Parsed::default();

    // Usage may ride on the same chunk as content/finish — collect it
    // without dropping the rest.
    if let Some(u) = v.get("usage").filter(|u| u.is_object()) {
        out.events.push(StreamEvent::Usage {
            input: u["prompt_tokens"].as_u64().unwrap_or(0),
            output: u["completion_tokens"].as_u64().unwrap_or(0),
        });
    }

    let Some(choice) = v["choices"].get(0) else {
        return Ok(out);
    };
    let delta = &choice["delta"];

    // Reasoning deltas (DeepSeek-R1, OpenRouter, z.ai).
    for field in ["reasoning_content", "reasoning", "reasoning_text"] {
        if let Some(t) = delta[field].as_str() {
            if !t.is_empty() {
                out.events.push(StreamEvent::Thinking(t.to_string()));
            }
        }
    }

    if let Some(text) = delta["content"].as_str() {
        if !text.is_empty() {
            out.events.push(StreamEvent::Text(text.to_string()));
        }
    }
    if let Some(calls) = delta["tool_calls"].as_array() {
        for call in calls {
            let index = call["index"].as_u64().unwrap_or(0) as usize;
            let f = &call["function"];
            out.events.push(StreamEvent::ToolCallDelta {
                index,
                id: call["id"].as_str().map(String::from),
                name: f["name"].as_str().map(String::from),
                arguments: f["arguments"].as_str().unwrap_or("").to_string(),
            });
        }
    }

    // finish_reason rides on the last content chunk — possibly alongside
    // usage and trailing deltas.
    if let Some(reason) = choice["finish_reason"].as_str() {
        out.finish = Some(match reason {
            "stop" => StopReason::Stop,
            "length" | "max_tokens" => StopReason::Length,
            "tool_calls" | "function_call" => StopReason::ToolCalls,
            _ => StopReason::Other,
        });
    }
    Ok(out)
}

