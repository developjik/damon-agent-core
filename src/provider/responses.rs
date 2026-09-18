//! OpenAI Responses API transport (`/responses`). Used by o-series, GPT-5,
//! Codex, xAI and other responses-only endpoints. Translates the OpenAI
//! chat-completions shape to/from the responses shape.

use anyhow::{Context, bail};
use bytes::Bytes;
use futures::StreamExt;
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde_json::{Value, json};

use crate::config::ProviderCompat;
use crate::llm::StreamEvent;

pub struct OpenAiResponses {
    pub name: String,
    pub base_url: String,
    client: reqwest::Client,
    auth: Option<HeaderValue>,
    headers: Vec<(HeaderName, HeaderValue)>,
    compat: ProviderCompat,
}

impl OpenAiResponses {
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
            client: super::http_client(),
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

    /// Chat-completions request → responses request.
    fn translate_request(&self, body: &Value, stream: bool) -> anyhow::Result<Value> {
        let model = body["model"].as_str().context("missing model")?;
        let mut instructions = Vec::new();
        let mut input = Vec::new();

        for m in body["messages"].as_array().cloned().unwrap_or_default() {
            match m["role"].as_str() {
                Some("system") | Some("developer") => {
                    let t = super::content_text(&m["content"]);
                    if !t.is_empty() {
                        instructions.push(t);
                    }
                }
                Some("user") => input.push(json!({
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": super::content_text(&m["content"])}],
                })),
                Some("assistant") => {
                    let t = super::content_text(&m["content"]);
                    if !t.is_empty() {
                        input.push(json!({
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": t}],
                        }));
                    }
                    for tc in m["tool_calls"].as_array().cloned().unwrap_or_default() {
                        let f = &tc["function"];
                        input.push(json!({
                            "type": "function_call",
                            "call_id": tc["id"],
                            "name": f["name"],
                            "arguments": f["arguments"].as_str().unwrap_or("{}"),
                        }));
                    }
                }
                Some("tool") => input.push(json!({
                    "type": "function_call_output",
                    "call_id": m["tool_call_id"],
                    "output": super::content_text(&m["content"]),
                })),
                _ => {}
            }
        }

        let tools: Vec<Value> = body["tools"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|t| {
                let f = &t["function"];
                json!({
                    "type": "function",
                    "name": f["name"],
                    "description": f["description"],
                    "parameters": f["parameters"],
                })
            })
            .collect();

        let mut out = json!({
            "model": model,
            "input": input,
            "stream": stream,
        });
        if !instructions.is_empty() {
            out["instructions"] = json!(instructions.join("\n"));
        }
        if !tools.is_empty() {
            out["tools"] = json!(tools);
        }
        // Parameter renames / passthroughs.
        for (from, to) in [
            ("max_tokens", "max_output_tokens"),
            ("max_completion_tokens", "max_output_tokens"),
            ("temperature", "temperature"),
            ("top_p", "top_p"),
        ] {
            if let Some(v) = body.get(from) {
                out[to] = v.clone();
            }
        }
        // tool_choice: strings pass through; {type:"function",function:{name}}
        // flattens to {type:"function",name}.
        match &body["tool_choice"] {
            Value::String(s) => out["tool_choice"] = json!(s),
            Value::Object(_) if body["tool_choice"]["type"].as_str() == Some("function") => {
                out["tool_choice"] = json!({
                    "type": "function",
                    "name": body["tool_choice"]["function"]["name"],
                });
            }
            _ => {}
        }
        // response_format → text.format (structured output).
        match body["response_format"]["type"].as_str() {
            Some("json_object") => {
                out["text"] = json!({"format": {"type": "json_object"}});
            }
            Some("json_schema") => {
                let s = &body["response_format"]["json_schema"];
                out["text"] = json!({"format": {
                    "type": "json_schema",
                    "name": s["name"].as_str().unwrap_or("response"),
                    "schema": s["schema"],
                    "strict": s["strict"],
                }});
            }
            Some("text") => {}
            _ => {}
        }
        // Direct passthroughs the Responses API accepts verbatim.
        for key in [
            "parallel_tool_calls",
            "user",
            "metadata",
            "include",
            "service_tier",
        ] {
            if let Some(v) = body.get(key) {
                out[key] = v.clone();
            }
        }
        // Client-sent reasoning/text/truncation merge — a caller's
        // reasoning config must not be silently replaced by _thinking.
        for key in ["reasoning", "text", "truncation"] {
            if let Some(v) = body.get(key).filter(|v| v.is_object()) {
                for (k, val) in v.as_object().unwrap() {
                    out[key][k] = val.clone();
                }
            }
        }
        // Thinking level → reasoning.effort (merges into any client
        // reasoning object set above).
        if let Some(level) = body["_thinking"].as_str() {
            out["reasoning"]["effort"] = json!(level);
        }
        // Compat: store + extra_body.
        if self.compat.supports_store {
            out["store"] = Value::Bool(false);
        }
        for (k, v) in &self.compat.extra_body {
            out[k] = v.clone();
        }
        Ok(out)
    }

    async fn send(&self, body: Value) -> anyhow::Result<reqwest::Response> {
        let resp = self
            .authed(self.client.post(format!("{}/responses", self.base_url)))
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .context("upstream request failed")?;
        Ok(resp)
    }

    pub async fn chat_stream(
        &self,
        body: Value,
    ) -> anyhow::Result<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send + Unpin>>
    {
        let req = self.translate_request(&body, true)?;
        let resp = self.send(req).await?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(crate::provider::RateLimited(crate::provider::retry_after(&resp)).into());
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            bail!("upstream {status}: {text}");
        }
        let byte_stream = resp
            .bytes_stream()
            .map(|c| c.map_err(std::io::Error::other));
        Ok(Box::new(responses_events(Box::pin(byte_stream))))
    }

    pub async fn chat(&self, body: Value) -> anyhow::Result<Value> {
        let req = self.translate_request(&body, false)?;
        let resp = self.send(req).await?;
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(crate::provider::RateLimited(crate::provider::retry_after(&resp)).into());
        }
        // Read the body as text first: a gateway 502/504 answers HTML,
        // and json() failing before the status check would lose both.
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("upstream {status}: {text}");
        }
        let v: Value = serde_json::from_str(&text).context("invalid upstream JSON")?;
        responses_to_openai(&v)
    }

    pub async fn list_models(&self) -> anyhow::Result<Value> {
        let resp = self
            .authed(self.client.get(format!("{}/models", self.base_url)))
            .send()
            .await
            .context("upstream request failed")?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("upstream {status}: {text}");
        }
        let v: Value = serde_json::from_str(&text).context("invalid upstream JSON")?;
        Ok(v)
    }
}

/// Responses JSON → OpenAI chat-completions response. `status` is
/// checked first: a `failed` response must surface as an error and an
/// `incomplete` one as finish_reason "length" — returning them as a
/// clean 200 with empty content is indistinguishable from a real
/// empty reply.
fn responses_to_openai(v: &Value) -> anyhow::Result<Value> {
    if v["status"].as_str() == Some("failed") {
        let msg = v["error"]["message"]
            .as_str()
            .unwrap_or("upstream response failed");
        anyhow::bail!("upstream response failed: {msg}");
    }
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for item in v["output"].as_array().cloned().unwrap_or_default() {
        match item["type"].as_str() {
            Some("message") => {
                for c in item["content"].as_array().cloned().unwrap_or_default() {
                    if c["type"].as_str() == Some("output_text") {
                        text.push_str(c["text"].as_str().unwrap_or(""));
                    }
                }
            }
            Some("function_call") => tool_calls.push(json!({
                "id": item["call_id"],
                "type": "function",
                "function": {
                    "name": item["name"],
                    "arguments": item["arguments"].as_str().unwrap_or("{}"),
                }
            })),
            _ => {}
        }
    }
    let mut msg = json!({"role": "assistant", "content": text});
    if !tool_calls.is_empty() {
        msg["tool_calls"] = json!(tool_calls);
    }
    // "incomplete" (max_output_tokens truncation, content filter) maps
    // to "length" — the streaming path already maps it the same way.
    let finish = if v["status"].as_str() == Some("incomplete") {
        "length"
    } else if tool_calls.is_empty() {
        "stop"
    } else {
        "tool_calls"
    };
    let usage = json!({
        "prompt_tokens": v["usage"]["input_tokens"],
        "completion_tokens": v["usage"]["output_tokens"],
        "total_tokens": v["usage"]["total_tokens"],
    });
    Ok(json!({
        "choices": [{"index": 0, "message": msg, "finish_reason": finish}],
        "usage": usage,
    }))
}

/// Parse Responses SSE into normalized StreamEvents.
/// Events: response.output_text.delta, response.output_item.added
/// (function_call), response.function_call_arguments.delta,
/// response.completed/failed/incomplete.
fn responses_events(
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>>,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send>> {
    // pending holds a Usage event parsed alongside a terminal event so both
    // get emitted.
    Box::pin(futures::stream::unfold(
        (
            stream,
            Vec::<u8>::new(),
            false,
            None::<StreamEvent>,
            // output_index → dense tool-call index. Output items include
            // reasoning and message entries, so raw output_index is sparse
            // and would leave empty accumulator slots.
            std::collections::HashMap::<u64, usize>::new(),
        ),
        |(mut stream, mut buf, mut done, mut pending, mut index_map)| async move {
            loop {
                if let Some(ev) = pending.take() {
                    return Some((Ok(ev), (stream, buf, done, pending, index_map)));
                }
                if done {
                    return None;
                }
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    // buf[..pos] is a complete UTF-8 boundary: \n (0x0A)
                    // never appears inside a multi-byte sequence.
                    let line = String::from_utf8_lossy(&buf[..pos])
                        .trim_end_matches('\r')
                        .to_string();
                    buf.drain(..=pos);
                    if line.is_empty() {
                        continue;
                    }
                    if let Some(data) = line.strip_prefix("data:") {
                        let data = data.trim();
                        if data == "[DONE]" {
                            done = true;
                            return Some((
                                Ok(StreamEvent::Done(crate::llm::StopReason::Other)),
                                (stream, buf, done, pending, index_map),
                            ));
                        }
                        match parse_event(data) {
                            Ok(Some(mut ev)) => {
                                // Remap sparse output_index to a dense
                                // tool-call index — reasoning/message items
                                // share the output index space.
                                if let StreamEvent::ToolCallDelta { index, .. } = &mut ev {
                                    let next = index_map.len();
                                    *index = *index_map.entry(*index as u64).or_insert(next);
                                }
                                // Terminal events may carry usage — emit it
                                // first, then the Done on the next poll.
                                if let StreamEvent::Done(_) = ev {
                                    done = true;
                                    if let Some(u) = usage_of(data) {
                                        pending = Some(ev);
                                        return Some((
                                            Ok(u),
                                            (stream, buf, done, pending, index_map),
                                        ));
                                    }
                                }
                                return Some((Ok(ev), (stream, buf, done, pending, index_map)));
                            }
                            Ok(None) => continue,
                            Err(e) => {
                                // A fatal event (response.failed, error)
                                // ends the stream — continuing would emit
                                // a second truncation error at EOF.
                                done = true;
                                return Some((Err(e), (stream, buf, done, pending, index_map)));
                            }
                        }
                    }
                    continue;
                }
                match stream.next().await {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(e)) => {
                        done = true;
                        return Some((Err(e.into()), (stream, buf, done, pending, index_map)));
                    }
                    None => {
                        done = true;
                        // Reaching EOF without response.completed/failed/
                        // incomplete means the stream was truncated —
                        // surface an error so the caller can retry, not a
                        // clean Done that persists partial text.
                        return Some((
                            Err(anyhow::anyhow!("stream ended without a terminal event")),
                            (stream, buf, done, pending, index_map),
                        ));
                    }
                }
            }
        },
    ))
}

/// Extract usage from a terminal response.* event payload.
fn usage_of(data: &str) -> Option<StreamEvent> {
    let v: Value = serde_json::from_str(data).ok()?;
    let u = &v["response"]["usage"];
    if !u.is_object() {
        return None;
    }
    Some(StreamEvent::Usage {
        input: u["input_tokens"].as_u64().unwrap_or(0),
        output: u["output_tokens"].as_u64().unwrap_or(0),
    })
}

fn parse_event(data: &str) -> anyhow::Result<Option<StreamEvent>> {
    use crate::llm::StopReason;
    let v: Value = serde_json::from_str(data).context("invalid SSE JSON")?;
    match v["type"].as_str() {
        Some("response.output_text.delta") => {
            let d = v["delta"].as_str().unwrap_or("");
            if d.is_empty() {
                return Ok(None);
            }
            Ok(Some(StreamEvent::Text(d.to_string())))
        }
        // Reasoning summary + raw reasoning deltas → Thinking.
        Some("response.reasoning_summary_text.delta") | Some("response.reasoning_text.delta") => {
            let d = v["delta"].as_str().unwrap_or("");
            if d.is_empty() {
                return Ok(None);
            }
            Ok(Some(StreamEvent::Thinking(d.to_string())))
        }
        Some("response.output_item.added") => {
            let item = &v["item"];
            if item["type"].as_str() == Some("function_call") {
                // Some providers ship complete arguments on the added
                // event — the delta events then never come, so use them.
                return Ok(Some(StreamEvent::ToolCallDelta {
                    index: v["output_index"].as_u64().unwrap_or(0) as usize,
                    id: item["call_id"].as_str().map(String::from),
                    name: item["name"].as_str().map(String::from),
                    arguments: item["arguments"].as_str().unwrap_or("").to_string(),
                }));
            }
            Ok(None)
        }
        Some("response.function_call_arguments.delta") => Ok(Some(StreamEvent::ToolCallDelta {
            index: v["output_index"].as_u64().unwrap_or(0) as usize,
            id: None,
            name: None,
            arguments: v["delta"].as_str().unwrap_or("").to_string(),
        })),
        Some("response.completed") | Some("response.failed") | Some("response.incomplete") => {
            let r = &v["response"];
            // A failed response carries the real error — surface it
            // instead of ending the turn as if it completed.
            if r["status"].as_str() == Some("failed") {
                let msg = r["error"]["message"].as_str().unwrap_or("response failed");
                anyhow::bail!("responses api: {msg}");
            }
            let reason = match r["status"].as_str() {
                Some("completed") => {
                    // completed with function calls → tool_calls
                    let has_calls = r["output"].as_array().is_some_and(|o| {
                        o.iter()
                            .any(|i| i["type"].as_str() == Some("function_call"))
                    });
                    if has_calls {
                        StopReason::ToolCalls
                    } else {
                        StopReason::Stop
                    }
                }
                Some("incomplete") => StopReason::Length,
                _ => StopReason::Other,
            };
            Ok(Some(StreamEvent::Done(reason)))
        }
        // Mid-stream error event — surface it instead of waiting for an
        // EOF that now reports truncation without the real message.
        Some("error") => {
            let msg = v["message"].as_str().unwrap_or("stream error");
            anyhow::bail!("responses api: {msg}");
        }
        _ => Ok(None),
    }
}
