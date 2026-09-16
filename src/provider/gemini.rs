use anyhow::{Context, bail};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::llm::StreamEvent;

/// Gemini generateContent adapter. Translates OpenAI-shaped requests to
/// Gemini format and streams back normalized events.
///
/// Gemini has no tool-call ids — synthetic ids `call_N` are generated.
/// Tool names are mangled `server.tool` → `server__tool` (Gemini requires
/// [a-zA-Z0-9_-]).
pub struct Gemini {
    pub name: String,
    base_url: String,
    client: reqwest::Client,
    key: Option<String>,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
}

impl Gemini {
    pub fn new(
        name: &str,
        base_url: &str,
        key: Option<String>,
        headers: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let headers = headers
            .iter()
            .map(|(k, v)| {
                Ok((
                    reqwest::header::HeaderName::from_bytes(k.as_bytes())
                        .with_context(|| format!("invalid header name '{k}'"))?,
                    reqwest::header::HeaderValue::from_str(v)
                        .with_context(|| format!("invalid header value for '{k}'"))?,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            name: name.to_string(),
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
            key,
            headers,
        })
    }

    fn url(&self, model: &str, stream: bool) -> String {
        let method = if stream {
            "streamGenerateContent?alt=sse"
        } else {
            "generateContent"
        };
        format!("{}/v1beta/models/{model}:{method}", self.base_url)
    }

    fn translate_request(&self, body: &Value) -> anyhow::Result<Value> {
        let mut contents = Vec::new();
        let mut system = Vec::new();

        // OpenAI tool messages carry only tool_call_id — map it back to
        // the function name from the assistant turn's tool_calls, or
        // Gemini rejects the functionResponse for a name mismatch.
        let mut call_names: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for m in body["messages"].as_array().cloned().unwrap_or_default() {
            match m["role"].as_str() {
                Some("system") => {
                    if let Some(t) = m["content"].as_str() {
                        system.push(json!({"text": t}));
                    }
                }
                Some("user") => contents.push(json!({
                    "role": "user",
                    "parts": [{"text": super::content_text(&m["content"])}],
                })),
                Some("assistant") => {
                    let mut parts = Vec::new();
                    let t = super::content_text(&m["content"]);
                    if !t.is_empty() {
                        parts.push(json!({"text": t}));
                    }
                    for tc in m["tool_calls"].as_array().cloned().unwrap_or_default() {
                        let f = &tc["function"];
                        let name = f["name"].as_str().unwrap_or("");
                        if let Some(id) = tc["id"].as_str() {
                            call_names.insert(id.to_string(), name.to_string());
                        }
                        parts.push(json!({
                            "functionCall": {
                                "name": mangle(name),
                                "args": serde_json::from_str::<Value>(
                                    f["arguments"].as_str().unwrap_or("{}")
                                ).unwrap_or(json!({})),
                            }
                        }));
                    }
                    contents.push(json!({"role": "model", "parts": parts}));
                }
                Some("tool") => {
                    let name = m["tool_call_id"]
                        .as_str()
                        .and_then(|id| call_names.get(id).cloned())
                        .or_else(|| m["name"].as_str().map(String::from))
                        .unwrap_or_else(|| "tool".to_string());
                    contents.push(json!({
                        "role": "user",
                        "parts": [{
                            "functionResponse": {
                                "name": mangle(&name),
                                "response": {"result": super::content_text(&m["content"])},
                            }
                        }],
                    }));
                }
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
                    "name": mangle(f["name"].as_str().unwrap_or("")),
                    "description": f["description"].as_str().unwrap_or(""),
                    "parameters": f["parameters"].clone(),
                })
            })
            .collect();

        let mut out = json!({"contents": contents});
        if !system.is_empty() {
            out["systemInstruction"] = json!({"parts": system});
        }
        if !tools.is_empty() {
            out["tools"] = json!([{"functionDeclarations": tools}]);
        }
        // Thinking level → generationConfig.thinkingConfig.thinkingBudget.
        if let Some(level) = body["_thinking"].as_str() {
            let budget = match level {
                "low" => 1024,
                "medium" => 8192,
                "high" => 24576,
                _ => 0,
            };
            if budget > 0 {
                out["generationConfig"] = json!({
                    "thinkingConfig": {"thinkingBudget": budget}
                });
            }
        }
        Ok(out)
    }

    pub async fn chat_stream(
        &self,
        body: Value,
    ) -> anyhow::Result<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send + Unpin>>
    {
        let model = body["model"].as_str().context("missing model")?.to_string();
        let req = self.translate_request(&body)?;
        let mut r = self
            .client
            .post(self.url(&model, true))
            .header("content-type", "application/json")
            .json(&req);
        if let Some(k) = &self.key {
            r = r.header("x-goog-api-key", k);
        }
        for (k, v) in &self.headers {
            r = r.header(k.clone(), v.clone());
        }
        let resp = r.send().await.context("upstream failed")?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!("upstream: {text}");
        }
        let byte_stream = resp
            .bytes_stream()
            .map(|c| c.map_err(std::io::Error::other));
        Ok(Box::new(gemini_events(Box::pin(byte_stream))))
    }

    pub async fn chat(&self, body: Value) -> anyhow::Result<Value> {
        let model = body["model"].as_str().context("missing model")?.to_string();
        let req = self.translate_request(&body)?;
        let mut r = self
            .client
            .post(self.url(&model, false))
            .header("content-type", "application/json")
            .json(&req);
        if let Some(k) = &self.key {
            r = r.header("x-goog-api-key", k);
        }
        for (k, v) in &self.headers {
            r = r.header(k.clone(), v.clone());
        }
        let resp = r.send().await.context("upstream failed")?;
        let status = resp.status();
        let v: Value = resp.json().await?;
        if !status.is_success() {
            bail!("upstream {status}: {v}");
        }
        Ok(gemini_to_openai(&v))
    }
}

fn mangle(name: &str) -> String {
    name.replace('.', "__")
}

fn unmangle(name: &str) -> String {
    name.replacen("__", ".", 1)
}

/// Gemini response JSON → OpenAI-shaped response.
fn gemini_to_openai(v: &Value) -> Value {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for (i, part) in v["candidates"][0]["content"]["parts"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .enumerate()
    {
        if let Some(t) = part["text"].as_str() {
            text.push_str(t);
        }
        if let Some(fc) = part.get("functionCall") {
            tool_calls.push(json!({
                "id": format!("call_{i}"),
                "type": "function",
                "function": {
                    "name": unmangle(fc["name"].as_str().unwrap_or("")),
                    "arguments": fc["args"].to_string(),
                }
            }));
        }
    }
    let mut msg = json!({"role": "assistant", "content": text});
    if !tool_calls.is_empty() {
        msg["tool_calls"] = json!(tool_calls);
    }
    json!({
        "choices": [{"index": 0, "message": msg, "finish_reason": "stop"}],
        "usage": v["usageMetadata"],
    })
}

/// Parse Gemini SSE (streamGenerateContent?alt=sse) into normalized events.
fn gemini_events(
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send>>,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send>> {
    use crate::llm::StopReason;
    use std::collections::VecDeque;
    // Events parsed from one chunk queue up; we drain one per poll.
    Box::pin(futures::stream::unfold(
        (stream, Vec::<u8>::new(), false, 0usize, VecDeque::new()),
        |(mut stream, mut buf, mut done, mut call_idx, mut queue)| async move {
            loop {
                if let Some(ev) = queue.pop_front() {
                    return Some((Ok(ev), (stream, buf, done, call_idx, queue)));
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
                    let Some(data) = line.strip_prefix("data:") else {
                        continue;
                    };
                    let Ok(v) = serde_json::from_str::<Value>(data.trim()) else {
                        continue;
                    };
                    // Each SSE data is a full GenerateContentResponse chunk.
                    for part in v["candidates"][0]["content"]["parts"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default()
                    {
                        if let Some(t) = part["text"].as_str() {
                            // Gemini marks reasoning parts with thought:true.
                            if part["thought"].as_bool() == Some(true) {
                                queue.push_back(StreamEvent::Thinking(t.to_string()));
                            } else {
                                queue.push_back(StreamEvent::Text(t.to_string()));
                            }
                        }
                        if let Some(fc) = part.get("functionCall") {
                            queue.push_back(StreamEvent::ToolCallDelta {
                                index: call_idx,
                                id: Some(format!("call_{call_idx}")),
                                name: Some(unmangle(fc["name"].as_str().unwrap_or(""))),
                                arguments: fc["args"].to_string(),
                            });
                            call_idx += 1;
                        }
                    }
                    if let Some(r) = v["candidates"][0]["finishReason"].as_str() {
                        // usageMetadata rides every chunk as a running
                        // total — emit only the final counts.
                        if let Some(u) = v.get("usageMetadata").filter(|u| u.is_object()) {
                            queue.push_back(StreamEvent::Usage {
                                input: u["promptTokenCount"].as_u64().unwrap_or(0),
                                output: u["candidatesTokenCount"].as_u64().unwrap_or(0),
                            });
                        }
                        // A response that emitted function calls ends the
                        // turn for tool use, not a plain stop.
                        let reason = match r {
                            "STOP" if call_idx > 0 => StopReason::ToolCalls,
                            "STOP" => StopReason::Stop,
                            "MAX_TOKENS" => StopReason::Length,
                            _ => StopReason::Other,
                        };
                        queue.push_back(StreamEvent::Done(reason));
                        done = true;
                    }
                    continue;
                }
                match stream.next().await {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(e)) => {
                        done = true;
                        return Some((Err(e.into()), (stream, buf, done, call_idx, queue)));
                    }
                    None => {
                        done = true;
                        return Some((
                            Ok(StreamEvent::Done(StopReason::Other)),
                            (stream, buf, done, call_idx, queue),
                        ));
                    }
                }
            }
        },
    ))
}
