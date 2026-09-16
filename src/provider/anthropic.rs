use anyhow::{Context, bail};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::{Value, json};

use crate::llm::StreamEvent;

/// Anthropic Messages API adapter. Translates OpenAI-shaped requests to
/// Anthropic format and streams Anthropic SSE back as normalized events.
///
/// Tool names: MCP namespaced names (`server.tool`) are mangled to
/// `server__tool` on the way out (Anthropic forbids dots) and unmangled
/// in streamed tool calls.
pub struct Anthropic {
    pub name: String,
    base_url: String,
    client: reqwest::Client,
    /// Static API key, or None when OAuth-backed.
    key: Option<String>,
    /// When true, resolve the key from the OAuth keychain on each request
    /// (auto-refreshing) instead of using `key`.
    oauth: bool,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
}

impl Anthropic {
    pub fn new(
        name: &str,
        base_url: &str,
        key: Option<String>,
        headers: &std::collections::HashMap<String, String>,
        oauth: bool,
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
            oauth,
            headers,
        })
    }

    /// Resolve the credential: OAuth token (auto-refreshing) or static key.
    async fn credential(&self) -> anyhow::Result<Option<String>> {
        if self.oauth {
            return crate::oauth::access_token("anthropic").await.map(Some);
        }
        Ok(self.key.clone())
    }

    fn request(&self, body: Value, key: Option<&str>) -> reqwest::RequestBuilder {
        let mut req = self
            .client
            .post(format!("{}/v1/messages", self.base_url))
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body);
        if let Some(k) = key {
            req = req.header("x-api-key", k);
        }
        if self.oauth {
            req = req
                .header("anthropic-beta", "oauth-2025-04-20")
                .header("user-agent", "damond");
        }
        for (k, v) in &self.headers {
            req = req.header(k.clone(), v.clone());
        }
        req
    }

    /// OpenAI messages/tools → Anthropic request body.
    fn translate_request(&self, body: &Value, stream: bool) -> anyhow::Result<Value> {
        let model = body["model"].as_str().context("missing model")?;
        let mut system = Vec::new();
        let mut messages = Vec::new();

        for m in body["messages"].as_array().cloned().unwrap_or_default() {
            match m["role"].as_str() {
                Some("system") => {
                    if let Some(t) = m["content"].as_str() {
                        system.push(t.to_string());
                    }
                }
                Some("user") => messages.push(json!({
                    "role": "user",
                    "content": [{"type": "text", "text": m["content"].as_str().unwrap_or("")}],
                })),
                Some("assistant") => {
                    let mut content = Vec::new();
                    if let Some(t) = m["content"].as_str() {
                        if !t.is_empty() {
                            content.push(json!({"type": "text", "text": t}));
                        }
                    }
                    for tc in m["tool_calls"].as_array().cloned().unwrap_or_default() {
                        let f = &tc["function"];
                        content.push(json!({
                            "type": "tool_use",
                            "id": tc["id"],
                            "name": mangle(&f["name"].as_str().unwrap_or("")),
                            "input": serde_json::from_str::<Value>(
                                f["arguments"].as_str().unwrap_or("{}")
                            ).unwrap_or(json!({})),
                        }));
                    }
                    messages.push(json!({"role": "assistant", "content": content}));
                }
                Some("tool") => {
                    messages.push(json!({
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": m["tool_call_id"],
                            "content": m["content"].as_str().unwrap_or(""),
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
                    "input_schema": f["parameters"].clone(),
                })
            })
            .collect();

        let mut out = json!({
            "model": model,
            "max_tokens": body["max_tokens"].as_u64().unwrap_or(8192),
            "messages": messages,
            "stream": stream,
        });

        // Thinking level: `model:low|medium|high` → thinking.budget_tokens.
        // max_tokens must exceed the budget.
        if let Some(level) = body["_thinking"].as_str() {
            let budget = match level {
                "low" => 1024,
                "medium" => 8192,
                "high" => 32768,
                _ => 0,
            };
            if budget > 0 {
                out["thinking"] = json!({"type": "enabled", "budget_tokens": budget});
                let mt = out["max_tokens"].as_u64().unwrap_or(8192);
                if mt <= budget {
                    out["max_tokens"] = json!(budget + 8192);
                }
            }
        }

        // Prompt caching: mark the last system block and the last message's
        // last content block as ephemeral breakpoints (max 4 allowed).
        if !system.is_empty() {
            let mut blocks: Vec<Value> = system
                .iter()
                .map(|t| json!({"type": "text", "text": t}))
                .collect();
            if let Some(last) = blocks.last_mut() {
                last["cache_control"] = json!({"type": "ephemeral"});
            }
            out["system"] = json!(blocks);
        }
        if let Some(last_msg) = out["messages"].as_array_mut().and_then(|m| m.last_mut()) {
            if let Some(blocks) = last_msg["content"].as_array_mut() {
                if let Some(last) = blocks.last_mut() {
                    last["cache_control"] = json!({"type": "ephemeral"});
                }
            }
        }

        if !tools.is_empty() {
            out["tools"] = json!(tools);
        }
        Ok(out)
    }

    pub async fn chat_stream(
        &self,
        body: Value,
    ) -> anyhow::Result<
        Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send + Unpin>,
    > {
        let req = self.translate_request(&body, true)?;
        let key = self.credential().await?;
        let resp = self.request(req.clone(), key.as_deref()).send().await.context("upstream failed")?;
        // OAuth: a 401 may mean the token expired between requests — force
        // a refresh and retry once.
        let resp = if resp.status() == reqwest::StatusCode::UNAUTHORIZED && self.oauth {
            let key = crate::oauth::access_token("anthropic").await?;
            self.request(req, Some(&key)).send().await.context("upstream failed")?
        } else {
            resp
        };
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!("upstream: {text}");
        }
        let byte_stream = resp
            .bytes_stream()
            .map(|c| c.map_err(std::io::Error::other));
        Ok(Box::new(anthropic_events(Box::pin(byte_stream))))
    }

    pub async fn chat(&self, body: Value) -> anyhow::Result<Value> {
        let req = self.translate_request(&body, false)?;
        let key = self.credential().await?;
        let resp = self.request(req.clone(), key.as_deref()).send().await.context("upstream failed")?;
        let resp = if resp.status() == reqwest::StatusCode::UNAUTHORIZED && self.oauth {
            let key = crate::oauth::access_token("anthropic").await?;
            self.request(req, Some(&key)).send().await.context("upstream failed")?
        } else {
            resp
        };
        let status = resp.status();
        let v: Value = resp.json().await?;
        if !status.is_success() {
            bail!("upstream {status}: {v}");
        }
        // Anthropic response → OpenAI shape.
        let mut text = String::new();
        let mut tool_calls = Vec::new();
        for block in v["content"].as_array().cloned().unwrap_or_default() {
            match block["type"].as_str() {
                Some("text") => text.push_str(block["text"].as_str().unwrap_or("")),
                Some("tool_use") => tool_calls.push(json!({
                    "id": block["id"],
                    "type": "function",
                    "function": {
                        "name": unmangle(block["name"].as_str().unwrap_or("")),
                        "arguments": block["input"].to_string(),
                    }
                })),
                _ => {}
            }
        }
        let mut msg = json!({"role": "assistant", "content": text});
        if !tool_calls.is_empty() {
            msg["tool_calls"] = json!(tool_calls);
        }
        Ok(json!({
            "choices": [{"index": 0, "message": msg, "finish_reason": "stop"}],
            "usage": v["usage"],
        }))
    }
}

/// `server.tool` → `server__tool` (Anthropic forbids dots in tool names).
fn mangle(name: &str) -> String {
    name.replace('.', "__")
}

fn unmangle(name: &str) -> String {
    name.replacen("__", ".", 1)
}

/// Parse Anthropic SSE into normalized StreamEvents.
/// Events: content_block_start (text|tool_use), content_block_delta
/// (text_delta|input_json_delta), message_stop.
fn anthropic_events(
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>>,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send>> {
    use crate::llm::StopReason;
    // finish carries message_delta.stop_reason into the terminal Done.
    Box::pin(futures::stream::unfold(
        (stream, String::new(), false, String::new(), 0usize, StopReason::Other),
        |(mut stream, mut buf, mut done, mut cur_tool, mut tool_index, mut finish)| async move {
            if done {
                return None;
            }
            loop {
                if let Some(pos) = buf.find('\n') {
                    let line = buf[..pos].trim_end_matches('\r').to_string();
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
                    let ev = match v["type"].as_str() {
                        Some("message_start") => {
                            let u = &v["message"]["usage"];
                            if u.is_object() {
                                Some(StreamEvent::Usage {
                                    input: u["input_tokens"].as_u64().unwrap_or(0),
                                    output: u["output_tokens"].as_u64().unwrap_or(0),
                                })
                            } else {
                                None
                            }
                        }
                        Some("content_block_start") => {
                            let block = &v["content_block"];
                            if block["type"] == "tool_use" {
                                cur_tool = block["id"].as_str().unwrap_or("").to_string();
                                tool_index = v["index"].as_u64().unwrap_or(0) as usize;
                                Some(StreamEvent::ToolCallDelta {
                                    index: tool_index,
                                    id: Some(cur_tool.clone()),
                                    name: Some(unmangle(
                                        block["name"].as_str().unwrap_or(""),
                                    )),
                                    arguments: String::new(),
                                })
                            } else {
                                None
                            }
                        }
                        Some("content_block_delta") => {
                            let d = &v["delta"];
                            match d["type"].as_str() {
                                Some("text_delta") => Some(StreamEvent::Text(
                                    d["text"].as_str().unwrap_or("").to_string(),
                                )),
                                Some("thinking_delta") => Some(StreamEvent::Thinking(
                                    d["thinking"].as_str().unwrap_or("").to_string(),
                                )),
                                Some("input_json_delta") => Some(StreamEvent::ToolCallDelta {
                                    index: tool_index,
                                    id: None,
                                    name: None,
                                    arguments: d["partial_json"]
                                        .as_str()
                                        .unwrap_or("")
                                        .to_string(),
                                }),
                                _ => None,
                            }
                        }
                        Some("message_delta") => {
                            if let Some(r) = v["delta"]["stop_reason"].as_str() {
                                finish = match r {
                                    "end_turn" | "stop_sequence" => StopReason::Stop,
                                    "max_tokens" => StopReason::Length,
                                    "tool_use" => StopReason::ToolCalls,
                                    _ => StopReason::Other,
                                };
                            }
                            // message_delta also carries final output_tokens.
                            let u = &v["usage"];
                            if u.is_object() {
                                Some(StreamEvent::Usage {
                                    input: u["input_tokens"].as_u64().unwrap_or(0),
                                    output: u["output_tokens"].as_u64().unwrap_or(0),
                                })
                            } else {
                                None
                            }
                        }
                        Some("message_stop") => {
                            done = true;
                            Some(StreamEvent::Done(finish))
                        }
                        _ => None,
                    };
                    if let Some(ev) = ev {
                        return Some((
                            Ok(ev),
                            (stream, buf, done, cur_tool, tool_index, finish),
                        ));
                    }
                    continue;
                }
                match stream.next().await {
                    Some(Ok(chunk)) => buf.push_str(&String::from_utf8_lossy(&chunk)),
                    Some(Err(e)) => {
                        done = true;
                        return Some((
                            Err(e.into()),
                            (stream, buf, done, cur_tool, tool_index, finish),
                        ));
                    }
                    None => {
                        done = true;
                        return Some((
                            Ok(StreamEvent::Done(finish)),
                            (stream, buf, done, cur_tool, tool_index, finish),
                        ));
                    }
                }
            }
        },
    ))
}
