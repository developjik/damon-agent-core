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
    /// Send `Authorization: Bearer <key>` instead of `x-api-key` — AWS
    /// Bedrock bearer surfaces and Bearer-fronting gateways.
    bearer: bool,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
}

impl Anthropic {
    pub fn new(
        name: &str,
        base_url: &str,
        key: Option<String>,
        headers: &std::collections::HashMap<String, String>,
        oauth: bool,
        bearer: bool,
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
            client: super::http_client(),
            key,
            oauth,
            bearer,
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
            req = if self.bearer {
                req.header(reqwest::header::AUTHORIZATION, format!("Bearer {k}"))
            } else {
                req.header("x-api-key", k)
            };
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

    /// OpenAI messages/tools → Anthropic request body. Also returns the
    /// request's mangled→original tool-name map: `mangle` rewrites every
    /// `.`, so a one-shot `replacen` cannot restore multi-dot names —
    /// responses must be unmangled through this map.
    fn translate_request(
        &self,
        body: &Value,
        stream: bool,
    ) -> anyhow::Result<(Value, std::collections::HashMap<String, String>)> {
        let model = body["model"].as_str().context("missing model")?;
        let mut system = Vec::new();
        let mut messages = Vec::new();
        let mut names = std::collections::HashMap::new();
        // Tools register their wire names BEFORE the message history is
        // walked: a replayed tool_use must resolve to the same name the
        // declaration sent, so collision variants are assigned from the
        // declaration set first, not by history order.
        let tools: Vec<Value> = body["tools"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|t| {
                let f = &t["function"];
                let orig = f["name"].as_str().unwrap_or("");
                let mangled = super::mangled_for(&mut names, orig);
                json!({
                    "name": mangled,
                    "description": f["description"].as_str().unwrap_or(""),
                    "input_schema": f["parameters"].clone(),
                })
            })
            .collect();

        // Set when an assistant tool-use turn arrives without its
        // thinking blocks (OpenAI-wire clients can't carry them).
        let mut tool_turn_missing_thinking = false;
        // OpenAI histories store one role:"tool" row per call, and /v1
        // clients may send adjacent user/assistant rows. Anthropic
        // rejects consecutive same-role messages with a 400, so fold
        // each run of user/tool rows into ONE user message — tool_result
        // blocks leading, as the API requires — and each run of
        // assistant rows into a single assistant message.
        fn flush(
            messages: &mut Vec<Value>,
            role: &str,
            results: &mut Vec<Value>,
            others: &mut Vec<Value>,
        ) {
            if role.is_empty() {
                return;
            }
            let mut content = std::mem::take(results);
            content.append(others);
            // Anthropic 400s on an empty content array — a folded
            // message with no surviving blocks is dropped entirely.
            if !content.is_empty() {
                messages.push(json!({ "role": role, "content": content }));
            }
        }
        let mut cur_role = "";
        let mut cur_results: Vec<Value> = Vec::new();
        let mut cur_others: Vec<Value> = Vec::new();
        for m in body["messages"].as_array().cloned().unwrap_or_default() {
            match m["role"].as_str() {
                Some("system") => {
                    let t = super::content_text(&m["content"]);
                    if !t.is_empty() {
                        system.push(t);
                    }
                }
                Some("user" | "tool") => {
                    if cur_role != "user" {
                        flush(&mut messages, cur_role, &mut cur_results, &mut cur_others);
                        cur_role = "user";
                    }
                    if m["role"].as_str() == Some("tool") {
                        cur_results.push(json!({
                            "type": "tool_result",
                            "tool_use_id": m["tool_call_id"],
                            "content": super::content_text(&m["content"]),
                        }));
                    } else {
                        // Anthropic 400s on empty text blocks — a user
                        // message with no text (or only non-text parts)
                        // must not emit one.
                        let t = super::content_text(&m["content"]);
                        if !t.is_empty() {
                            cur_others.push(json!({
                                "type": "text",
                                "text": t,
                            }));
                        }
                    }
                }
                Some("assistant") => {
                    let mut content = Vec::new();
                    let thinking_blocks = m["thinking"].as_array().cloned().unwrap_or_default();
                    let tool_calls = m["tool_calls"].as_array().cloned().unwrap_or_default();
                    // Anthropic demands thinking blocks replayed before
                    // tool_use when thinking is enabled. OpenAI-wire
                    // clients (/v1) cannot echo them back — their history
                    // would 400 every later turn, so drop thinking for
                    // such requests instead.
                    if !tool_calls.is_empty() && thinking_blocks.is_empty() {
                        tool_turn_missing_thinking = true;
                    }
                    // Replay persisted thinking blocks first — Anthropic
                    // requires them verbatim (signature included) when
                    // thinking is enabled and the turn used tools.
                    for b in thinking_blocks {
                        content.push(b);
                    }
                    let t = super::content_text(&m["content"]);
                    if !t.is_empty() {
                        content.push(json!({"type": "text", "text": t}));
                    }
                    for tc in tool_calls {
                        let f = &tc["function"];
                        let orig = f["name"].as_str().unwrap_or("");
                        let mangled = super::mangled_for(&mut names, orig);
                        content.push(json!({
                            "type": "tool_use",
                            "id": tc["id"],
                            "name": mangled,
                            "input": match &f["arguments"] {
                                // arguments may arrive as a JSON string
                                // (OpenAI wire) or already an object.
                                Value::String(s) => serde_json::from_str::<Value>(s)
                                    .unwrap_or(json!({})),
                                other => other.clone(),
                            },
                        }));
                    }
                    if cur_role != "assistant" {
                        flush(&mut messages, cur_role, &mut cur_results, &mut cur_others);
                        cur_role = "assistant";
                    }
                    cur_others.append(&mut content);
                }
                _ => {}
            }
        }
        flush(&mut messages, cur_role, &mut cur_results, &mut cur_others);

        let mut out = json!({
            "model": model,
            "max_tokens": body["max_tokens"].as_u64().unwrap_or(8192),
            "messages": messages,
            "stream": stream,
        });

        // Sampling parameters pass through under Anthropic names.
        for key in ["temperature", "top_p", "top_k"] {
            if !body[key].is_null() {
                out[key] = body[key].clone();
            }
        }
        if let Some(stop) = body
            .get("stop_sequences")
            .or_else(|| body.get("stop"))
            .filter(|v| !v.is_null())
        {
            // OpenAI allows a bare-string stop; Anthropic's
            // stop_sequences is always an array.
            out["stop_sequences"] = match stop.as_str() {
                Some(s) => json!([s]),
                None => stop.clone(),
            };
        }
        // OpenAI tool_choice → Anthropic tool_choice. A function pick
        // names a mangled tool, so record it in the name map too.
        match &body["tool_choice"] {
            Value::String(s) => {
                let t = match s.as_str() {
                    "auto" => Some("auto"),
                    "required" => Some("any"),
                    "none" => Some("none"),
                    _ => None,
                };
                if let Some(t) = t {
                    out["tool_choice"] = json!({"type": t});
                }
            }
            Value::Object(_) if body["tool_choice"]["type"].as_str() == Some("function") => {
                let orig = body["tool_choice"]["function"]["name"]
                    .as_str()
                    .unwrap_or("");
                let mangled = super::mangled_for(&mut names, orig);
                out["tool_choice"] = json!({"type": "tool", "name": mangled});
            }
            _ => {}
        }
        if let Some(user) = body["user"].as_str() {
            out["metadata"] = json!({"user_id": user});
        }

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

        // A tool replay that can't carry thinking blocks would make
        // every follow-up turn 400 — drop thinking for the whole
        // request so the tool loop keeps working (thinking resumes on
        // requests whose history is faithfully replayable).
        if tool_turn_missing_thinking && out["thinking"].is_object() {
            tracing::warn!(
                "dropping thinking: assistant tool history has no thinking blocks to replay"
            );
            out.as_object_mut()
                .expect("out is an object")
                .remove("thinking");
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
        if let Some(last_msg) = out["messages"].as_array_mut().and_then(|m| m.last_mut())
            && let Some(blocks) = last_msg["content"].as_array_mut()
            && let Some(last) = blocks.last_mut()
        {
            last["cache_control"] = json!({"type": "ephemeral"});
        }

        if !tools.is_empty() {
            out["tools"] = json!(tools);
        }
        Ok((out, names))
    }

    pub async fn chat_stream(
        &self,
        body: Value,
    ) -> anyhow::Result<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send + Unpin>>
    {
        let (req, names) = self.translate_request(&body, true)?;
        let key = self.credential().await?;
        let resp = self
            .request(req.clone(), key.as_deref())
            .send()
            .await
            .context("upstream failed")?;
        // OAuth: a 401 means the server rejected this token — force a
        // refresh (not the cached-token path) and retry once.
        let resp = if resp.status() == reqwest::StatusCode::UNAUTHORIZED && self.oauth {
            let key = crate::oauth::force_refresh("anthropic").await?;
            self.request(req, Some(&key))
                .send()
                .await
                .context("upstream failed")?
        } else {
            resp
        };
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
            || resp.status() == reqwest::StatusCode::from_u16(529).unwrap()
        {
            return Err(crate::provider::RateLimited(crate::provider::retry_after(&resp)).into());
        }
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!("upstream: {text}");
        }
        let byte_stream = resp
            .bytes_stream()
            .map(|c| c.map_err(std::io::Error::other));
        Ok(Box::new(anthropic_events(
            Box::pin(byte_stream),
            std::sync::Arc::new(names),
        )))
    }

    pub async fn chat(&self, body: Value) -> anyhow::Result<Value> {
        let (req, names) = self.translate_request(&body, false)?;
        let key = self.credential().await?;
        let resp = self
            .request(req.clone(), key.as_deref())
            .send()
            .await
            .context("upstream failed")?;
        let resp = if resp.status() == reqwest::StatusCode::UNAUTHORIZED && self.oauth {
            let key = crate::oauth::force_refresh("anthropic").await?;
            self.request(req, Some(&key))
                .send()
                .await
                .context("upstream failed")?
        } else {
            resp
        };
        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::from_u16(529).unwrap()
        {
            return Err(crate::provider::RateLimited(crate::provider::retry_after(&resp)).into());
        }
        // Read the body as text first: a gateway 502/504 answers HTML,
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("upstream {status}: {text}");
        }
        let v: Value = serde_json::from_str(&text).context("invalid upstream JSON")?;
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
                        "name": unmangle(block["name"].as_str().unwrap_or(""), &names),
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
        // Map the real stop_reason — a hardcoded "stop" would break
        // tool-loop detection for /v1 clients.
        let finish_reason = match v["stop_reason"].as_str() {
            Some("tool_use") => "tool_calls",
            Some("max_tokens") => "length",
            _ => "stop",
        };
        // Anthropic usage → OpenAI field names; upstream reports no
        // total, so derive it.
        let prompt = v["usage"]["input_tokens"].as_u64().unwrap_or(0);
        let completion = v["usage"]["output_tokens"].as_u64().unwrap_or(0);
        Ok(json!({
            "choices": [{"index": 0, "message": msg, "finish_reason": finish_reason}],
            "usage": {
                "prompt_tokens": prompt,
                "completion_tokens": completion,
                "total_tokens": prompt + completion,
            },
        }))
    }
}

/// Restore the original tool name via this request's mangle map. A
/// name the map doesn't know returns unchanged: upstream-invented
/// names were never mangled, and guessing a substitution could route
/// a call to a silently wrong tool — a visible unknown-tool error is
/// better than quiet misexecution.
fn unmangle(name: &str, names: &std::collections::HashMap<String, String>) -> String {
    names.get(name).cloned().unwrap_or_else(|| name.to_string())
}

/// Parse Anthropic SSE into normalized StreamEvents.
/// Events: content_block_start (text|tool_use), content_block_delta
/// (text_delta|input_json_delta), message_stop.
fn anthropic_events(
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>>,
    names: std::sync::Arc<std::collections::HashMap<String, String>>,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send>> {
    use crate::llm::StopReason;
    // finish carries message_delta.stop_reason into the terminal Done.
    Box::pin(futures::stream::unfold(
        (
            stream,
            Vec::<u8>::new(),
            false,
            String::new(),
            0usize,
            // Dense tool-call counter — content-block indexes are sparse
            // (text/thinking blocks share the index space), so they must
            // not be used as ToolCallDelta.index directly.
            0usize,
            StopReason::Other,
            // (thinking text, signature) for the in-flight thinking block.
            String::new(),
            String::new(),
            // Whether the current content block is thinking — set on
            // content_block_start, cleared on content_block_stop.
            false,
            // Output tokens already reported by message_start — the
            // message_delta Usage must carry only the increment or
            // callers double-count.
            0u64,
        ),
        move |(
            mut stream,
            mut buf,
            mut done,
            mut cur_tool,
            mut tool_index,
            mut tool_count,
            mut finish,
            mut think_text,
            mut think_sig,
            mut cur_is_thinking,
            mut start_output,
        )| {
            let names = names.clone();
            async move {
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
                                    // message_delta will re-report cumulative
                                    // output_tokens — remember this baseline
                                    // so the delta event carries only the
                                    // increment, never a double count.
                                    start_output = u["output_tokens"].as_u64().unwrap_or(0);
                                    Some(StreamEvent::Usage {
                                        input: u["input_tokens"].as_u64().unwrap_or(0),
                                        output: start_output,
                                    })
                                } else {
                                    None
                                }
                            }
                            Some("content_block_start") => {
                                let block = &v["content_block"];
                                // A non-thinking block must clear the flag —
                                // a missing content_block_stop would
                                // otherwise emit a bogus ThinkingBlock at
                                // the next stop.
                                cur_is_thinking = false;
                                match block["type"].as_str() {
                                    Some("tool_use") => {
                                        cur_tool = block["id"].as_str().unwrap_or("").to_string();
                                        tool_index = tool_count;
                                        tool_count += 1;
                                        Some(StreamEvent::ToolCallDelta {
                                            index: tool_index,
                                            id: Some(cur_tool.clone()),
                                            name: Some(unmangle(
                                                block["name"].as_str().unwrap_or(""),
                                                &names,
                                            )),
                                            arguments: String::new(),
                                        })
                                    }
                                    Some("thinking") => {
                                        cur_is_thinking = true;
                                        think_text.clear();
                                        think_sig.clear();
                                        // A thinking block can arrive with
                                        // initial text on the start event.
                                        if let Some(t) = block["thinking"].as_str() {
                                            think_text.push_str(t);
                                        }
                                        None
                                    }
                                    // Redacted thinking arrives complete —
                                    // replay it verbatim on the next request.
                                    Some("redacted_thinking") => {
                                        Some(StreamEvent::ThinkingBlock(block.clone()))
                                    }
                                    _ => None,
                                }
                            }
                            Some("content_block_delta") => {
                                let d = &v["delta"];
                                match d["type"].as_str() {
                                    Some("text_delta") => Some(StreamEvent::Text(
                                        d["text"].as_str().unwrap_or("").to_string(),
                                    )),
                                    Some("thinking_delta") => {
                                        let t = d["thinking"].as_str().unwrap_or("");
                                        think_text.push_str(t);
                                        Some(StreamEvent::Thinking(t.to_string()))
                                    }
                                    // The signature arrives as its own delta —
                                    // required verbatim on the next request.
                                    Some("signature_delta") => {
                                        think_sig.push_str(d["signature"].as_str().unwrap_or(""));
                                        None
                                    }
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
                            Some("content_block_stop") => {
                                if cur_is_thinking {
                                    cur_is_thinking = false;
                                    Some(StreamEvent::ThinkingBlock(json!({
                                        "type": "thinking",
                                        "thinking": think_text,
                                        "signature": think_sig,
                                    })))
                                } else {
                                    None
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
                                // message_delta carries final output_tokens —
                                // emit only the increment over message_start's
                                // baseline or callers double-count.
                                let u = &v["usage"];
                                if u.is_object() {
                                    let out = u["output_tokens"].as_u64().unwrap_or(0);
                                    Some(StreamEvent::Usage {
                                        input: 0,
                                        output: out.saturating_sub(start_output),
                                    })
                                } else {
                                    None
                                }
                            }
                            Some("message_stop") => {
                                done = true;
                                Some(StreamEvent::Done(finish))
                            }
                            // Mid-stream errors (overloaded_error etc.) arrive
                            // as an `error` event — surface them instead of
                            // ending the turn as if it completed.
                            Some("error") => {
                                done = true;
                                // overloaded_error/rate_limit_error mid-stream map
                                // to the same RateLimited error an HTTP 429/529
                                // head produces, so the runtime/api backoff paths
                                // treat both identically.
                                if matches!(
                                    v["error"]["type"].as_str(),
                                    Some("overloaded_error") | Some("rate_limit_error")
                                ) {
                                    return Some((
                                        Err(anyhow::Error::new(crate::provider::RateLimited(
                                            std::time::Duration::from_secs(2),
                                        ))),
                                        (
                                            stream,
                                            buf,
                                            done,
                                            cur_tool,
                                            tool_index,
                                            tool_count,
                                            finish,
                                            think_text,
                                            think_sig,
                                            cur_is_thinking,
                                            start_output,
                                        ),
                                    ));
                                }
                                let msg = v["error"]["message"]
                                    .as_str()
                                    .unwrap_or("upstream stream error");
                                return Some((
                                    Err(anyhow::anyhow!("anthropic stream error: {msg}")),
                                    (
                                        stream,
                                        buf,
                                        done,
                                        cur_tool,
                                        tool_index,
                                        tool_count,
                                        finish,
                                        think_text,
                                        think_sig,
                                        cur_is_thinking,
                                        start_output,
                                    ),
                                ));
                            }
                            _ => None,
                        };
                        if let Some(ev) = ev {
                            return Some((
                                Ok(ev),
                                (
                                    stream,
                                    buf,
                                    done,
                                    cur_tool,
                                    tool_index,
                                    tool_count,
                                    finish,
                                    think_text,
                                    think_sig,
                                    cur_is_thinking,
                                    start_output,
                                ),
                            ));
                        }
                        continue;
                    }
                    match stream.next().await {
                        Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                        Some(Err(e)) => {
                            done = true;
                            return Some((
                                Err(e.into()),
                                (
                                    stream,
                                    buf,
                                    done,
                                    cur_tool,
                                    tool_index,
                                    tool_count,
                                    finish,
                                    think_text,
                                    think_sig,
                                    cur_is_thinking,
                                    start_output,
                                ),
                            ));
                        }
                        None => {
                            // A proxy may deliver the last SSE line without a
                            // trailing newline — feed one so the loop parses
                            // the buffered line instead of dropping it.
                            if !buf.is_empty() {
                                buf.push(b'\n');
                                continue;
                            }
                            done = true;
                            // Reaching EOF without a message_stop means the
                            // stream was truncated — surface an error so the
                            // caller can retry, not a clean Done that
                            // persists partial text as finished.
                            return Some((
                                Err(anyhow::anyhow!("stream ended without message_stop")),
                                (
                                    stream,
                                    buf,
                                    done,
                                    cur_tool,
                                    tool_index,
                                    tool_count,
                                    finish,
                                    think_text,
                                    think_sig,
                                    cur_is_thinking,
                                    start_output,
                                ),
                            ));
                        }
                    }
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prov() -> Anthropic {
        Anthropic::new(
            "claude",
            "http://localhost",
            None,
            &Default::default(),
            false,
            false,
        )
        .unwrap()
    }

    #[test]
    fn mangle_collision_assigns_distinct_names() {
        let p = prov();
        let body = json!({
            "model": "claude-sonnet-4",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "", "tool_calls": [
                    {"id": "t1", "type": "function",
                     "function": {"name": "a.b", "arguments": "{}"}}
                ]},
            ],
            "tools": [
                {"type": "function", "function": {"name": "a.b", "parameters": {}}},
                {"type": "function", "function": {"name": "a__b", "parameters": {}}},
            ],
        });
        let (out, names) = p.translate_request(&body, false).unwrap();
        let sent: Vec<&str> = out["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_ne!(sent[0], sent[1], "collision must not merge two tools");
        for n in &sent {
            assert!(n.len() <= 64, "wire name {n} exceeds the 64-char limit");
            assert_eq!(
                names.get(*n).unwrap(),
                if *n == "a__b" { "a.b" } else { "a__b" },
                "map must restore the original for {n}"
            );
        }
        // Declarations register first: the replayed tool_use resolves to
        // the same wire name the declaration sent.
        let replay = out["messages"][1]["content"][0]["name"].as_str().unwrap();
        assert_eq!(replay, sent[0]);
    }

    #[test]
    fn unmangle_unknown_name_returns_unchanged() {
        let mut names = std::collections::HashMap::new();
        names.insert("a__b".to_string(), "a.b".to_string());
        assert_eq!(unmangle("a__b", &names), "a.b");
        // Map miss: no guessed substitution — an upstream-invented
        // x__y must not silently become x.y.
        assert_eq!(unmangle("x__y", &names), "x__y");
    }

    #[test]
    fn stop_string_wraps_to_array() {
        let p = prov();
        let (out, _) = p
            .translate_request(&json!({"model": "m", "messages": [], "stop": "foo"}), false)
            .unwrap();
        assert_eq!(out["stop_sequences"], json!(["foo"]));
        let (out, _) = p
            .translate_request(
                &json!({"model": "m", "messages": [], "stop": ["foo", "bar"]}),
                false,
            )
            .unwrap();
        assert_eq!(out["stop_sequences"], json!(["foo", "bar"]));
        let (out, _) = p
            .translate_request(
                &json!({"model": "m", "messages": [], "stop_sequences": ["a"], "stop": "b"}),
                false,
            )
            .unwrap();
        assert_eq!(out["stop_sequences"], json!(["a"]));
        let (out, _) = p
            .translate_request(&json!({"model": "m", "messages": [], "stop": null}), false)
            .unwrap();
        assert!(out.get("stop_sequences").is_none());
    }

    /// Drive `anthropic_events` over one static SSE payload.
    fn drive(sse: &'static str) -> Vec<anyhow::Result<StreamEvent>> {
        let stream: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>> =
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(sse.as_bytes()))
            }));
        let names = std::sync::Arc::new(std::collections::HashMap::new());
        let mut s = anthropic_events(stream, names);
        futures::executor::block_on(async {
            let mut out = Vec::new();
            while let Some(ev) = s.next().await {
                out.push(ev);
            }
            out
        })
    }

    #[test]
    fn midstream_overloaded_error_maps_to_rate_limited() {
        let evs = drive(concat!(
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
        ));
        let err = evs
            .iter()
            .filter_map(|e| e.as_ref().err())
            .next()
            .expect("overloaded_error must surface as Err");
        let rl = err
            .downcast_ref::<crate::provider::RateLimited>()
            .expect("overloaded_error must downcast to RateLimited");
        assert_eq!(rl.0, std::time::Duration::from_secs(2));
    }

    #[test]
    fn midstream_generic_error_stays_generic() {
        let evs = drive(
            "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"boom\"}}\n\n",
        );
        let err = evs.iter().filter_map(|e| e.as_ref().err()).next().unwrap();
        assert!(err.downcast_ref::<crate::provider::RateLimited>().is_none());
        assert!(err.to_string().contains("boom"), "got {err}");
    }
}
