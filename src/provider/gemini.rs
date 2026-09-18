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
            client: super::http_client(),
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

    fn translate_request(
        &self,
        body: &Value,
    ) -> anyhow::Result<(Value, std::collections::HashMap<String, String>)> {
        // Mangled→original tool-name map for this request: `mangle`
        // rewrites every `.`, so a one-shot `replacen` cannot restore
        // multi-dot names — responses must be unmangled through this map.
        let mut names = std::collections::HashMap::new();
        let mut contents = Vec::new();
        let mut system = Vec::new();

        // OpenAI tool messages carry only tool_call_id — map it back to
        // the function name from the assistant turn's tool_calls, or
        // Gemini rejects the functionResponse for a name mismatch.
        let mut call_names: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        // OpenAI histories store one role:"tool" row per call. Gemini
        // rejects consecutive same-role contents with a 400, so fold
        // each run of user/tool rows into ONE user turn — functionResponse
        // parts leading — and each run of assistant rows into one model
        // turn.
        fn flush(
            contents: &mut Vec<Value>,
            role: &str,
            results: &mut Vec<Value>,
            others: &mut Vec<Value>,
        ) {
            if role.is_empty() {
                return;
            }
            let mut parts = std::mem::take(results);
            parts.append(others);
            // Gemini rejects empty parts arrays — a folded message with
            // no surviving parts is dropped entirely.
            if !parts.is_empty() {
                contents.push(json!({ "role": role, "parts": parts }));
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
                        system.push(json!({"text": t}));
                    }
                }
                Some("user" | "tool") => {
                    if cur_role != "user" {
                        flush(&mut contents, cur_role, &mut cur_results, &mut cur_others);
                        cur_role = "user";
                    }
                    if m["role"].as_str() == Some("tool") {
                        let name = m["tool_call_id"]
                            .as_str()
                            .and_then(|id| call_names.get(id).cloned())
                            .or_else(|| m["name"].as_str().map(String::from))
                            // A tool row whose id resolves to no known
                            // call produces a functionResponse matching
                            // no declared function — upstream 400s.
                            .with_context(|| {
                                format!(
                                    "tool message references unknown tool_call_id {:?}",
                                    m["tool_call_id"]
                                )
                            })?;
                        cur_results.push(json!({
                            "functionResponse": {
                                "name": mangle(&name),
                                "response": {"result": super::content_text(&m["content"])},
                            }
                        }));
                    } else {
                        // Gemini rejects empty text parts — a user
                        // message with no text (or only non-text parts)
                        // must not emit one.
                        let t = super::content_text(&m["content"]);
                        if !t.is_empty() {
                            cur_others.push(json!({"text": t}));
                        }
                    }
                }
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
                                "args": match &f["arguments"] {
                                    // arguments may arrive as a JSON
                                    // string (OpenAI wire) or an object.
                                    Value::String(s) => serde_json::from_str::<Value>(s)
                                        .unwrap_or(json!({})),
                                    other => other.clone(),
                                },
                            }
                        }));
                    }
                    if cur_role != "model" {
                        flush(&mut contents, cur_role, &mut cur_results, &mut cur_others);
                        cur_role = "model";
                    }
                    cur_others.append(&mut parts);
                }
                _ => {}
            }
        }
        flush(&mut contents, cur_role, &mut cur_results, &mut cur_others);

        let tools: Vec<Value> = body["tools"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|t| {
                let f = &t["function"];
                let orig = f["name"].as_str().unwrap_or("");
                let mangled = mangle(orig);
                names.insert(mangled.clone(), orig.to_string());
                json!({
                    "name": mangled,
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
        // Sampling/generation parameters → generationConfig. Merge with
        // the thinking block below instead of overwriting the key.
        let mut gc = json!({});
        if let Some(mt) = body["max_tokens"]
            .as_u64()
            .or_else(|| body["max_completion_tokens"].as_u64())
        {
            gc["maxOutputTokens"] = json!(mt);
        }
        for (src, dst) in [("temperature", "temperature"), ("top_p", "topP")] {
            if !body[src].is_null() {
                gc[dst] = body[src].clone();
            }
        }
        if let Some(stop) = body["stop"].as_array() {
            let seq: Vec<&str> = stop.iter().filter_map(|s| s.as_str()).collect();
            if !seq.is_empty() {
                gc["stopSequences"] = json!(seq);
            }
        } else if let Some(s) = body["stop"].as_str() {
            gc["stopSequences"] = json!([s]);
        }
        match body["response_format"]["type"].as_str() {
            Some("json_object") => gc["responseMimeType"] = json!("application/json"),
            Some("json_schema") => {
                gc["responseMimeType"] = json!("application/json");
                if let Some(s) = body["response_format"]["json_schema"]["schema"].as_object() {
                    gc["responseJsonSchema"] = json!(s);
                }
            }
            _ => {}
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
                gc["thinkingConfig"] = json!({"thinkingBudget": budget});
            }
        }
        if gc.as_object().is_some_and(|o| !o.is_empty()) {
            out["generationConfig"] = gc;
        }
        // OpenAI tool_choice → functionCallingConfig. A function pick
        // names a mangled tool, so record it in the name map too.
        match &body["tool_choice"] {
            Value::String(s) => {
                let mode = match s.as_str() {
                    "auto" => Some("AUTO"),
                    "required" => Some("ANY"),
                    "none" => Some("NONE"),
                    _ => None,
                };
                if let Some(m) = mode {
                    out["toolConfig"] = json!({"functionCallingConfig": {"mode": m}});
                }
            }
            Value::Object(_) if body["tool_choice"]["type"].as_str() == Some("function") => {
                let orig = body["tool_choice"]["function"]["name"]
                    .as_str()
                    .unwrap_or("");
                let mangled = mangle(orig);
                names.insert(mangled.clone(), orig.to_string());
                out["toolConfig"] = json!({
                    "functionCallingConfig": {"mode": "ANY", "allowedFunctionNames": [mangled]}
                });
            }
            _ => {}
        }
        Ok((out, names))
    }

    pub async fn chat_stream(
        &self,
        body: Value,
    ) -> anyhow::Result<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send + Unpin>>
    {
        let model = body["model"].as_str().context("missing model")?.to_string();
        let (req, names) = self.translate_request(&body)?;
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
        // 429 AND 503 (RESOURCE_EXHAUSTED overload) both warrant backoff —
        // a generic bail would skip the runtime's retry path entirely.
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
            || resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            return Err(super::RateLimited(super::retry_after(&resp)).into());
        }
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!("upstream: {text}");
        }
        let byte_stream = resp
            .bytes_stream()
            .map(|c| c.map_err(std::io::Error::other));
        Ok(Box::new(gemini_events(
            Box::pin(byte_stream),
            std::sync::Arc::new(names),
        )))
    }

    pub async fn chat(&self, body: Value) -> anyhow::Result<Value> {
        let model = body["model"].as_str().context("missing model")?.to_string();
        let (req, names) = self.translate_request(&body)?;
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
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            return Err(super::RateLimited(super::retry_after(&resp)).into());
        }
        // Read the body as text first: a gateway 502/504 answers HTML,
        // and json() failing before the status check would lose both.
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("upstream {status}: {text}");
        }
        let v: Value = serde_json::from_str(&text).context("invalid upstream JSON")?;
        Ok(gemini_to_openai(&v, &names))
    }
}

fn mangle(name: &str) -> String {
    name.replace('.', "__")
}

/// Restore the original tool name via this request's mangle map; fall
/// back to the single-dot heuristic for names this request never
/// mangled (e.g. upstream-invented calls).
fn unmangle(name: &str, names: &std::collections::HashMap<String, String>) -> String {
    names
        .get(name)
        .cloned()
        .unwrap_or_else(|| name.replacen("__", ".", 1))
}

/// Gemini response JSON → OpenAI-shaped response.
fn gemini_to_openai(v: &Value, names: &std::collections::HashMap<String, String>) -> Value {
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
                    "name": unmangle(fc["name"].as_str().unwrap_or(""), names),
                    "arguments": fc["args"].to_string(),
                }
            }));
        }
    }
    let mut msg = json!({"role": "assistant", "content": text});
    if !tool_calls.is_empty() {
        msg["tool_calls"] = json!(tool_calls);
    }
    // Map the real finishReason — a hardcoded "stop" would break
    // tool-loop detection for /v1 clients.
    let finish_reason = match v["candidates"][0]["finishReason"].as_str() {
        Some("STOP") if !tool_calls.is_empty() => "tool_calls",
        Some("MAX_TOKENS") => "length",
        _ => "stop",
    };
    // Gemini usage → OpenAI field names; upstream reports no
    // prompt/completion split under those keys.
    let u = &v["usageMetadata"];
    let prompt = u["promptTokenCount"].as_u64().unwrap_or(0);
    let completion = u["candidatesTokenCount"].as_u64().unwrap_or(0);
    json!({
        "choices": [{"index": 0, "message": msg, "finish_reason": finish_reason}],
        "usage": {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": u["totalTokenCount"].as_u64().unwrap_or(prompt + completion),
        },
    })
}

/// Parse Gemini SSE (streamGenerateContent?alt=sse) into normalized events.
fn gemini_events(
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send>>,
    names: std::sync::Arc<std::collections::HashMap<String, String>>,
) -> std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send>> {
    use crate::llm::StopReason;
    use std::collections::VecDeque;
    // Events parsed from one chunk queue up; we drain one per poll.
    Box::pin(futures::stream::unfold(
        (stream, Vec::<u8>::new(), false, 0usize, VecDeque::new()),
        move |(mut stream, mut buf, mut done, mut call_idx, mut queue)| {
            let names = names.clone();
            async move {
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
                        // Mid-stream errors arrive as {"error": {...}} with no
                        // candidates — surface them instead of ending cleanly.
                        if let Some(e) = v.get("error").filter(|e| e.is_object()) {
                            done = true;
                            let msg = e["message"].as_str().unwrap_or("upstream stream error");
                            return Some((
                                Err(anyhow::anyhow!("gemini stream error: {msg}")),
                                (stream, buf, done, call_idx, queue),
                            ));
                        }
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
                                    name: Some(unmangle(fc["name"].as_str().unwrap_or(""), &names)),
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
                            // A proxy may deliver the last SSE line without a
                            // trailing newline — feed one so the loop parses
                            // the buffered line instead of dropping it.
                            if !buf.is_empty() {
                                buf.push(b'\n');
                                continue;
                            }
                            done = true;
                            // Reaching EOF without a finishReason means the
                            // stream was truncated — surface an error so the
                            // caller can retry, not a clean Done that
                            // persists partial text as finished.
                            return Some((
                                Err(anyhow::anyhow!("stream ended without finishReason")),
                                (stream, buf, done, call_idx, queue),
                            ));
                        }
                    }
                }
            }
        },
    ))
}
