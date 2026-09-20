//! POST /v1/responses — OpenAI Responses-API surface for Codex-style
//! clients.
//!
//! The endpoint is a shape adapter, not a second dispatch path: the
//! request is translated to the internal chat-completions body
//! (`{model, messages, tools, stream, _thinking?}`) and run through the
//! same `forward()` used by /v1/chat/completions, so provider routing,
//! `provider/model:level` suffixes, translation for anthropic/gemini
//! upstreams, and context promotion all behave identically. The chat
//! completion that comes back is then re-shaped into a Responses object
//! (or a Responses SSE stream).

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde_json::{Value, json};
use tracing::warn;

use crate::api::{AppState, forward, openai_error};

/// POST /v1/responses handler. `store` is accepted but ignored — the
/// daemon is stateless across calls, so `item_reference` inputs and
/// `previous_response_id` can't be resolved (see `to_chat_request`).
pub async fn responses(State(state): State<Arc<AppState>>, body: bytes::Bytes) -> Response {
    let req: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return openai_error(
                StatusCode::BAD_REQUEST,
                "invalid JSON body",
                "invalid_request_error",
            );
        }
    };
    let stream = req["stream"].as_bool().unwrap_or(false);
    let chat = match to_chat_request(&req) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    let resp = forward(
        &state,
        reqwest::Method::POST,
        "/chat/completions",
        bytes::Bytes::from(chat.to_string()),
    )
    .await;
    // Upstream errors already carry the OpenAI {error:{message,type,code}}
    // shape the chat path returns — pass them through untouched.
    if !resp.status().is_success() {
        return resp;
    }
    let is_sse = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("text/event-stream"));
    if stream && is_sse {
        return stream_response(resp);
    }
    // Non-streaming (or a stream request the provider answered with JSON —
    // e.g. an upstream that ignores `stream`): buffer and re-shape.
    map_json_response(resp).await
}

/// Responses request → internal chat-completions body.
fn to_chat_request(req: &Value) -> Result<Value, Box<Response>> {
    // previous_response_id chains off a stored response — same store we
    // don't keep, so fail loudly rather than dropping the prior turn.
    if req
        .get("previous_response_id")
        .is_some_and(|v| !v.is_null())
    {
        return Err(Box::new(openai_error(
            StatusCode::BAD_REQUEST,
            "previous_response_id requires stored responses, which this endpoint does not support",
            "invalid_request_error",
        )));
    }
    let mut messages: Vec<Value> = Vec::new();
    if let Some(s) = req["instructions"].as_str() {
        messages.push(json!({"role": "system", "content": s}));
    }
    // Consecutive function_call items fold into ONE assistant message's
    // tool_calls array — the chat shape can't express adjacent assistant
    // calls any other way, and providers expect them grouped.
    let mut pending_calls: Vec<Value> = Vec::new();
    let flush_calls = |messages: &mut Vec<Value>, pending: &mut Vec<Value>| {
        if !pending.is_empty() {
            messages.push(json!({
                "role": "assistant",
                "content": Value::Null,
                "tool_calls": std::mem::take(pending),
            }));
        }
    };

    match &req["input"] {
        Value::String(s) => messages.push(json!({"role": "user", "content": s})),
        Value::Array(items) => {
            for item in items {
                // Items without an explicit type that carry role+content
                // are messages (the API tolerates the shorthand).
                let ty = item["type"].as_str().unwrap_or_else(|| {
                    if item.get("role").is_some() {
                        "message"
                    } else {
                        ""
                    }
                });
                match ty {
                    "message" => {
                        flush_calls(&mut messages, &mut pending_calls);
                        // "developer" is the Responses name for a system
                        // message; chat-completions only knows "system".
                        let role = match item["role"].as_str() {
                            Some("developer") | Some("system") => "system",
                            Some("assistant") => "assistant",
                            _ => "user",
                        };
                        messages.push(json!({
                            "role": role,
                            "content": translate_content(&item["content"]),
                        }));
                    }
                    "function_call" => {
                        pending_calls.push(json!({
                            "id": item["call_id"].as_str().or_else(|| item["id"].as_str()).unwrap_or(""),
                            "type": "function",
                            "function": {
                                "name": item["name"].as_str().unwrap_or(""),
                                "arguments": item["arguments"].as_str().unwrap_or(""),
                            },
                        }));
                    }
                    "function_call_output" => {
                        flush_calls(&mut messages, &mut pending_calls);
                        let output = match &item["output"] {
                            Value::String(s) => s.clone(),
                            // Structured output parts (output_text /
                            // output_image / …) — flatten to text; images
                            // inside tool results have no chat equivalent.
                            Value::Array(parts) => parts
                                .iter()
                                .filter_map(|p| p["text"].as_str())
                                .collect::<Vec<_>>()
                                .join(""),
                            other => other.to_string(),
                        };
                        messages.push(json!({
                            "role": "tool",
                            "tool_call_id": item["call_id"].as_str().unwrap_or(""),
                            "content": output,
                        }));
                    }
                    // Server-side items we can't replay: reasoning blocks
                    // are provider-internal (encrypted_content needs a
                    // store we don't keep), and item_reference /
                    // previous_response_id need the Responses store that
                    // a `store:false`-style daemon doesn't have.
                    // Referencing one is a client error, not something to
                    // skip silently — the conversation would lose a turn.
                    "item_reference" => {
                        return Err(Box::new(openai_error(
                            StatusCode::BAD_REQUEST,
                            "item_reference requires stored responses, which this endpoint does not support",
                            "invalid_request_error",
                        )));
                    }
                    // Unknown/future item types are dropped rather than
                    // failing the whole request — same leniency the chat
                    // endpoint shows unknown message fields.
                    _ => {}
                }
            }
        }
        _ => {}
    }
    flush_calls(&mut messages, &mut pending_calls);

    let mut out = json!({
        "model": req["model"].as_str().unwrap_or(""),
        "messages": messages,
        "stream": req["stream"].as_bool().unwrap_or(false),
    });
    // Scalar passthroughs both APIs share verbatim.
    for key in [
        "temperature",
        "top_p",
        "parallel_tool_calls",
        "user",
        "metadata",
    ] {
        if let Some(v) = req.get(key) {
            out[key] = v.clone();
        }
    }
    // tool_choice: strings ("auto"/"required"/…) pass through; the
    // Responses function form {type:"function",name} wraps into chat's
    // {type:"function",function:{name}}. Other object shapes
    // (allowed_tools, mcp, …) have no chat equivalent — dropped.
    match &req["tool_choice"] {
        Value::String(_) => out["tool_choice"] = req["tool_choice"].clone(),
        Value::Object(_)
            if req["tool_choice"]["type"].as_str() == Some("function")
                && req["tool_choice"]["name"].is_string() =>
        {
            out["tool_choice"] = json!({
                "type": "function",
                "function": {"name": req["tool_choice"]["name"]},
            });
        }
        _ => {}
    }
    // max_tokens is the internal canonical field — provider adapters
    // rename it (compat.max_tokens_field, max_output_tokens, …).
    if let Some(v) = req.get("max_output_tokens") {
        out["max_tokens"] = v.clone();
    }
    // reasoning.effort → the `_thinking` convention the adapters read
    // (same mechanism as the `model:low|medium|high` suffix; a suffix on
    // the model string wins because forward() overwrites `_thinking`).
    // Unknown effort values pass through — adapters ignore what they
    // don't know.
    if let Some(effort) = req["reasoning"]["effort"].as_str() {
        out["_thinking"] = json!(effort);
    }
    // text.format → response_format (inverse of the responses provider's
    // request translation).
    match req["text"]["format"]["type"].as_str() {
        Some("json_object") => out["response_format"] = json!({"type": "json_object"}),
        Some("json_schema") => {
            let f = &req["text"]["format"];
            out["response_format"] = json!({
                "type": "json_schema",
                "json_schema": {
                    "name": f["name"].as_str().unwrap_or("response"),
                    "schema": f["schema"],
                    "strict": f["strict"],
                },
            });
        }
        _ => {}
    }
    // Responses tools are flat {type:"function",name,…}; chat wraps them
    // in a `function` object. Non-function tools (web_search, mcp, …)
    // have no chat-completions equivalent and are dropped — the model
    // simply won't see them rather than the request failing.
    if let Some(tools) = req["tools"].as_array() {
        let mapped: Vec<Value> = tools
            .iter()
            .filter(|t| t["type"].as_str() == Some("function"))
            .map(|t| {
                let mut f = serde_json::Map::new();
                for k in ["name", "description", "parameters", "strict"] {
                    if let Some(v) = t.get(k) {
                        f.insert(k.to_string(), v.clone());
                    }
                }
                json!({"type": "function", "function": Value::Object(f)})
            })
            .collect();
        if !mapped.is_empty() {
            out["tools"] = json!(mapped);
        }
    }
    // Ask the upstream for a usage chunk so response.completed can carry
    // real token counts; providers that don't support stream_options
    // ignore it and usage is simply omitted downstream.
    if out["stream"].as_bool() == Some(true) && req.get("stream_options").is_none() {
        out["stream_options"] = json!({"include_usage": true});
    }
    Ok(out)
}

/// Responses message content → chat-completions content. Strings pass
/// through; part arrays map input_text/output_text → text and
/// input_image → image_url. Parts with no chat equivalent (input_file,
/// input_audio) are dropped.
fn translate_content(content: &Value) -> Value {
    match content {
        Value::String(_) => content.clone(),
        Value::Array(parts) => {
            let mapped: Vec<Value> = parts
                .iter()
                .filter_map(|p| match p["type"].as_str() {
                    Some("input_text") | Some("output_text") | Some("text") => {
                        Some(json!({"type": "text", "text": p["text"].as_str().unwrap_or("")}))
                    }
                    Some("input_image") => {
                        // image_url carries a URL or data: URI; file_id
                        // needs the Responses file store — dropped.
                        p["image_url"].as_str().map(|u| {
                            let mut img = json!({"url": u});
                            if let Some(d) = p["detail"].as_str() {
                                img["detail"] = json!(d);
                            }
                            json!({"type": "image_url", "image_url": img})
                        })
                    }
                    // refusal parts keep their text so the turn isn't lost.
                    Some("refusal") => {
                        Some(json!({"type": "text", "text": p["refusal"].as_str().unwrap_or("")}))
                    }
                    _ => None,
                })
                .collect();
            Value::Array(mapped)
        }
        _ => content.clone(),
    }
}

/// Buffer a non-stream upstream chat completion and re-shape it into a
/// Responses object.
async fn map_json_response(resp: Response) -> Response {
    let status = resp.status();
    let stream = resp.into_body().into_data_stream();
    let stream = stream.map(|r| r.map_err(std::io::Error::other));
    let buf = match crate::provider::collect_stream(Box::pin(stream)).await {
        Ok(b) => b,
        Err(e) => {
            warn!("upstream response body read failed: {e}");
            return openai_error(StatusCode::BAD_GATEWAY, "upstream error", "server_error");
        }
    };
    let v: Value = match serde_json::from_slice(&buf) {
        Ok(v) => v,
        Err(_) => {
            warn!("upstream returned non-JSON success body");
            return openai_error(
                StatusCode::BAD_GATEWAY,
                "upstream returned malformed response",
                "server_error",
            );
        }
    };
    let choice = &v["choices"][0];
    let msg = &choice["message"];
    let mut output: Vec<Value> = Vec::new();
    if let Some(text) = msg["content"].as_str() {
        output.push(json!({
            "id": new_item_id("msg"),
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text, "annotations": []}],
        }));
    }
    if let Some(calls) = msg["tool_calls"].as_array() {
        for tc in calls {
            output.push(function_call_item(
                tc["id"].as_str().unwrap_or(""),
                tc["function"]["name"].as_str().unwrap_or(""),
                tc["function"]["arguments"].as_str().unwrap_or(""),
            ));
        }
    }
    let finish = choice["finish_reason"].as_str().unwrap_or("stop");
    let body = build_response(&v, output, finish, false);
    (status, axum::Json(body)).into_response()
}

/// One function_call output item. `call_id` echoes the chat tool_call id
/// (what the client must send back in function_call_output); `id` is a
/// fresh item id.
fn function_call_item(call_id: &str, name: &str, arguments: &str) -> Value {
    json!({
        "id": new_item_id("fc"),
        "type": "function_call",
        "status": "completed",
        "call_id": call_id,
        "name": name,
        "arguments": arguments,
    })
}

fn new_item_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Assemble the terminal response object shared by the JSON and SSE
/// paths. `finish` is the chat finish_reason; `failed` marks a mid-stream
/// upstream error.
fn build_response(upstream: &Value, output: Vec<Value>, finish: &str, failed: bool) -> Value {
    let status = if failed {
        "failed"
    } else if finish == "length" {
        "incomplete"
    } else {
        "completed"
    };
    let mut r = json!({
        "id": format!("resp_{}", uuid::Uuid::new_v4().simple()),
        "object": "response",
        "created_at": upstream["created"].as_u64().unwrap_or_else(unix_now),
        "status": status,
        "model": upstream["model"].as_str().unwrap_or(""),
        "output": output,
    });
    if status == "incomplete" {
        r["incomplete_details"] = json!({"reason": "max_output_tokens"});
    }
    if failed {
        r["error"] = json!({"code": "server_error", "message": "upstream stream error"});
    }
    if let Some(u) = map_usage(&upstream["usage"]) {
        r["usage"] = u;
    }
    r
}

/// Chat usage → Responses usage. Absent usage stays absent (a stream
/// without a usage chunk reports none rather than fabricating zeros).
fn map_usage(u: &Value) -> Option<Value> {
    let input = u["prompt_tokens"].as_u64();
    let output = u["completion_tokens"].as_u64();
    if input.is_none() && output.is_none() {
        return None;
    }
    let mut out = json!({
        "input_tokens": input.unwrap_or(0),
        "output_tokens": output.unwrap_or(0),
        "total_tokens": u["total_tokens"]
            .as_u64()
            .unwrap_or_else(|| input.unwrap_or(0) + output.unwrap_or(0)),
    });
    if let Some(c) = u["prompt_tokens_details"]["cached_tokens"].as_u64() {
        out["input_tokens_details"] = json!({"cached_tokens": c});
    }
    if let Some(r) = u["completion_tokens_details"]["reasoning_tokens"].as_u64() {
        out["output_tokens_details"] = json!({"reasoning_tokens": r});
    }
    Some(out)
}

// ---------------------------------------------------------------------
// Streaming: chat-completions SSE → Responses SSE.
//
// Minimum-viable event set (documented choice): text streams as
// response.output_item.added + response.output_text.delta* +
// response.output_item.done; reasoning_content deltas get the parallel
// reasoning item; tool calls are accumulated and emitted as complete
// function_call items (added+done back-to-back) at end of stream rather
// than as function_call_arguments.delta — Codex clients consume the
// completed item and the chat delta stream doesn't guarantee clean
// argument boundaries anyway. The terminal event is response.completed /
// response.incomplete / response.failed carrying the full response
// object including usage.

/// Wrap the upstream SSE body in a Responses-event stream.
fn stream_response(resp: Response) -> Response {
    let upstream = resp.into_body().into_data_stream();
    let state = SseState::new(Box::pin(upstream));
    let stream = futures::stream::unfold(state, |mut st| async move {
        loop {
            if let Some(ev) = st.pending.pop_front() {
                return Some((Ok::<_, std::io::Error>(bytes::Bytes::from(ev)), st));
            }
            if st.done {
                return None;
            }
            match st.upstream.next().await {
                Some(Ok(chunk)) => st.ingest(&chunk),
                Some(Err(e)) => {
                    warn!("upstream stream read failed: {e}");
                    st.upstream_error = true;
                    st.finalize();
                }
                None => st.finalize(),
            }
        }
    });
    Response::builder()
        .status(200)
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// One accumulated output item, in first-appearance order. `id` is
/// minted at creation — delta/done events must echo the SAME item id the
/// added event announced, so it can't be generated per serialization.
enum Item {
    Text {
        id: String,
        text: String,
    },
    Reasoning {
        id: String,
        text: String,
    },
    Call {
        id: String,
        call_id: String,
        name: String,
        arguments: String,
    },
}

impl Item {
    fn id(&self) -> &str {
        match self {
            Item::Text { id, .. } | Item::Reasoning { id, .. } | Item::Call { id, .. } => id,
        }
    }

    fn json(&self) -> Value {
        match self {
            Item::Text { id, text } => json!({
                "id": id,
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text, "annotations": []}],
            }),
            Item::Reasoning { id, text } => json!({
                "id": id,
                "type": "reasoning",
                "status": "completed",
                "summary": [{"type": "summary_text", "text": text}],
            }),
            Item::Call {
                id,
                call_id,
                name,
                arguments,
            } => json!({
                "id": id,
                "type": "function_call",
                "status": "completed",
                "call_id": call_id,
                "name": name,
                "arguments": arguments,
            }),
        }
    }
}

type UpstreamStream =
    Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, axum::Error>> + Send>>;

struct SseState {
    upstream: UpstreamStream,
    /// Undelivered SSE frames, already serialized.
    pending: VecDeque<String>,
    /// Leftover bytes between SSE event boundaries.
    buf: Vec<u8>,
    items: Vec<Item>,
    /// Index into `items` of the currently-open text/reasoning item —
    /// tool calls are never "open" (emitted complete at finalize).
    open: Option<usize>,
    finish: String,
    usage: Option<Value>,
    model: String,
    upstream_error: bool,
    done: bool,
    finalized: bool,
}

impl SseState {
    fn new(upstream: UpstreamStream) -> Self {
        let mut pending = VecDeque::new();
        // Spec order: created then in_progress. The response object is
        // still empty here; the full one rides on the terminal event.
        let stub = json!({
            "id": format!("resp_{}", uuid::Uuid::new_v4().simple()),
            "object": "response",
            "created_at": unix_now(),
            "status": "in_progress",
            "output": [],
        });
        pending.push_back(format!(
            "event: response.created\ndata: {stub}\n\n\
             event: response.in_progress\ndata: {stub}\n\n"
        ));
        Self {
            upstream,
            pending,
            buf: Vec::new(),
            items: Vec::new(),
            open: None,
            finish: "stop".into(),
            usage: None,
            model: String::new(),
            upstream_error: false,
            done: false,
            finalized: false,
        }
    }

    fn emit(&mut self, event: &str, data: Value) {
        self.pending
            .push_back(format!("event: {event}\ndata: {data}\n\n"));
    }

    /// Append bytes and process every complete SSE event they finish.
    fn ingest(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
        while let Some((end, dlen)) = find_event_end(&self.buf) {
            let ev: Vec<u8> = self.buf.drain(..end + dlen).collect();
            self.process_event(&ev[..end]);
        }
    }

    /// One SSE event (bytes between blank-line separators). Only `data:`
    /// lines matter — upstream chat SSE doesn't send `event:` lines.
    fn process_event(&mut self, ev: &[u8]) {
        if self.finalized {
            return;
        }
        let text = String::from_utf8_lossy(ev);
        let data: Vec<&str> = text
            .lines()
            .filter_map(|l| {
                l.strip_prefix("data:")
                    .map(|d| d.strip_prefix(' ').unwrap_or(d))
            })
            .collect();
        if data.is_empty() {
            return;
        }
        let data = data.join("\n");
        if data.trim() == "[DONE]" {
            self.finalize();
            return;
        }
        let Ok(v) = serde_json::from_str::<Value>(&data) else {
            return;
        };
        if v.get("error").is_some() {
            self.upstream_error = true;
            return;
        }
        if let Some(m) = v["model"].as_str() {
            self.model = m.to_string();
        }
        if v.get("usage").is_some_and(|u| u.is_object()) {
            self.usage = Some(v["usage"].clone());
        }
        let choice = &v["choices"][0];
        if let Some(f) = choice["finish_reason"].as_str() {
            self.finish = f.to_string();
        }
        let delta = &choice["delta"];
        if let Some(t) = delta["content"].as_str()
            && !t.is_empty()
        {
            self.push_text(t);
        }
        if let Some(t) = delta["reasoning_content"].as_str()
            && !t.is_empty()
        {
            self.push_reasoning(t);
        }
        if let Some(calls) = delta["tool_calls"].as_array() {
            for tc in calls {
                self.push_call_delta(tc);
            }
        }
    }

    /// Locate the open text item, creating + announcing it on first use.
    fn open_text(&mut self) -> usize {
        if let Some(i) = self.open
            && matches!(self.items[i], Item::Text { .. })
        {
            return i;
        }
        self.close_open();
        let idx = self.items.len();
        self.items.push(Item::Text {
            id: new_item_id("msg"),
            text: String::new(),
        });
        self.open = Some(idx);
        self.emit(
            "response.output_item.added",
            json!({"output_index": idx, "item": self.items[idx].json()}),
        );
        idx
    }

    fn open_reasoning(&mut self) -> usize {
        if let Some(i) = self.open
            && matches!(self.items[i], Item::Reasoning { .. })
        {
            return i;
        }
        self.close_open();
        let idx = self.items.len();
        self.items.push(Item::Reasoning {
            id: new_item_id("rs"),
            text: String::new(),
        });
        self.open = Some(idx);
        self.emit(
            "response.output_item.added",
            json!({"output_index": idx, "item": self.items[idx].json()}),
        );
        idx
    }

    fn push_text(&mut self, t: &str) {
        let idx = self.open_text();
        if let Item::Text { text, .. } = &mut self.items[idx] {
            text.push_str(t);
        }
        self.emit(
            "response.output_text.delta",
            json!({"item_id": self.items[idx].id(), "output_index": idx,
                   "content_index": 0, "delta": t}),
        );
    }

    fn push_reasoning(&mut self, t: &str) {
        let idx = self.open_reasoning();
        if let Item::Reasoning { text, .. } = &mut self.items[idx] {
            text.push_str(t);
        }
        self.emit(
            "response.reasoning_summary_text.delta",
            json!({"item_id": self.items[idx].id(), "output_index": idx,
                   "summary_index": 0, "delta": t}),
        );
    }

    /// Accumulate a tool-call delta. The item is created (so output_index
    /// ordering matches first appearance) but NOT announced — function
    /// call items go out complete at finalize.
    fn push_call_delta(&mut self, tc: &Value) {
        self.close_open();
        let index = tc["index"].as_u64().unwrap_or(0) as usize;
        // Find or create the Call item for this tool_calls index. Chat
        // indexes are dense from 0; sparse indexes are tolerated by
        // scanning rather than positional indexing.
        let pos = self
            .items
            .iter()
            .enumerate()
            .filter(|(_, it)| matches!(it, Item::Call { .. }))
            .nth(index)
            .map(|(i, _)| i);
        let pos = match pos {
            Some(p) => p,
            None => {
                self.items.push(Item::Call {
                    id: new_item_id("fc"),
                    call_id: String::new(),
                    name: String::new(),
                    arguments: String::new(),
                });
                self.items.len() - 1
            }
        };
        if let Item::Call {
            call_id,
            name,
            arguments,
            ..
        } = &mut self.items[pos]
        {
            if let Some(id) = tc["id"].as_str()
                && !id.is_empty()
            {
                *call_id = id.to_string();
            }
            if let Some(n) = tc["function"]["name"].as_str()
                && !n.is_empty()
            {
                // Some providers repeat the name on every delta — only
                // fill it once rather than concatenating.
                if name.is_empty() {
                    *name = n.to_string();
                }
            }
            if let Some(a) = tc["function"]["arguments"].as_str() {
                arguments.push_str(a);
            }
        }
    }

    /// Close the open text/reasoning item, if any.
    fn close_open(&mut self) {
        let Some(idx) = self.open.take() else {
            return;
        };
        // Copy out before emitting — `emit` needs &mut self, which can't
        // coexist with the match borrow on self.items.
        let (done_event, payload) = match &self.items[idx] {
            Item::Text { id, text } => (
                "response.output_text.done",
                json!({"item_id": id, "output_index": idx,
                       "content_index": 0, "text": text}),
            ),
            Item::Reasoning { id, text } => (
                "response.reasoning_summary_text.done",
                json!({"item_id": id, "output_index": idx,
                       "summary_index": 0, "text": text}),
            ),
            Item::Call { .. } => ("", Value::Null),
        };
        if !done_event.is_empty() {
            self.emit(done_event, payload);
        }
        self.emit(
            "response.output_item.done",
            json!({"output_index": idx, "item": self.items[idx].json()}),
        );
    }

    /// End of upstream stream (or [DONE]): close the open item, emit the
    /// accumulated tool calls, then the terminal response event.
    fn finalize(&mut self) {
        if self.finalized {
            return;
        }
        self.finalized = true;
        self.close_open();
        for i in 0..self.items.len() {
            if matches!(self.items[i], Item::Call { .. }) {
                let item = self.items[i].json();
                self.emit(
                    "response.output_item.added",
                    json!({"output_index": i, "item": item}),
                );
                self.emit(
                    "response.output_item.done",
                    json!({"output_index": i, "item": item}),
                );
            }
        }
        let output: Vec<Value> = self.items.iter().map(|it| it.json()).collect();
        let mut upstream = json!({"model": self.model});
        if let Some(u) = &self.usage {
            upstream["usage"] = u.clone();
        }
        let body = build_response(&upstream, output, &self.finish, self.upstream_error);
        let event = match body["status"].as_str().unwrap_or("completed") {
            "incomplete" => "response.incomplete",
            "failed" => "response.failed",
            _ => "response.completed",
        };
        self.emit(event, body);
        self.done = true;
    }
}

/// Find the first SSE event boundary (`\n\n` or `\r\n\r\n`) in `buf`;
/// returns (event_end, delimiter_len).
fn find_event_end(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == b'\n' && buf[i + 1] == b'\n' {
            return Some((i, 2));
        }
        if i + 3 < buf.len()
            && buf[i] == b'\r'
            && buf[i + 1] == b'\n'
            && buf[i + 2] == b'\r'
            && buf[i + 3] == b'\n'
        {
            return Some((i, 4));
        }
        i += 1;
    }
    None
}
