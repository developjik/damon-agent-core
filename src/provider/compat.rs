//! OpenAI-shaped request shaping driven by `ProviderCompat` flags.
//! Applied inside the openai-completions transport (and reused for
//! extra_body merging by openai-responses).

use std::collections::HashMap;

use serde_json::Value;

use crate::config::ProviderCompat;

/// Apply compat flags to an OpenAI chat-completions request body in place.
/// `body` must already be a JSON object with a `messages` array.
pub fn apply(body: &mut Value, compat: &ProviderCompat) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };

    // Thinking level: `model:low|medium|high` was split upstream and rides
    // here as `_thinking`. Map to reasoning_effort; drop the private key.
    if let Some(level) = obj
        .remove("_thinking")
        .and_then(|v| v.as_str().map(String::from))
    {
        obj.insert("reasoning_effort".into(), Value::String(level));
    }

    // store
    if compat.supports_store {
        obj.insert("store".into(), Value::Bool(false));
    }

    // max_tokens field rename
    if let Some(field) = &compat.max_tokens_field
        && field != "max_tokens"
        && let Some(v) = obj.remove("max_tokens")
    {
        obj.insert(field.clone(), v);
    }

    // streaming usage
    if compat.supports_usage_in_streaming
        && obj.get("stream").and_then(Value::as_bool) == Some(true)
    {
        obj.insert(
            "stream_options".into(),
            serde_json::json!({"include_usage": true}),
        );
    }

    // extra_body merge (top-level keys)
    for (k, v) in &compat.extra_body {
        obj.insert(k.clone(), v.clone());
    }

    // message-level transforms
    if let Some(messages) = obj.get_mut("messages").and_then(Value::as_array_mut) {
        shape_messages(messages, compat);
    }
}

fn shape_messages(messages: &mut Vec<Value>, compat: &ProviderCompat) {
    // Mistral tool ids: normalize every tool_call id + matching tool_call_id.
    if compat.requires_mistral_tool_ids {
        normalize_tool_ids(messages);
    }

    // Build tool_call_id → name map for requires_tool_result_name.
    let mut names: HashMap<String, String> = HashMap::new();
    if compat.requires_tool_result_name {
        for m in messages.iter() {
            if m["role"].as_str() != Some("assistant") {
                continue;
            }
            for tc in m["tool_calls"].as_array().into_iter().flatten() {
                if let (Some(id), Some(name)) = (tc["id"].as_str(), tc["function"]["name"].as_str())
                {
                    names.insert(id.to_string(), name.to_string());
                }
            }
        }
    }

    let mut out: Vec<Value> = Vec::with_capacity(messages.len());
    for m in messages.drain(..) {
        let mut m = m;
        let role = m["role"].as_str().unwrap_or("").to_string();

        // developer role rewrite
        if compat.supports_developer_role && role == "system" {
            m["role"] = Value::String("developer".into());
        }

        // tool result name
        if compat.requires_tool_result_name
            && role == "tool"
            && let Some(id) = m["tool_call_id"].as_str()
            && let Some(name) = names.get(id)
        {
            m.as_object_mut()
                .unwrap()
                .insert("name".into(), Value::String(name.clone()));
        }

        // coalesce consecutive same-role messages when the endpoint
        // can't handle multiple system/developer entries
        if !compat.supports_multiple_system_messages
            && let Some(prev) = out.last_mut()
        {
            let prev_role = prev["role"].as_str().unwrap_or("");
            let cur_role = m["role"].as_str().unwrap_or("");
            let mergeable = matches!(prev_role, "system" | "developer") && prev_role == cur_role;
            if mergeable {
                let prev_text = crate::provider::content_text(&prev["content"]);
                let cur_text = crate::provider::content_text(&m["content"]);
                prev["content"] = Value::String(format!("{prev_text}\n{cur_text}"));
                continue;
            }
        }

        out.push(m);
    }
    *messages = out;
}

/// Rewrite every tool_call id and tool_call_id to a 9-char alphanumeric
/// form, keeping call→result pairing consistent.
fn normalize_tool_ids(messages: &mut [Value]) {
    let mut map: HashMap<String, String> = HashMap::new();
    for m in messages.iter_mut() {
        if m["role"].as_str() == Some("assistant") {
            for tc in m["tool_calls"].as_array_mut().into_iter().flatten() {
                if let Some(id) = tc["id"].as_str() {
                    let short = mistral_id(id);
                    map.insert(id.to_string(), short.clone());
                    tc["id"] = Value::String(short);
                }
            }
        }
    }
    for m in messages.iter_mut() {
        if m["role"].as_str() == Some("tool")
            && let Some(id) = m["tool_call_id"].as_str()
            && let Some(short) = map.get(id)
        {
            m["tool_call_id"] = Value::String(short.clone());
        }
    }
}

/// Deterministic 9-char alphanumeric id derived from the original.
fn mistral_id(id: &str) -> String {
    const ALNUM: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    // FNV-1a over the original id, expanded to 9 chars.
    let mut h: u64 = 0xcbf29ce484222325;
    for b in id.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    let mut out = String::with_capacity(9);
    for i in 0..9 {
        h ^= h >> 13;
        h = h.wrapping_mul(0x5bd1e995);
        out.push(ALNUM[(h as usize + i * 7) % ALNUM.len()] as char);
    }
    out
}
