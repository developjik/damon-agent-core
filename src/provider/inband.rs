//! In-band tool dialect for models without a native tool API.
//! Tools are rendered into the system prompt; the model replies with
//! <tool_call>{"name":..., "arguments":{...}}</tool_call> blocks in text,
//! which we parse back into tool calls.

use serde_json::{Value, json};

/// Render `tools` into a system prompt and strip the field so the endpoint
/// never sees a tool schema it would reject.
pub fn render_tools(body: &mut Value) {
    let tools = match body["tools"].as_array() {
        Some(t) if !t.is_empty() => t.clone(),
        _ => return,
    };
    body.as_object_mut().map(|o| o.remove("tools"));
    body.as_object_mut().map(|o| o.remove("tool_choice"));

    let mut prompt = String::from(
        "You may call tools by emitting a block exactly like:\n\
         <tool_call>{\"name\": \"tool.name\", \"arguments\": {...}}</tool_call>\n\
         Emit one block per call. Do not narrate around it.\n\n\
         Available tools:\n",
    );
    for t in &tools {
        let f = &t["function"];
        prompt.push_str(&format!(
            "- {}: {}\n  parameters: {}\n",
            f["name"].as_str().unwrap_or(""),
            f["description"].as_str().unwrap_or(""),
            f["parameters"],
        ));
    }

    // Prepend to the first system message, or insert one.
    if let Some(msgs) = body["messages"].as_array_mut() {
        if let Some(sys) = msgs
            .iter_mut()
            .find(|m| m["role"].as_str() == Some("system"))
        {
            if sys["content"].is_array() {
                // Multimodal array content: append a text part — replacing
                // it with a flat string would destroy the original parts.
                sys["content"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"type": "text", "text": prompt}));
            } else {
                let prev = sys["content"].as_str().unwrap_or("").to_string();
                sys["content"] = Value::String(format!("{prev}\n\n{prompt}"));
            }
        } else {
            msgs.insert(0, json!({"role": "system", "content": prompt}));
        }
    }
}

/// Extract <tool_call> blocks from text. Returns (clean_text, calls).
/// Malformed blocks are left in the text.
pub fn extract_tool_calls(text: &str) -> (String, Vec<(String, String)>) {
    let mut clean = String::new();
    let mut calls = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("<tool_call>") {
        clean.push_str(&rest[..start]);
        let after = &rest[start + "<tool_call>".len()..];
        match after.find("</tool_call>") {
            Some(end) => {
                let payload = after[..end].trim();
                let block_len = "<tool_call>".len() + end + "</tool_call>".len();
                if let Ok(v) = serde_json::from_str::<Value>(payload) {
                    let name = v["name"].as_str().unwrap_or("").to_string();
                    let args = match &v["arguments"] {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    if !name.is_empty() {
                        calls.push((name, args));
                    } else {
                        clean.push_str(&rest[start..start + block_len]);
                    }
                } else {
                    // Not valid JSON — keep the whole block in the text.
                    clean.push_str(&rest[start..start + block_len]);
                }
                rest = &rest[start + block_len..];
            }
            None => {
                // Unterminated block — keep it as text.
                clean.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    clean.push_str(rest);
    (clean.trim().to_string(), calls)
}

/// Post-process a buffered event stream: extract <tool_call> blocks from the
/// accumulated text and re-emit as ToolCallDelta events before Done.
pub fn events_with_tool_calls(
    events: Vec<crate::llm::StreamEvent>,
) -> Vec<crate::llm::StreamEvent> {
    use crate::llm::StreamEvent;
    let mut text = String::new();
    let mut out = Vec::new();
    // Output position of the first Text chunk — the cleaned text must be
    // re-inserted there, not at index 0, or events that arrived earlier
    // (thinking, usage) end up after the response body.
    let mut text_pos: Option<usize> = None;
    for ev in events {
        match ev {
            StreamEvent::Text(t) => {
                if text_pos.is_none() {
                    text_pos = Some(out.len());
                }
                text.push_str(&t);
            }
            other => out.push(other),
        }
    }
    let (clean, calls) = extract_tool_calls(&text);
    if !clean.is_empty() {
        out.insert(text_pos.unwrap_or(0), StreamEvent::Text(clean));
    }
    let done_pos = out
        .iter()
        .position(|e| matches!(e, StreamEvent::Done(_)))
        .unwrap_or(out.len());
    for (i, (name, args)) in calls.into_iter().enumerate() {
        out.insert(
            done_pos + i,
            StreamEvent::ToolCallDelta {
                index: i,
                id: Some(format!("call_{i}")),
                name: Some(name),
                arguments: args,
            },
        );
    }
    out
}

/// Post-process a non-streaming response: extract <tool_call> blocks from
/// message content into proper tool_calls.
pub fn response_with_tool_calls(v: &mut Value) {
    let Some(msg) = v["choices"].get_mut(0).map(|c| &mut c["message"]) else {
        return;
    };
    let Some(content) = msg["content"].as_str() else {
        return;
    };
    let (clean, calls) = extract_tool_calls(content);
    if calls.is_empty() {
        return;
    }
    msg["content"] = Value::String(clean);
    msg["tool_calls"] = json!(
        calls
            .into_iter()
            .enumerate()
            .map(|(i, (name, args))| json!({
                "id": format!("call_{i}"),
                "type": "function",
                "function": {"name": name, "arguments": args},
            }))
            .collect::<Vec<_>>()
    );
    v["choices"][0]["finish_reason"] = json!("tool_calls");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::StreamEvent;
    use serde_json::json;

    /// An array (multimodal) system message keeps its parts — the tool
    /// prompt is appended as one more text part, never a flat replacement.
    #[test]
    fn render_tools_appends_text_part_to_array_system() {
        let mut body = json!({
            "tools": [{"function": {"name": "t", "description": "d", "parameters": {}}}],
            "messages": [
                {"role": "system", "content": [{"type": "text", "text": "orig"}]},
                {"role": "user", "content": "hi"},
            ],
        });
        render_tools(&mut body);
        let content = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content[0]["text"], "orig", "original part preserved");
        assert_eq!(content[1]["type"], "text");
        assert!(
            content[1]["text"]
                .as_str()
                .unwrap()
                .contains("Available tools:")
        );
        assert_eq!(body.get("tools"), None, "tools stripped from the wire body");
    }

    /// The cleaned text re-enters at the first Text chunk's position, so
    /// earlier events (thinking) stay ahead of the response body.
    #[test]
    fn clean_text_inserted_at_first_text_position() {
        let events = vec![
            StreamEvent::Thinking("hmm".into()),
            StreamEvent::Text(
                "hello <tool_call>{\"name\": \"f\", \"arguments\": {}}</tool_call>".into(),
            ),
            StreamEvent::Usage {
                input: 1,
                output: 1,
            },
            StreamEvent::Done(crate::llm::StopReason::Stop),
        ];
        let out = events_with_tool_calls(events);
        assert!(matches!(&out[0], StreamEvent::Thinking(t) if t == "hmm"));
        assert!(matches!(&out[1], StreamEvent::Text(t) if t == "hello"));
        assert!(matches!(&out[2], StreamEvent::Usage { .. }));
        assert!(matches!(
            &out[3],
            StreamEvent::ToolCallDelta { name: Some(n), .. } if n == "f"
        ));
        assert!(matches!(&out[4], StreamEvent::Done(_)));
    }
}
