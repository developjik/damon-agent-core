/// One parsed event from a streaming chat completion, normalized across
/// provider adapters to the OpenAI delta model.
#[derive(Debug)]
pub enum StreamEvent {
    /// Text delta for the assistant message.
    Text(String),
    /// Reasoning/thinking delta (o-series, Claude thinking, Gemini thought).
    Thinking(String),
    /// A complete provider thinking block, stored verbatim for round-trip.
    /// Anthropic requires prior thinking blocks (with signature) to be
    /// replayed on the next request when thinking is enabled.
    ThinkingBlock(serde_json::Value),
    /// Incremental tool-call fragment, keyed by its `index` in the chunk.
    ToolCallDelta {
        index: usize,
        id: Option<String>,
        name: Option<String>,
        arguments: String,
    },
    /// Token usage reported by the provider (usually the final chunk).
    Usage { input: u64, output: u64 },
    /// Stream ended with a normalized stop reason.
    Done(StopReason),
}

/// Why the model stopped, normalized across providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// Natural end of turn.
    Stop,
    /// Hit a token limit.
    Length,
    /// Stopped to make tool calls.
    ToolCalls,
    /// The agent loop hit its per-turn tool-call round-trip cap.
    MaxTurnRequests,
    /// Provider-specific or unknown reason.
    Other,
}

/// A fully accumulated tool call after streaming completes.
#[derive(Debug, Clone)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    /// Raw JSON arguments string as produced by the model.
    pub arguments: String,
}

/// Accumulate streamed tool-call deltas into complete calls.
#[derive(Default)]
pub struct ToolCallAccumulator {
    slots: Vec<(Option<String>, Option<String>, String)>,
}

impl ToolCallAccumulator {
    /// A hostile or buggy upstream can send a huge tool_call `index` —
    /// growing `slots` to it would OOM the daemon. Real responses never
    /// carry more than a few dozen calls; deltas past the cap are dropped.
    const MAX_INDEX: usize = 256;

    pub fn push(&mut self, index: usize, id: Option<String>, name: Option<String>, args: &str) {
        if index >= Self::MAX_INDEX {
            return;
        }
        while self.slots.len() <= index {
            self.slots.push((None, None, String::new()));
        }
        let slot = &mut self.slots[index];
        if let Some(id) = id {
            slot.0 = Some(id);
        }
        if let Some(name) = name {
            slot.1 = Some(name);
        }
        slot.2.push_str(args);
    }

    pub fn finish(self) -> Vec<ToolCall> {
        // Slots that never received an id AND name are padding from sparse
        // provider indices (e.g. Anthropic content-block indexes where a
        // text block precedes tool_use) — emitting them would persist a
        // fake call with empty id/name and poison the session history.
        self.slots
            .into_iter()
            .filter(|(id, name, _)| id.is_some() && name.is_some())
            .map(|(id, name, arguments)| ToolCall {
                id: id.unwrap_or_default(),
                name: name.unwrap_or_default(),
                arguments,
            })
            .collect()
    }
}

/// Parse possibly-incomplete JSON from streamed tool arguments.
/// Tries strict parsing first, then repairs by closing open strings and
/// brackets. A member left dangling by a cut mid-pair is dropped back to
/// its leading comma so earlier pairs survive. Returns `{}` when nothing
/// salvageable remains.
pub fn parse_partial_json(s: &str) -> serde_json::Value {
    let s = s.trim();
    if s.is_empty() {
        return serde_json::json!({});
    }
    if let Ok(v) = serde_json::from_str(s) {
        return v;
    }
    // Repair: track open brackets and string state, then close them. If the
    // result still fails to parse, drop the trailing member back to its
    // leading comma and retry.
    let mut end = s.len();
    loop {
        let mut stack: Vec<char> = Vec::new();
        let mut in_string = false;
        let mut escape = false;
        let mut last_comma: Option<usize> = None;
        for (i, c) in s[..end].char_indices() {
            if escape {
                escape = false;
                continue;
            }
            match c {
                '\\' if in_string => escape = true,
                '"' => in_string = !in_string,
                ',' if !in_string => last_comma = Some(i),
                '{' | '[' if !in_string => stack.push(c),
                '}' if !in_string && stack.last() == Some(&'{') => {
                    stack.pop();
                }
                ']' if !in_string && stack.last() == Some(&'[') => {
                    stack.pop();
                }
                _ => {}
            }
        }
        let mut repaired = s[..end].to_string();
        if in_string {
            repaired.push('"');
        }
        // Drop a trailing key or colon left dangling by a cut mid-pair.
        while repaired.ends_with(':') || repaired.ends_with(',') {
            repaired.pop();
        }
        for open in stack.iter().rev() {
            repaired.push(match open {
                '{' => '}',
                '[' => ']',
                _ => unreachable!(),
            });
        }
        if let Ok(v) = serde_json::from_str(&repaired) {
            return v;
        }
        match last_comma {
            // Cut the dangling member at its leading comma and try again.
            Some(i) => end = i,
            // Nothing left to salvage.
            None => return serde_json::json!({}),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_partial_json;

    /// A stream cut after a complete key must not discard earlier pairs.
    #[test]
    fn salvages_pairs_before_dangling_key() {
        assert_eq!(
            parse_partial_json(r#"{"path":"/tmp/x","mode""#),
            serde_json::json!({"path": "/tmp/x"})
        );
        // Cut right after the key's colon.
        assert_eq!(
            parse_partial_json(r#"{"path":"/tmp/x","mode":"#),
            serde_json::json!({"path": "/tmp/x"})
        );
    }

    /// Complete JSON must pass through untouched.
    #[test]
    fn complete_json_passes_through_unchanged() {
        assert_eq!(
            parse_partial_json(r#"{"path":"/tmp/x","mode":"w","n":3}"#),
            serde_json::json!({"path": "/tmp/x", "mode": "w", "n": 3})
        );
    }

    /// An unterminated string value is closed and kept.
    #[test]
    fn repairs_unterminated_string_value() {
        assert_eq!(
            parse_partial_json(r#"{"path":"/tmp/unclosed"#),
            serde_json::json!({"path": "/tmp/unclosed"})
        );
    }

    /// Nothing salvageable falls back to an empty object.
    #[test]
    fn unsalvageable_input_falls_back_to_empty_object() {
        assert_eq!(parse_partial_json("{"), serde_json::json!({}));
        assert_eq!(parse_partial_json("not json"), serde_json::json!({}));
        assert_eq!(parse_partial_json(r#"{"only-key""#), serde_json::json!({}));
    }
}
