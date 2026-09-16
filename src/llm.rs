/// One parsed event from a streaming chat completion, normalized across
/// provider adapters to the OpenAI delta model.
#[derive(Debug)]
pub enum StreamEvent {
    /// Text delta for the assistant message.
    Text(String),
    /// Reasoning/thinking delta (o-series, Claude thinking, Gemini thought).
    Thinking(String),
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
    pub fn push(&mut self, index: usize, id: Option<String>, name: Option<String>, args: &str) {
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
        self.slots
            .into_iter()
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
/// brackets. Returns `{}` when nothing salvageable remains.
pub fn parse_partial_json(s: &str) -> serde_json::Value {
    let s = s.trim();
    if s.is_empty() {
        return serde_json::json!({});
    }
    if let Ok(v) = serde_json::from_str(s) {
        return v;
    }
    // Repair: track open brackets and string state, then close them.
    let mut stack: Vec<char> = Vec::new();
    let mut in_string = false;
    let mut escape = false;
    for c in s.chars() {
        if escape {
            escape = false;
            continue;
        }
        match c {
            '\\' if in_string => escape = true,
            '"' => in_string = !in_string,
            '{' | '[' if !in_string => stack.push(c),
            '}' if !in_string => {
                if stack.last() == Some(&'{') {
                    stack.pop();
                }
            }
            ']' if !in_string => {
                if stack.last() == Some(&'[') {
                    stack.pop();
                }
            }
            _ => {}
        }
    }
    let mut repaired = s.to_string();
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
    serde_json::from_str(&repaired).unwrap_or_else(|_| serde_json::json!({}))
}
