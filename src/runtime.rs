use std::sync::Arc;

use anyhow::Context;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::api::AppState;
use crate::config::Config;
use crate::llm::{self, StreamEvent, ToolCallAccumulator};

/// Max tool-call round trips per prompt turn.
const MAX_ITERATIONS: usize = 25;

/// No upstream event for this long → the stream is stalled; abort the turn.
const STREAM_STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Tool results are truncated to this many bytes before persisting —
/// unbounded tool output is the fastest way to blow the context window.
const MAX_TOOL_OUTPUT: usize = 8 * 1024;

/// Channel back to the connected client: session updates and
/// server-initiated requests (permission prompts).
#[async_trait::async_trait]
pub trait ClientChannel: Send + Sync {
    async fn notify(&self, method: &str, params: Value);
    async fn request(&self, method: &str, params: Value) -> anyhow::Result<Value>;
}

/// Run one prompt turn: append the user message, stream the model,
/// execute tool calls (with permission), repeat until the model stops.
/// Emits ACP-shaped `session/update` notifications along the way.
pub async fn run_prompt(
    state: &Arc<AppState>,
    session_id: &str,
    text: &str,
    client: &Arc<dyn ClientChannel>,
    cancel: CancellationToken,
) -> anyhow::Result<llm::StopReason> {
    state
        .store
        .append(session_id, "user", &json!({"role": "user", "content": text}))
        .await?;

    for _ in 0..MAX_ITERATIONS {
        if cancel.is_cancelled() {
            return Ok(llm::StopReason::Other);
        }
        let tools = state.mcp.openai_tools();
        let (provider, model, thinking) = {
            let cfg = state.config.read();
            let (name, pcfg) = cfg.default_provider().context("no provider configured")?;
            let raw_model = pcfg
                .default_model
                .clone()
                .unwrap_or_else(|| "default".to_string());
            let (model, thinking) = Config::split_thinking_level(&raw_model);
            let provider = state
                .providers
                .read()
                .get(name)
                .cloned()
                .context("provider not built")?;
            (provider, model.to_string(), thinking.map(String::from))
        };

        // Compact before loading messages: if the estimated token count
        // exceeds 85% of the model's context window, summarize the oldest
        // half via the provider and record the cutoff.
        maybe_compact(state, session_id, &provider, &model).await;

        let messages = state.store.messages(session_id).await?;
        let mut body = serde_json::json!({
            "model": model,
            "messages": messages,
            "tools": tools,
        });
        if let Some(level) = &thinking {
            body["_thinking"] = json!(level);
        }
        // Retry the initial request once — a transient connect/5xx error
        // should not kill the whole turn.
        let stream = match provider.chat_stream(body.clone()).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "chat_stream failed; retrying once");
                provider.chat_stream(body).await?
            }
        };
        let mut stream = std::pin::pin!(stream);
        let mut text_out = String::new();
        let mut acc = ToolCallAccumulator::default();

        use futures::StreamExt;
        let mut stop = llm::StopReason::Other;
        let mut stream_err: Option<anyhow::Error> = None;
        loop {
            // select! so cancel and a stall timeout fire even when the
            // upstream is silent — a pending stream.next() alone would
            // wait forever.
            let next = tokio::select! {
                _ = cancel.cancelled() => None,
                _ = tokio::time::sleep(STREAM_STALL_TIMEOUT) => {
                    stream_err = Some(anyhow::anyhow!(
                        "upstream stream stalled for {}s",
                        STREAM_STALL_TIMEOUT.as_secs()
                    ));
                    None
                }
                ev = stream.next() => ev,
            };
            let Some(ev) = next else { break };
            match ev {
                Ok(StreamEvent::Text(t)) => {
                    text_out.push_str(&t);
                    client
                        .notify(
                            "session/update",
                            json!({
                                "sessionId": session_id,
                                "update": {
                                    "sessionUpdate": "agent_message_chunk",
                                    "content": {"type": "text", "text": t}
                                }
                            }),
                        )
                        .await;
                }
                Ok(StreamEvent::Thinking(t)) => {
                    client
                        .notify(
                            "session/update",
                            json!({
                                "sessionId": session_id,
                                "update": {
                                    "sessionUpdate": "agent_thought_chunk",
                                    "content": {"type": "text", "text": t}
                                }
                            }),
                        )
                        .await;
                }
                Ok(StreamEvent::ToolCallDelta {
                    index,
                    id,
                    name,
                    arguments,
                }) => acc.push(index, id, name, &arguments),
                Ok(StreamEvent::Usage { .. }) => {}
                Ok(StreamEvent::Done(r)) => {
                    stop = r;
                    break;
                }
                Err(e) => {
                    stream_err = Some(e);
                    break;
                }
            }
        }

        // Cancelled or stream error: persist whatever text was produced so
        // the client-visible output and the session history agree. Partial
        // tool calls are dropped — persisting them without responses would
        // poison every later turn.
        if cancel.is_cancelled() || stream_err.is_some() {
            if !text_out.is_empty() {
                let _ = state
                    .store
                    .append(
                        session_id,
                        "assistant",
                        &json!({"role": "assistant", "content": text_out}),
                    )
                    .await;
            }
            if let Some(e) = stream_err {
                return Err(e);
            }
            return Ok(llm::StopReason::Other);
        }

        let calls = acc.finish();
        if calls.is_empty() {
            state
                .store
                .append(
                    session_id,
                    "assistant",
                    &json!({"role": "assistant", "content": text_out}),
                )
                .await?;
            return Ok(stop);
        }

        // Persist the assistant turn with its tool calls, then execute each.
        let tool_calls_json: Vec<Value> = calls
            .iter()
            .map(|c| {
                json!({
                    "id": c.id,
                    "type": "function",
                    "function": {"name": c.name, "arguments": c.arguments}
                })
            })
            .collect();
        state
            .store
            .append(
                session_id,
                "assistant",
                &json!({
                    "role": "assistant",
                    "content": if text_out.is_empty() { Value::Null } else { json!(text_out) },
                    "tool_calls": tool_calls_json,
                }),
            )
            .await?;

        // Every persisted tool_call MUST get a matching role:"tool" row —
        // an orphan poisons every later turn ("tool_calls without tool
        // responses" 400). Track completions and force-append "cancelled"
        // for any call that never got a response.
        let mut completed = 0usize;
        let mut tool_err: Option<anyhow::Error> = None;
        for call in &calls {
            if cancel.is_cancelled() {
                break;
            }
            let result = execute_tool(state, session_id, call, client).await;
            let mut content = match &result {
                Ok(v) => serde_json::to_string(v).unwrap_or_default(),
                Err(e) => format!("error: {e:#}"),
            };
            if content.len() > MAX_TOOL_OUTPUT {
                let mut end = MAX_TOOL_OUTPUT;
                while !content.is_char_boundary(end) {
                    end -= 1;
                }
                content.truncate(end);
                content.push_str("\n… [truncated]");
            }
            if let Err(e) = state
                .store
                .append(
                    session_id,
                    "tool",
                    &json!({
                        "role": "tool",
                        "tool_call_id": call.id,
                        "content": content,
                    }),
                )
                .await
            {
                tool_err = Some(e);
                break;
            }
            completed += 1;
            client
                .notify(
                    "session/update",
                    json!({
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "tool_call_update",
                            "toolCallId": call.id,
                            "status": if result.is_ok() { "completed" } else { "failed" },
                        }
                    }),
                )
                .await;
        }
        // Repair: any call that never got a response gets a "cancelled"
        // tool row so the next turn's history is well-formed.
        for call in &calls[completed..] {
            let _ = state
                .store
                .append(
                    session_id,
                    "tool",
                    &json!({
                        "role": "tool",
                        "tool_call_id": call.id,
                        "content": "cancelled",
                    }),
                )
                .await;
        }
        if let Some(e) = tool_err {
            return Err(e);
        }
        if cancel.is_cancelled() {
            return Ok(llm::StopReason::Other);
        }
    }
    warn!(session_id, "hit max tool iterations");
    Ok(llm::StopReason::Other)
}

/// Rough token estimate: ~4 chars per token for mixed text/code.
fn estimate_tokens(messages: &[Value]) -> u64 {
    messages
        .iter()
        .map(|m| {
            let content = m["content"].as_str().map(|s| s.len()).unwrap_or(0)
                + m["tool_calls"]
                    .as_array()
                    .map(|t| t.iter().map(|c| c.to_string().len()).sum::<usize>())
                    .unwrap_or(0);
            (content / 4) as u64
        })
        .sum()
}

/// If the session's estimated tokens exceed 85% of the model's context
/// window, summarize the oldest half of messages via the provider and
/// record the compaction. Failures are logged, never fatal — the turn
/// proceeds with the full history.
async fn maybe_compact(
    state: &Arc<AppState>,
    session_id: &str,
    provider: &Arc<crate::provider::Provider>,
    model: &str,
) {
    let window = {
        let cfg = state.config.read();
        cfg.model_meta(model).context_window
    };
    let Some(window) = window else { return };

    let Ok(full) = state.store.messages_full(session_id).await else {
        return;
    };
    let messages: Vec<Value> = full.iter().map(|m| m.data.clone()).collect();
    let est = estimate_tokens(&messages);
    if est < window * 85 / 100 {
        return;
    }

    // Split: keep the newest ~50% of messages, summarize the rest.
    let keep_from = full.len() / 2;
    let dropped = &full[..keep_from];
    if dropped.is_empty() {
        return;
    }
    let through_id = dropped.last().unwrap().id;

    // Build a summarization prompt from the dropped messages.
    let mut transcript = String::new();
    for m in dropped {
        let role = m.data["role"].as_str().unwrap_or("?");
        let content = m.data["content"].as_str().unwrap_or("");
        let content = if content.len() > 2000 {
            &content[..content.floor_char_boundary(2000)]
        } else {
            content
        };
        transcript.push_str(&format!("{role}: {content}\n"));
    }
    let prompt = format!(
        "Summarize this conversation transcript in under 500 words. \
         Preserve: decisions made, tool calls and their outcomes, file paths, \
         errors encountered, and any state the next turn needs.\n\n{transcript}"
    );
    let req = json!({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
    });
    let summary = match provider.chat(req).await {
        Ok(v) => v["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        Err(e) => {
            warn!(session_id, error = %e, "compaction summarize failed; truncating");
            format!("[{keep_from} earlier messages removed — summarization failed]")
        }
    };
    if summary.is_empty() {
        return;
    }
    if let Err(e) = state
        .store
        .set_compaction(session_id, through_id, &summary)
        .await
    {
        warn!(session_id, error = %e, "failed to record compaction");
    }
}

async fn execute_tool(
    state: &Arc<AppState>,
    session_id: &str,
    call: &llm::ToolCall,
    client: &Arc<dyn ClientChannel>,
) -> anyhow::Result<Value> {
    if !state.mcp.has_tool(&call.name) {
        anyhow::bail!("unknown tool {}", call.name);
    }
    if !state.mcp.auto_approve(&call.name) {
        let granted = request_permission(state, session_id, call, client).await;
        if !granted {
            anyhow::bail!("permission denied for {}", call.name);
        }
    }
    let args: Value = llm::parse_partial_json(&call.arguments);
    state.mcp.call(&call.name, args).await
}

/// ACP session/request_permission round-trip. Failures and non-allow
/// outcomes both deny.
async fn request_permission(
    _state: &Arc<AppState>,
    session_id: &str,
    call: &llm::ToolCall,
    client: &Arc<dyn ClientChannel>,
) -> bool {
    let resp = client
        .request(
            "session/request_permission",
            json!({
                "sessionId": session_id,
                "toolCall": {
                    "toolCallId": call.id,
                    "title": call.name,
                    "rawInput": llm::parse_partial_json(&call.arguments),
                },
                "options": [
                    {"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
                    {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"},
                ],
            }),
        )
        .await;
    match resp {
        Ok(v) => v["outcome"]["optionId"]
            .as_str()
            .is_some_and(|id| id.starts_with("allow")),
        Err(_) => false,
    }
}
