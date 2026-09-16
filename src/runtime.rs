use std::sync::Arc;

use anyhow::Context;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::api::AppState;
use crate::config::Config;
use crate::llm::{self, StreamEvent, ToolCallAccumulator};
use crate::store::StoredMessage;

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
    model: Option<&str>,
    client: &Arc<dyn ClientChannel>,
    cancel: CancellationToken,
) -> anyhow::Result<llm::StopReason> {
    state
        .store
        .append(
            session_id,
            "user",
            &json!({"role": "user", "content": text}),
        )
        .await?;

    // Model resolution order: per-prompt param → session default (stored
    // by session/new) → provider's default_model. Read the session row
    // before taking the config lock so no lock is held across .await.
    let session_model = state.store.session_model(session_id).await?;
    let requested = model.or(session_model.as_deref()).map(String::from);

    for _ in 0..MAX_ITERATIONS {
        if cancel.is_cancelled() {
            return Ok(llm::StopReason::Other);
        }
        let tools = state.mcp.openai_tools();
        let (provider, model, thinking) = {
            let cfg = state.config.read();
            let providers = state.providers.read();
            let discovered = state.discovered.read();
            // A `:low|:medium|:high` suffix is a thinking-level selector —
            // strip it before routing, same as the /v1 path.
            let (req_model, req_thinking) = requested
                .as_deref()
                .map(Config::split_thinking_level)
                .unwrap_or(("", None));
            let resolved = if req_model.is_empty() {
                cfg.default_provider().map(|(n, p)| {
                    // default_model may itself carry a thinking suffix.
                    let raw = p.default_model.clone().unwrap_or_else(|| "default".into());
                    let (m, t) = Config::split_thinking_level(&raw);
                    (n.to_string(), m.to_string(), t.map(String::from))
                })
            } else {
                // Strict route (prefix/glob), then discovered ids, then the
                // default provider still gets the requested model name —
                // mirrors the /v1 forward path.
                cfg.route_model_strict(req_model)
                    .map(|(name, _, upstream)| (name.to_string(), upstream))
                    .or_else(|| {
                        discovered.iter().find_map(|(name, ids)| {
                            ids.iter()
                                .any(|id| id == req_model)
                                .then(|| (name.clone(), req_model.to_string()))
                        })
                    })
                    .or_else(|| {
                        cfg.default_provider()
                            .map(|(n, _)| (n.to_string(), req_model.to_string()))
                    })
                    .map(|(n, m)| (n, m, req_thinking.map(String::from)))
            };
            let (name, model, thinking) = resolved.context("no provider configured")?;
            let provider = providers
                .get(&name)
                .cloned()
                .context("provider not built")?;
            (provider, model, thinking)
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
        let mut thinking_blocks: Vec<Value> = Vec::new();
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
                // Complete provider thinking block — persisted with the
                // assistant message so it can be replayed next request.
                Ok(StreamEvent::ThinkingBlock(b)) => thinking_blocks.push(b),
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
        // Persisted thinking blocks ride on the assistant message — the
        // provider needs them verbatim on the next request.
        let thinking_json = if thinking_blocks.is_empty() {
            Value::Null
        } else {
            json!(thinking_blocks)
        };
        if calls.is_empty() {
            let mut msg = json!({"role": "assistant", "content": text_out});
            if !thinking_json.is_null() {
                msg["thinking"] = thinking_json;
            }
            state.store.append(session_id, "assistant", &msg).await?;
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
        let mut msg = json!({
            "role": "assistant",
            "content": if text_out.is_empty() { Value::Null } else { json!(text_out) },
            "tool_calls": tool_calls_json,
        });
        if !thinking_json.is_null() {
            msg["thinking"] = thinking_json;
        }
        state.store.append(session_id, "assistant", &msg).await?;

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
            let result = execute_tool(state, session_id, call, client, &cancel).await;
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

    let Ok((compacted_through, prev_summary)) = state.store.compaction(session_id).await else {
        return;
    };
    let Ok(full) = state.store.messages_full(session_id).await else {
        return;
    };
    // Only the uncompacted tail still costs context — counting the whole
    // log would re-trigger compaction on every turn.
    let tail: Vec<&StoredMessage> = full.iter().filter(|m| m.id > compacted_through).collect();
    let mut est = estimate_tokens(&tail.iter().map(|m| m.data.clone()).collect::<Vec<_>>());
    if let Some(s) = &prev_summary {
        est += (s.len() / 4) as u64;
    }
    if est < window * 85 / 100 {
        return;
    }

    // Split: keep the newest ~50% of the tail, summarize the rest. The
    // boundary must not leave a role:"tool" response at the head of the
    // kept half — its assistant tool_calls message would be summarized
    // away, orphaning it and 400ing every later turn.
    let mut keep_from = tail.len() / 2;
    while keep_from < tail.len() && tail[keep_from].data["role"] == "tool" {
        keep_from += 1;
    }
    let dropped = &tail[..keep_from];
    if dropped.is_empty() {
        return;
    }
    let through_id = dropped.last().unwrap().id;

    // Build a summarization prompt from the dropped messages, carrying the
    // previous summary forward so context is never lost across compactions.
    let mut transcript = String::new();
    if let Some(s) = &prev_summary {
        transcript.push_str(&format!("previous summary: {s}\n"));
    }
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
    cancel: &CancellationToken,
) -> anyhow::Result<Value> {
    if !state.mcp.has_tool(&call.name) {
        anyhow::bail!("unknown tool {}", call.name);
    }
    if !state.mcp.auto_approve(&call.name) {
        // Race the permission round-trip against cancellation — a silent
        // client must not stall the turn forever.
        let granted = tokio::select! {
            _ = cancel.cancelled() => false,
            g = request_permission(state, session_id, call, client) => g,
        };
        if !granted {
            anyhow::bail!("permission denied for {}", call.name);
        }
    }
    let args: Value = llm::parse_partial_json(&call.arguments);
    tokio::select! {
        _ = cancel.cancelled() => anyhow::bail!("cancelled"),
        r = state.mcp.call(&call.name, args) => r,
    }
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
