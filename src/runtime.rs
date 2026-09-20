use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::{Context, bail};
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
/// Overridable via `config.max_tool_output`.
const MAX_TOOL_OUTPUT: usize = 8 * 1024;

/// An unanswered permission prompt is denied after this long — a silent
/// client must not stall the turn forever. Overridable via
/// `config.permission_timeout_secs`.
pub const PERMISSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Sentinel error marking a tool call aborted by turn cancellation.
/// Distinct from a real tool failure so the persisted tool row can say
/// "cancelled" without string-matching arbitrary error text.
#[derive(Debug)]
struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cancelled")
    }
}

impl std::error::Error for Cancelled {}

/// Channel back to the connected client: session updates and
/// server-initiated requests (permission prompts).
#[async_trait::async_trait]
pub trait ClientChannel: Send + Sync {
    async fn notify(&self, method: &str, params: Value);
    async fn request(&self, method: &str, params: Value) -> anyhow::Result<Value>;
    /// How long `request` may wait for the client's answer. Transports
    /// override this with the configured permission timeout (+slack) so a
    /// hardcoded cap can't silently deny prompts early.
    fn request_timeout(&self) -> std::time::Duration {
        PERMISSION_TIMEOUT
    }
}
/// Result of one prompt turn: the terminal stop reason plus the upstream
/// model that actually served the first iteration. Fallback routing can
/// make the routed model differ from the requested one, so clients need
/// this to show what really answered.
#[derive(Debug, Clone, PartialEq)]
pub struct PromptOutcome {
    pub stop_reason: llm::StopReason,
    /// Upstream model resolved on the FIRST iteration. Later iterations
    /// re-resolve (config may change mid-turn), but the first answer is
    /// the one that produced the turn's opening output — keep it.
    pub model: Option<String>,
}

/// Run one prompt turn: append the user message, stream the model,
/// execute tool calls (with permission), repeat until the model stops.
/// Emits ACP-shaped `session/update` notifications along the way.
pub async fn run_prompt(
    state: &Arc<AppState>,
    session_id: &str,
    content: &Value,
    model: Option<&str>,
    client: &Arc<dyn ClientChannel>,
    cancel: CancellationToken,
) -> anyhow::Result<PromptOutcome> {
    state.metrics.prompts_total.fetch_add(1, Ordering::Relaxed);
    state
        .store
        .append(
            session_id,
            "user",
            &json!({"role": "user", "content": content}),
        )
        .await?;

    // First user message becomes the session title (deterministic —
    // no extra model call). set_title_if_empty no-ops once a title
    // exists, so renames and later prompts never overwrite it.
    if let Some(title) = derive_title(content) {
        let _ = state.store.set_title_if_empty(session_id, &title).await;
    }

    // Model resolution order: per-prompt param → session default (stored
    // by session/new) → provider's default_model. Read the session row
    // before taking the config lock so no lock is held across .await.
    let session_model = state.store.session_model(session_id).await?;
    let requested = model.or(session_model.as_deref()).map(String::from);
    // Compaction is re-evaluated every iteration — every pass through the
    // loop appends messages (assistant row + one tool row per call), so
    // history can only grow within a turn.

    let mut model_used: Option<String> = None;
    for _ in 0..MAX_ITERATIONS {
        if cancel.is_cancelled() {
            return Ok(PromptOutcome {
                stop_reason: llm::StopReason::Other,
                model: None,
            });
        }
        // Global MCP tools plus this session's overlay servers (ACP
        // session/new mcpServers) — rebuilt each iteration so a config
        // reload or a deleted overlay can't leak stale tools in.
        let session_mcp = state.session_mcp.lock().await.get(session_id).cloned();
        let mcp = crate::mcp::McpView::new(&state.mcp, session_mcp);
        let tools = mcp.openai_tools();
        let ResolvedModel {
            provider_name,
            provider,
            model,
            thinking,
        } = resolve_provider_model(state, requested.as_deref())?;
        // First successful resolution wins — later iterations re-resolve
        // (config may change mid-turn) but must not rewrite what the
        // client was told answered.
        if model_used.is_none() {
            model_used = Some(model.clone());
        }

        // Compact before loading messages — every iteration appends, so
        // this runs each pass. maybe_compact itself early-returns cheaply
        // when the uncompacted tail is under 85% of the window.
        maybe_compact(state, session_id, &provider, &model, &cancel).await;

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
        // should not kill the whole turn. select! against cancel + a
        // TTFB deadline: without it a wedged upstream parks this await
        // forever, the live_prompts entry never frees, and the session
        // rejects every new prompt ("busy") until daemon restart.
        let ttfb = std::time::Duration::from_secs(120);
        let started = std::time::Instant::now();
        let stream = tokio::select! {
            _ = cancel.cancelled() => return Ok(PromptOutcome { stop_reason: llm::StopReason::Other, model: model_used.clone() }),
            r = tokio::time::timeout(ttfb, provider.chat_stream(body.clone())) => {
                match r {
                    Ok(Ok(s)) => {
                        state.metrics.record_provider(&provider_name, true, started.elapsed());
                        s
                    }
                    Ok(Err(e)) => {
                        state.metrics.record_provider(&provider_name, false, started.elapsed());
                        // A rate-limited upstream tells us how long to
                        // back off — honor it before the single retry.
                        if let Some(rl) = e.downcast_ref::<crate::provider::RateLimited>() {
                            warn!(wait = ?rl.0, "rate limited; backing off before retry");
                            tokio::select! {
                                _ = cancel.cancelled() => return Ok(PromptOutcome { stop_reason: llm::StopReason::Other, model: model_used.clone() }),
                                _ = tokio::time::sleep(rl.0) => {}
                            }
                        } else {
                            warn!(error = %e, "chat_stream failed; retrying once");
                        }
                        let retry_started = std::time::Instant::now();
                        let retried = tokio::select! {
                            _ = cancel.cancelled() => return Ok(PromptOutcome { stop_reason: llm::StopReason::Other, model: model_used.clone() }),
                            r = tokio::time::timeout(ttfb, provider.chat_stream(body)) => r,
                        };
                        match retried {
                            Ok(Ok(s)) => {
                                state.metrics.record_provider(&provider_name, true, retry_started.elapsed());
                                s
                            }
                            Ok(Err(e)) => {
                                state.metrics.record_provider(&provider_name, false, retry_started.elapsed());
                                return Err(e);
                            }
                            Err(_) => {
                                state.metrics.record_provider(&provider_name, false, retry_started.elapsed());
                                bail!("no response from upstream within {}s (retry)", ttfb.as_secs())
                            }
                        }
                    }
                    Err(_) => {
                        state.metrics.record_provider(&provider_name, false, started.elapsed());
                        bail!("no response from upstream within {}s", ttfb.as_secs())
                    }
                }
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
                Ok(StreamEvent::Usage { input, output }) => {
                    state
                        .metrics
                        .tokens_input
                        .fetch_add(input, Ordering::Relaxed);
                    state
                        .metrics
                        .tokens_output
                        .fetch_add(output, Ordering::Relaxed);
                }
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
        // Persisted thinking blocks ride on the assistant message — the
        // provider needs them verbatim on the next request.
        let thinking_json = if thinking_blocks.is_empty() {
            Value::Null
        } else {
            json!(thinking_blocks)
        };
        if cancel.is_cancelled() || stream_err.is_some() {
            if !text_out.is_empty() {
                let mut msg = json!({"role": "assistant", "content": text_out});
                if !thinking_json.is_null() {
                    msg["thinking"] = thinking_json.clone();
                }
                let _ = state.store.append(session_id, "assistant", &msg).await;
            }
            if let Some(e) = stream_err {
                return Err(e);
            }
            return Ok(PromptOutcome {
                stop_reason: llm::StopReason::Other,
                model: model_used,
            });
        }

        let calls = acc.finish();
        if calls.is_empty() {
            let mut msg = json!({"role": "assistant", "content": text_out});
            if !thinking_json.is_null() {
                msg["thinking"] = thinking_json;
            }
            state.store.append(session_id, "assistant", &msg).await?;
            return Ok(PromptOutcome {
                stop_reason: stop,
                model: model_used,
            });
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

        // Tell the client the calls started — without an in_progress
        // update, ACP clients can't render pending state and the first
        // signal they see is completion.
        for call in &calls {
            client
                .notify(
                    "session/update",
                    json!({
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "tool_call_update",
                            "toolCallId": call.id,
                            "status": "in_progress",
                        }
                    }),
                )
                .await;
        }
        // Independent tool calls run concurrently; results are collected in
        // call order so persisted tool rows keep the model's ordering.
        // Permission prompts serialize on a turn-local mutex — the client
        // must never see two prompts at once.
        let perm_lock = tokio::sync::Mutex::new(());
        let results =
            futures::future::join_all(calls.iter().map(|call| {
                execute_tool(state, &mcp, session_id, call, client, &cancel, &perm_lock)
            }))
            .await;

        // Every persisted tool_call MUST get a matching role:"tool" row —
        // an orphan poisons every later turn ("tool_calls without tool
        // responses" 400). A call that was still in flight when the turn
        // was cancelled is recorded as "cancelled"; calls that finished
        // keep their real result.
        let max_output = state
            .config
            .read()
            .max_tool_output
            .unwrap_or(MAX_TOOL_OUTPUT);
        let mut completed = 0usize;
        let mut tool_err: Option<anyhow::Error> = None;
        for (call, result) in calls.iter().zip(results) {
            let cancelled =
                cancel.is_cancelled() && matches!(&result, Err(e) if e.is::<Cancelled>());
            let content = if cancelled {
                "cancelled".to_string()
            } else {
                let raw = match &result {
                    Ok(v) => serde_json::to_string(v).unwrap_or_default(),
                    Err(e) => format!("error: {e:#}"),
                };
                truncate_tool_output(raw, max_output)
            };
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
                            // Same cancelled test as the persisted row
                            // above — a user cancellation must not render
                            // as a tool failure in the client.
                            "status": if cancelled {
                                "cancelled"
                            } else if result.is_ok() {
                                "completed"
                            } else {
                                "failed"
                            },
                        }
                    }),
                )
                .await;
        }
        // Repair: any call whose row failed to persist gets a "cancelled"
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
            return Ok(PromptOutcome {
                stop_reason: llm::StopReason::Other,
                model: model_used.clone(),
            });
        }
        // This iteration appended the assistant tool_calls row plus one
        // tool row per call — history grew, so compaction re-evaluates
        // at the top of the next iteration.
    }
    warn!(session_id, "hit max tool iterations");
    Ok(PromptOutcome {
        stop_reason: llm::StopReason::MaxTurnRequests,
        model: model_used,
    })
}

/// Derive a session title from the first user message: first non-empty
/// line, truncated to 60 chars on a char boundary. Returns None when the
/// prompt has no usable text (image-only prompts keep a NULL title until
/// a text turn arrives).
fn derive_title(content: &Value) -> Option<String> {
    let text = match content {
        Value::String(s) => s.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter(|p| p["type"] == "text")
            .filter_map(|p| p["text"].as_str())
            .collect::<Vec<_>>()
            .join(" "),
        _ => return None,
    };
    let first_line = text.lines().find(|l| !l.trim().is_empty())?.trim();
    let mut t: String = first_line.chars().take(60).collect();
    if first_line.chars().count() > 60 {
        t.push('…');
    }
    Some(t)
}

/// Rough token estimate. Text runs ~4 chars/token for ASCII; non-ASCII
/// bytes are weighted so a 3-byte CJK char costs ~1 token (see
/// `text_tokens`). Image/binary content parts cost a fixed
/// 1100 tokens regardless of their serialized size; thinking blocks and
/// tool_calls count by their serialized length.
fn estimate_tokens<'a>(messages: impl Iterator<Item = &'a Value>) -> u64 {
    messages
        .map(|m| {
            // content may be a string or a parts array (multimodal).
            let content = match &m["content"] {
                Value::String(s) => text_tokens(s),
                Value::Array(parts) => parts.iter().map(part_tokens).sum(),
                _ => 0,
            };
            // Persisted thinking blocks are replayed to the provider on
            // the next request — they cost real context and must count.
            let thinking = if m["thinking"].is_null() {
                0
            } else {
                (m["thinking"].to_string().len() / 4) as u64
            };
            let tool_calls = m["tool_calls"]
                .as_array()
                .map(|t| t.iter().map(|c| c.to_string().len() / 4).sum::<usize>())
                .unwrap_or(0) as u64;
            content + thinking + tool_calls
        })
        .sum()
}

/// ~4 chars/token for ASCII; non-ASCII bytes are weighted so a 3-byte
/// CJK char ≈ 1 token (its real per-char cost — the old 0.75 undershot
/// and delayed compaction past the context window). ASCII stays exactly
/// len/4.
fn text_tokens(s: &str) -> u64 {
    let non_ascii = s.bytes().filter(|b| !b.is_ascii()).count();
    ((s.len() + non_ascii / 3) / 4) as u64
}

/// Tokens for one content part. Image/binary payloads are priced at a
/// fixed 1100 tokens — their serialized form (base64) wildly overstates
/// the real vision cost.
fn part_tokens(p: &Value) -> u64 {
    match p["type"].as_str() {
        Some("image" | "image_url" | "input_image" | "binary") => 1100,
        _ => text_tokens(&p.to_string()),
    }
}

/// Bound persisted tool output: keep the first 3/4 and last 1/4 of the
/// bytes with an explicit truncation marker — the tail usually carries
/// the error a head-only cut would lose.
fn truncate_tool_output(content: String, max: usize) -> String {
    if content.len() <= max {
        return content;
    }
    let head = content.floor_char_boundary(max * 3 / 4);
    let tail_start = content.ceil_char_boundary(content.len() - max / 4);
    let omitted = tail_start - head;
    format!(
        "{}\n… [{omitted} bytes truncated] …\n{}",
        &content[..head],
        &content[tail_start..]
    )
}

/// A resolved prompt target: which provider serves the request and the
/// upstream model id to send it.
pub struct ResolvedModel {
    pub provider_name: String,
    pub provider: Arc<crate::provider::Provider>,
    pub model: String,
    pub thinking: Option<String>,
}

/// Resolve which provider + upstream model a request targets: discovered
/// exact ids first (a `provider/model` prefix match would hijack a
/// discovered id like "meta-llama/llama-3" whose first segment collides
/// with a provider name), then strict prefix/glob routing, then the
/// default provider still gets the requested name — mirrors the /v1
/// forward path. A `:low|:medium|:high` suffix is a thinking-level
/// selector, stripped before routing.
fn resolve_provider_model(
    state: &Arc<AppState>,
    requested: Option<&str>,
) -> anyhow::Result<ResolvedModel> {
    let cfg = state.config.read();
    let providers = state.providers.read();
    let discovered = state.discovered.read();
    let (req_model, req_thinking) = requested
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
        discovered
            .iter()
            .find_map(|(name, ids)| {
                ids.iter()
                    .any(|id| id == req_model)
                    .then(|| (name.clone(), req_model.to_string()))
            })
            .or_else(|| {
                cfg.route_model_strict(req_model)
                    .map(|(name, _, upstream)| (name.to_string(), upstream))
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
    Ok(ResolvedModel {
        provider_name: name,
        provider,
        model,
        thinking,
    })
}

/// Outcome of one compaction attempt. `compacted` is true only when a
/// summary was produced and recorded; `reason` explains a no-op.
#[derive(Debug)]
pub struct CompactReport {
    pub compacted: bool,
    /// Newest message id folded into the summary.
    pub compacted_through: Option<i64>,
    pub reason: Option<String>,
}

impl CompactReport {
    fn done(through: i64) -> Self {
        Self {
            compacted: true,
            compacted_through: Some(through),
            reason: None,
        }
    }
    fn skipped(reason: impl Into<String>) -> Self {
        Self {
            compacted: false,
            compacted_through: None,
            reason: Some(reason.into()),
        }
    }
}

/// Force a compaction pass on a session regardless of the 85% threshold —
/// the manual escape hatch for a session wedged against the real context
/// window while the estimate still reads under it. Resolves the model the
/// same way a prompt turn would (session default → provider default).
/// Callers must hold the session's live_prompts slot so a turn can't
/// interleave history writes mid-summarize.
pub async fn compact_session(
    state: &Arc<AppState>,
    session_id: &str,
    cancel: CancellationToken,
) -> anyhow::Result<CompactReport> {
    let session_model = state.store.session_model(session_id).await?;
    let resolved = resolve_provider_model(state, session_model.as_deref())?;
    compact_inner(
        state,
        session_id,
        &resolved.provider,
        &resolved.model,
        &cancel,
        true,
    )
    .await
}

/// If the session's estimated tokens exceed 85% of the model's context
/// window, summarize the oldest half of messages via the provider and
/// record the compaction. Failures are logged, never fatal — the turn
/// proceeds with the full history. `force` skips the threshold check
/// (manual `session/compact`); store failures still propagate so the
/// RPC caller sees them.
async fn maybe_compact(
    state: &Arc<AppState>,
    session_id: &str,
    provider: &Arc<crate::provider::Provider>,
    model: &str,
    cancel: &CancellationToken,
) {
    match compact_inner(state, session_id, provider, model, cancel, false).await {
        Ok(_) => {}
        Err(e) => warn!(session_id, error = %e, "compaction failed; keeping full history"),
    }
}

async fn compact_inner(
    state: &Arc<AppState>,
    session_id: &str,
    provider: &Arc<crate::provider::Provider>,
    model: &str,
    cancel: &CancellationToken,
    force: bool,
) -> anyhow::Result<CompactReport> {
    // The window only gates the automatic threshold — a forced compact
    // (manual escape hatch) must work even on models with no
    // context_window metadata.
    let window = {
        let cfg = state.config.read();
        cfg.model_meta(model).context_window
    };
    if window.is_none() && !force {
        return Ok(CompactReport::skipped(
            "model has no context_window metadata",
        ));
    }

    let (compacted_through, prev_summary) = state.store.compaction(session_id).await?;
    let full = state.store.messages_full(session_id).await?;
    // Only the uncompacted tail still costs context — counting the whole
    // log would re-trigger compaction on every turn.
    let tail: Vec<&StoredMessage> = full.iter().filter(|m| m.id > compacted_through).collect();
    let mut est = estimate_tokens(tail.iter().map(|m| &m.data));
    if let Some(s) = &prev_summary {
        est += text_tokens(s);
    }
    if !force && est < window.unwrap_or(u64::MAX) * 85 / 100 {
        return Ok(CompactReport::skipped("under 85% of context window"));
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
        return Ok(CompactReport::skipped("nothing to compact"));
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
        // content may be a string OR an array of parts (multimodal /
        // structured) — extract text either way so tool results and
        // part-array messages aren't summarized as empty lines.
        let mut content = crate::provider::content_text(&m.data["content"]);
        // Assistant tool calls carry no content — serialize them or the
        // summary loses "which tool ran" entirely.
        if let Some(calls) = m.data["tool_calls"].as_array() {
            for c in calls {
                content.push_str(&format!(
                    "\n[tool_call {}({})]",
                    c["function"]["name"].as_str().unwrap_or("?"),
                    c["function"]["arguments"].as_str().unwrap_or("")
                ));
            }
        }
        let content = if content.len() > 2000 {
            &content[..content.floor_char_boundary(2000)]
        } else {
            &content
        };
        transcript.push_str(&format!("{role}: {content}\n"));
    }
    // A dedicated summary model (config.summary_model) keeps compaction
    // cheap even when the turn runs on a frontier model. It routes like a
    // prompt model — "provider/model", glob, or discovered id — so a
    // cross-provider value doesn't get sent verbatim to the wrong API.
    let (summary_provider, summary_model) = {
        let cfg = state.config.read();
        let providers = state.providers.read();
        let discovered = state.discovered.read();
        match cfg.summary_model.as_deref() {
            Some(raw) => {
                let (m, _t) = Config::split_thinking_level(raw);
                // Discovered exact ids claim the model first — same
                // order as the prompt-model route above.
                let resolved = discovered
                    .iter()
                    .find_map(|(name, ids)| {
                        ids.iter()
                            .any(|id| id == m)
                            .then(|| (name.clone(), m.to_string()))
                    })
                    .or_else(|| {
                        cfg.route_model_strict(m)
                            .map(|(name, _, upstream)| (name.to_string(), upstream))
                    });
                match resolved.and_then(|(n, m)| providers.get(&n).cloned().map(|p| (p, m))) {
                    Some(v) => v,
                    None => {
                        warn!(session_id, model = %raw, "summary_model not routable; skipping compaction");
                        return Ok(CompactReport::skipped("summary_model not routable"));
                    }
                }
            }
            None => (provider.clone(), model.to_string()),
        }
    };
    let prompt = format!(
        "Summarize this conversation transcript in under 500 words. \
         Preserve: decisions made, tool calls and their outcomes, file paths, \
         errors encountered, and any state the next turn needs.\n\n{transcript}"
    );
    let req = json!({
        "model": summary_model,
        "messages": [{"role": "user", "content": prompt}],
    });
    // Bound the summarizer call — it is best-effort (failures are
    // logged, never fatal) and must never wedge the turn. Race it
    // against cancel too: a cancelled turn must release its
    // live_prompts slot promptly, not sit here for up to 120s.
    let summary = match tokio::select! {
        _ = cancel.cancelled() => {
            return Ok(CompactReport::skipped("cancelled"));
        }
        r = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            summary_provider.chat(req),
        ) => r,
    } {
        Ok(Ok(v)) => v["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        Ok(Err(e)) => {
            // Do NOT record a compaction on failure — a placeholder summary
            // would permanently drop the dropped range from the model's
            // context. The turn proceeds with full history and the next
            // iteration retries.
            warn!(session_id, error = %e, "compaction summarize failed; keeping full history");
            return Ok(CompactReport::skipped(format!("summarize failed: {e}")));
        }
        Err(_) => {
            warn!(
                session_id,
                "compaction summarize timed out; keeping full history"
            );
            return Ok(CompactReport::skipped("summarize timed out"));
        }
    };
    if summary.is_empty() {
        return Ok(CompactReport::skipped("empty summary"));
    }
    state
        .store
        .set_compaction(session_id, through_id, &summary)
        .await?;
    Ok(CompactReport::done(through_id))
}

async fn execute_tool(
    state: &Arc<AppState>,
    mcp: &crate::mcp::McpView<'_>,
    session_id: &str,
    call: &llm::ToolCall,
    client: &Arc<dyn ClientChannel>,
    cancel: &CancellationToken,
    perm_lock: &tokio::sync::Mutex<()>,
) -> anyhow::Result<Value> {
    if !mcp.has_tool(&call.name) {
        anyhow::bail!("unknown tool {}", call.name);
    }
    if !mcp.auto_approve(&call.name) && !mcp.session_approved(session_id, &call.name) {
        // Race the permission round-trip against cancellation and a
        // timeout — a silent client must not stall the turn forever.
        // The mutex serializes prompts so parallel calls never surface
        // two permission dialogs at once.
        let timeout = state
            .config
            .read()
            .permission_timeout_secs
            .map(std::time::Duration::from_secs)
            .unwrap_or(PERMISSION_TIMEOUT);
        let granted = {
            // Queued calls must not wait out the holder's full timeout
            // after cancel — race the lock acquisition too.
            let guard = tokio::select! {
                _ = cancel.cancelled() => None,
                g = perm_lock.lock() => Some(g),
            };
            let Some(_guard) = guard else {
                anyhow::bail!(Cancelled);
            };
            // Re-check under the lock: a parallel call to the same tool
            // may have just been granted "always allow" — skip a second
            // prompt instead of serializing two dialogs.
            if mcp.session_approved(session_id, &call.name) {
                true
            } else {
                tokio::select! {
                    _ = cancel.cancelled() => false,
                    g = tokio::time::timeout(timeout, request_permission(mcp, session_id, call, client)) => {
                        g.unwrap_or(false)
                    }
                }
            }
        };
        if !granted {
            anyhow::bail!("permission denied for {}", call.name);
        }
    }
    let args: Value = llm::parse_partial_json(&call.arguments);
    state.metrics.mcp_tool_calls.fetch_add(1, Ordering::Relaxed);
    let result = tokio::select! {
        _ = cancel.cancelled() => anyhow::bail!(Cancelled),
        r = mcp.call(&call.name, args) => r,
    };
    if result.is_err() {
        state
            .metrics
            .mcp_tool_errors
            .fetch_add(1, Ordering::Relaxed);
    }
    result
}

/// ACP session/request_permission round-trip. Failures and non-allow
/// outcomes both deny. "Always allow" records a session-scoped grant so
async fn request_permission(
    mcp: &crate::mcp::McpView<'_>,
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
                    {"optionId": "allow-always", "name": "Always allow", "kind": "allow_always"},
                    {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"},
                ],
            }),
        )
        .await;
    match resp {
        Ok(v) => {
            let option = v["outcome"]["optionId"].as_str().unwrap_or("");
            if option == "allow-always" {
                mcp.approve_for_session(session_id, &call.name);
            }
            option.starts_with("allow")
        }
        Err(_) => false,
    }
}
