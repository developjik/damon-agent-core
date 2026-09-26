//! Shared session machinery for backends whose CLI speaks stream-json
//! over stdio (Claude Code's `--output-format stream-json` family —
//! Amp, Cursor, Qwen, Kimi ship compatible or near-compatible dialects).
//!
//! Two session shapes share one dialect trait:
//! - [`StreamJsonSession`] — bidirectional: prompts go in over stdin,
//!   one process lives for the whole conversation (claude, amp).
//! - [`OneShotSession`] — the CLI only takes a prompt per process;
//!   each turn respawns with the dialect's resume flag (cursor, kimi,
//!   qwen). Permissions still relay while a turn's process is alive.
//!
//! Per-vendor differences live in [`StreamJsonDialect`]: launch args,
//! frame dispatch for vendor-private frames, permission wire format,
//! and control frames. Claude-family content/result translation is
//! shared through [`ClaudeFrameParser`], parameterized by
//! [`ClaudeQuirks`] so each dialect's exact behavior is data, not a
//! forked parser.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast};

use super::registry::ResolvedBackend;
use super::transport::{NdjsonTransport, request_roundtrip};
use super::types::*;
use super::{AgentClient, AgentSession};

/// Control requests we send (interrupt, set_model, set_mode) get a
/// bounded wait.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

/// A control operation the session wants to send on the wire. Dialects
/// that lack a control channel return `None` and the call fails cleanly.
pub enum ControlOp {
    Interrupt,
    SetModel(String),
    SetMode(String),
}

/// Which session shape a dialect drives.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SessionKind {
    /// stdin prompts, one long-lived process per session.
    Persistent,
    /// Prompt is a CLI arg; each turn respawns with the resume flag.
    OneShot,
}

/// What a dialect needs from its session: event emission, ask
/// bookkeeping, and raw frame writes. Implemented by both session
/// shapes so dialect translation code is shape-agnostic.
#[async_trait]
pub trait SessionCtx: Send + Sync {
    /// Push a normalized event to subscribers.
    fn emit(&self, ev: StreamEvent);
    /// Append to the transcript and emit it as a timeline event.
    async fn emit_timeline(&self, item: TimelineItem);
    /// Record the native session id (once) and emit ThreadStarted.
    async fn set_native_handle(&self, id: String);
    /// Register a permission ask and notify subscribers.
    async fn register_ask(&self, ask: PermissionRequest, wire_id: String);
    /// Resolve a pending wire request (answer to a control frame we sent).
    async fn resolve_wire(&self, id: &str, result: Result<Value, Value>);
    /// The in-flight turn id, if any.
    async fn current_turn(&self) -> Option<String>;
    /// Mark the current turn finished and raise the attention signal.
    async fn finish_turn(&self, kind: StreamEventKind);
    /// Write a raw frame to the live process (control replies, asks).
    /// Errors when no process is currently running.
    async fn send_raw(&self, frame: Value) -> Result<()>;
}

/// Per-vendor dialect of the stream-json wire format.
#[async_trait]
pub trait StreamJsonDialect: Send + Sync {
    /// Extra CLI args for launch. `resume` is the native session handle
    /// when reattaching; dialects choose the flag (`--resume`, `--session`).
    fn launch_args(&self, config: &SessionConfig, resume: Option<&str>) -> Vec<String>;

    /// CLI args carrying the prompt — one-shot dialects only
    /// (positional arg or `-p <text>`). Persistent dialects never see
    /// this called.
    fn prompt_args(&self, _prompt: &PromptInput) -> Result<Vec<String>> {
        bail!("dialect does not take prompt args")
    }

    /// Translate one inbound frame into session state changes and
    /// normalized events, using the ctx helpers.
    async fn on_frame(&self, ctx: &Arc<dyn SessionCtx>, frame: &Value);

    /// The stdin frame carrying a user prompt (persistent dialects).
    fn prompt_frame(&self, _prompt: &PromptInput) -> Result<Value> {
        bail!("dialect does not take stdin prompts")
    }

    /// The stdin frame carrying a mid-turn steering message. Default:
    /// identical to a prompt frame — dialects that mark steering on
    /// the wire (amp's `steer: true`) override this.
    fn steer_frame(&self, prompt: &PromptInput) -> Result<Value> {
        self.prompt_frame(prompt)
    }

    /// The stdin frame answering a permission ask. `wire_id` is the id
    /// the agent used in its request (stored in the pending ask).
    fn permission_response_frame(
        &self,
        ask: &PermissionRequest,
        wire_id: &str,
        response: &PermissionResponse,
    ) -> Value;

    /// The stdin frame for a control op, or None if the dialect has no
    /// control channel. `request_id` correlates the response.
    fn control_frame(&self, _request_id: &str, _op: &ControlOp) -> Option<Value> {
        None
    }

    /// When true, control frames are sent without awaiting a response —
    /// for dialects whose control channel is unverified or one-way.
    fn control_is_fire_and_forget(&self) -> bool {
        false
    }

    /// Static model/mode catalog (most CLIs have no listing wire call).
    fn catalog(&self) -> ProviderCatalog {
        ProviderCatalog::default()
    }

    /// Native sessions created outside the daemon that can be imported.
    async fn list_importable(&self, _cwd: &Path) -> Result<Vec<ImportableSession>> {
        Ok(vec![])
    }

    /// What this dialect can do — copied onto client and session.
    fn capabilities(&self) -> Capabilities;

    /// Session shape this dialect drives.
    fn session_kind(&self) -> SessionKind {
        SessionKind::Persistent
    }
}

// ---------------------------------------------------------------------------
// Claude-frame parser (shared by the claude/amp/kimi/qwen dialects)
// ---------------------------------------------------------------------------

/// Dialect quirks of the Claude stream-json frame family, expressed as
/// data so a single translator covers every vendor CLI. The presets
/// below document exactly where each dialect diverges; anything not
/// listed is identical across the family.
///
/// Cursor is deliberately NOT on this parser: its wire carries
/// complete `tool_call` envelopes instead of assistant tool_use blocks
/// and tool_result messages, so it keeps a private translator.
pub struct ClaudeQuirks {
    /// Normalizes a tool_use name+input into timeline detail. Dialects
    /// with vendor tool names (claude, amp) plug their mapper in; the
    /// default keeps the raw input so nothing is silently dropped.
    pub tool_detail: fn(name: &str, input: &Value) -> ToolCallDetail,
    /// Assistant frames may additionally carry OpenAI-style
    /// `tool_calls`, with results arriving as top-level `tool` frames
    /// (kimi's hybrid wire).
    pub openai_tool_calls: bool,
    /// Tool results arrive as `user` frames carrying `tool_result`
    /// blocks. Kimi reports them as `tool` frames instead.
    pub user_tool_results: bool,
    /// `usage: null` reports no usage at all (kimi, qwen) rather than
    /// an all-None Usage (claude and amp always report one).
    pub usage_optional: bool,
    /// Cost comes from the frame's `total_cost_usd` (claude, qwen);
    /// amp and kimi report none.
    pub cost_from_frame: bool,
    /// amp reports the context window as `usage.max_tokens`.
    pub context_from_max_tokens: bool,
    /// Failure text prefers the frame's `error` field (amp, kimi,
    /// qwen); claude only ever reports `result`.
    pub error_field_first: bool,
    /// claude marks user interrupts (subtype "interrupted", or
    /// terminal_reason "aborted_streaming") as a cancel, not a failure.
    pub detect_interrupt: bool,
    /// Task tool_use/tool_result pairs also raise Subagent lifecycle
    /// events (claude's Task tool is its subagent spawner), shaped like
    /// omp's raw subagent frames so consumers see one consistent
    /// name/description/status/state vocabulary across backends.
    pub subagent_from_task: bool,
}

impl Default for ClaudeQuirks {
    fn default() -> Self {
        Self {
            tool_detail: raw_tool_detail,
            openai_tool_calls: false,
            user_tool_results: true,
            usage_optional: false,
            cost_from_frame: false,
            context_from_max_tokens: false,
            error_field_first: false,
            detect_interrupt: false,
            subagent_from_task: false,
        }
    }
}

impl ClaudeQuirks {
    /// Claude Code proper: vendor tool names, frame-reported cost,
    /// interrupt detection, and Task subagents.
    pub fn claude() -> Self {
        Self {
            cost_from_frame: true,
            detect_interrupt: true,
            subagent_from_task: true,
            ..Self::default()
        }
    }

    /// Amp: Claude-compatible frames, no cost, the context window
    /// reported as `usage.max_tokens`, failure text preferring `error`.
    pub fn amp() -> Self {
        Self {
            context_from_max_tokens: true,
            error_field_first: true,
            ..Self::default()
        }
    }

    /// Kimi: hybrid wire — OpenAI `tool_calls` plus `tool` result
    /// frames, optional usage, no user-frame tool results.
    pub fn kimi() -> Self {
        Self {
            openai_tool_calls: true,
            user_tool_results: false,
            usage_optional: true,
            error_field_first: true,
            ..Self::default()
        }
    }

    /// Qwen: Claude-shaped blocks and user-frame tool results, with
    /// optional usage that carries the frame cost.
    pub fn qwen() -> Self {
        Self {
            usage_optional: true,
            cost_from_frame: true,
            error_field_first: true,
            ..Self::default()
        }
    }
}

/// Default tool_use detail: keep the raw input verbatim. Vendor
/// dialects override with their own mapper.
fn raw_tool_detail(_name: &str, input: &Value) -> ToolCallDetail {
    ToolCallDetail::Unknown {
        input: input.clone(),
        output: Value::Null,
    }
}

/// One translator for the Claude stream-json frame family. The
/// claude/amp/kimi/qwen dialects delegate here from `on_frame` after
/// handling their private arms (control frames, amp's error system
/// subtypes), so content-block and result-frame logic exists exactly
/// once instead of as four copy-pasted parsers that drift apart.
pub struct ClaudeFrameParser {
    quirks: ClaudeQuirks,
    /// In-flight Task calls (tool_use id → identity), consulted only
    /// when `subagent_from_task` is set so a tool_result can be told
    /// apart as a subagent ending. Claude tool_use ids are globally
    /// unique, so one parser shared across sessions stays consistent;
    /// the matching tool_result prunes its entry. A Task whose turn
    /// dies (interrupt, crash) leaves a dead id behind — a handful of
    /// strings costs nothing.
    tasks: Mutex<HashMap<String, TaskIdentity>>,
}

/// What a Task call remembered about its subagent, so the terminal
/// event can repeat the identity.
struct TaskIdentity {
    name: String,
    description: String,
}

impl ClaudeFrameParser {
    pub fn new(quirks: ClaudeQuirks) -> Self {
        Self {
            quirks,
            tasks: Mutex::new(HashMap::new()),
        }
    }

    /// Translate one inbound frame. Frame types the dialect owns
    /// (control_request/control_response, amp's error subtypes) are
    /// matched by the dialect before falling through to this.
    pub async fn on_frame(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        match f["type"].as_str().unwrap_or_default() {
            "system" => self.on_system(ctx, f).await,
            "assistant" => self.on_assistant(ctx, f).await,
            "user" if self.quirks.user_tool_results => self.on_user(ctx, f).await,
            "tool" if self.quirks.openai_tool_calls => self.on_tool(ctx, f).await,
            "result" => self.on_result(ctx, f).await,
            _ => {}
        }
    }

    async fn on_system(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        if f["subtype"].as_str() == Some("init") {
            let id = f["session_id"].as_str().unwrap_or_default().to_string();
            ctx.set_native_handle(id).await;
        }
        // Other system subtypes (hooks, thinking_tokens) are housekeeping.
    }

    async fn on_assistant(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        // `content` may be absent on kimi frames that only carry
        // tool_calls — no early return, or those frames would be
        // dropped before the OpenAI arm below.
        if let Some(blocks) = f["message"]["content"].as_array() {
            for block in blocks {
                // Set when this block starts a subagent Task — the
                // event rides behind the tool_call item so the timeline
                // shows the call first, then the subagent it spawned.
                let mut subagent = None;
                let item = match block["type"].as_str().unwrap_or_default() {
                    "text" => TimelineItem::AssistantMessage {
                        text: block["text"].as_str().unwrap_or_default().to_string(),
                    },
                    "thinking" => TimelineItem::Reasoning {
                        text: block["thinking"].as_str().unwrap_or_default().to_string(),
                    },
                    "tool_use" => {
                        let name = block["name"].as_str().unwrap_or_default();
                        let call = ToolCall {
                            call_id: block["id"].as_str().unwrap_or_default().to_string(),
                            name: name.to_string(),
                            status: ToolCallStatus::Running,
                            detail: (self.quirks.tool_detail)(name, &block["input"]),
                        };
                        if self.quirks.subagent_from_task && name == "Task" {
                            subagent =
                                Some(self.task_started(&call.call_id, &block["input"]).await);
                        }
                        TimelineItem::ToolCall(call)
                    }
                    _ => TimelineItem::Unknown { raw: block.clone() },
                };
                ctx.emit_timeline(item).await;
                if let Some(event) = subagent {
                    self.emit_subagent(ctx, event).await;
                }
            }
        }
        // OpenAI-style tool_calls array (kimi's hybrid wire).
        if self.quirks.openai_tool_calls
            && let Some(calls) = f["message"]["tool_calls"].as_array()
        {
            for call in calls {
                let name = call["function"]["name"].as_str().unwrap_or_default();
                ctx.emit_timeline(TimelineItem::ToolCall(ToolCall {
                    call_id: call["id"].as_str().unwrap_or_default().to_string(),
                    name: name.to_string(),
                    status: ToolCallStatus::Running,
                    detail: (self.quirks.tool_detail)(name, &call["function"]["arguments"]),
                }))
                .await;
            }
        }
    }

    /// tool_result blocks arrive as user messages (claude, amp, qwen).
    async fn on_user(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        let Some(blocks) = f["message"]["content"].as_array() else {
            return;
        };
        for block in blocks {
            if block["type"].as_str() != Some("tool_result") {
                continue;
            }
            let call_id = block["tool_use_id"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let is_error = block["is_error"].as_bool().unwrap_or(false);
            let output = match &block["content"] {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            ctx.emit_timeline(TimelineItem::ToolCall(ToolCall {
                call_id: call_id.clone(),
                name: String::new(),
                status: if is_error {
                    ToolCallStatus::Failed
                } else {
                    ToolCallStatus::Completed
                },
                detail: ToolCallDetail::Unknown {
                    input: Value::Null,
                    output: Value::String(output),
                },
            }))
            .await;
            if let Some(event) = self.task_finished(&call_id, is_error).await {
                self.emit_subagent(ctx, event).await;
            }
        }
    }

    /// OpenAI-style tool result frames: the result is the whole message
    /// (`tool_call_id` + top-level `content`). Kimi only.
    async fn on_tool(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        let call_id = f["tool_call_id"]
            .as_str()
            .or_else(|| f["tool_use_id"].as_str())
            .unwrap_or_default()
            .to_string();
        let output = match &f["content"] {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        ctx.emit_timeline(TimelineItem::ToolCall(ToolCall {
            call_id,
            name: String::new(),
            status: ToolCallStatus::Completed,
            detail: ToolCallDetail::Unknown {
                input: Value::Null,
                output: Value::String(output),
            },
        }))
        .await;
    }

    /// The per-turn `result` frame: usage shape and failure text follow
    /// the dialect's quirks; claude additionally recognizes interrupts.
    async fn on_result(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        let usage = if self.quirks.usage_optional && f["usage"].is_null() {
            None
        } else {
            Some(Usage {
                input_tokens: f["usage"]["input_tokens"].as_u64(),
                cached_input_tokens: f["usage"]["cache_read_input_tokens"].as_u64(),
                output_tokens: f["usage"]["output_tokens"].as_u64(),
                cost_usd: if self.quirks.cost_from_frame {
                    f["total_cost_usd"].as_f64()
                } else {
                    None
                },
                context_window: if self.quirks.context_from_max_tokens {
                    f["usage"]["max_tokens"].as_u64()
                } else {
                    None
                },
                context_used: None,
            })
        };
        let kind = if self.quirks.detect_interrupt
            && (f["subtype"].as_str() == Some("interrupted")
                || f["terminal_reason"].as_str() == Some("aborted_streaming"))
        {
            // Interrupt reports is_error:true with subtype
            // "error_during_execution" — the aborted_streaming terminal
            // reason is what marks a user cancel.
            StreamEventKind::TurnCanceled {
                reason: "interrupted".to_string(),
            }
        } else if f["is_error"].as_bool().unwrap_or(false) {
            let error = if self.quirks.error_field_first {
                f["error"].as_str().or_else(|| f["result"].as_str())
            } else {
                f["result"].as_str()
            }
            .unwrap_or("unknown error");
            StreamEventKind::TurnFailed {
                error: error.to_string(),
                code: f["subtype"].as_str().map(|s| s.to_string()),
            }
        } else {
            StreamEventKind::TurnCompleted { usage }
        };
        ctx.finish_turn(kind).await;
    }

    /// A Task tool_use opened a subagent — remember its identity and
    /// return the omp-shaped start event (name/description/status plus
    /// a mirror `state`, keyed by the tool_use id so the terminal
    /// event and the UI cards can correlate).
    async fn task_started(&self, call_id: &str, input: &Value) -> Value {
        let TaskIdentity { name, description } = TaskIdentity {
            name: input["subagent_type"]
                .as_str()
                .unwrap_or("Task")
                .to_string(),
            description: input["description"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        };
        self.tasks.lock().await.insert(
            call_id.to_string(),
            TaskIdentity {
                name: name.clone(),
                description: description.clone(),
            },
        );
        json!({
            "type": "subagent_lifecycle",
            "id": call_id,
            "name": name,
            "description": description,
            "status": "running",
            "state": "running",
        })
    }

    /// A tool_result closed a Task call — returns the terminal
    /// subagent event, or None when the result belongs to an ordinary
    /// tool.
    async fn task_finished(&self, call_id: &str, is_error: bool) -> Option<Value> {
        let identity = self.tasks.lock().await.remove(call_id)?;
        let status = if is_error { "failed" } else { "completed" };
        Some(json!({
            "type": "subagent_event",
            "id": call_id,
            "name": identity.name,
            "description": identity.description,
            "status": status,
            "state": status,
        }))
    }

    async fn emit_subagent(&self, ctx: &Arc<dyn SessionCtx>, event: Value) {
        let turn = ctx.current_turn().await;
        ctx.emit(StreamEvent {
            turn_id: turn,
            kind: StreamEventKind::Subagent { event },
        });
    }
}

/// Brand-level client for a stream-json dialect.
pub struct StreamJsonClient {
    resolved: ResolvedBackend,
    dialect: Arc<dyn StreamJsonDialect>,
    caps: Capabilities,
}

impl StreamJsonClient {
    pub fn new(resolved: ResolvedBackend, dialect: Arc<dyn StreamJsonDialect>) -> Self {
        let caps = dialect.capabilities();
        Self {
            resolved,
            dialect,
            caps,
        }
    }
}

#[async_trait]
impl AgentClient for StreamJsonClient {
    fn provider(&self) -> &str {
        &self.resolved.id
    }

    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn is_available(&self) -> bool {
        self.resolved.detected
    }

    async fn fetch_catalog(&self, _cwd: Option<&Path>) -> Result<ProviderCatalog> {
        Ok(self.dialect.catalog())
    }

    async fn create_session(&self, config: SessionConfig) -> Result<Arc<dyn AgentSession>> {
        match self.dialect.session_kind() {
            SessionKind::Persistent => {
                StreamJsonSession::spawn(&self.resolved, self.dialect.clone(), config, None)
                    .await
                    .map(|s| s as Arc<dyn AgentSession>)
            }
            SessionKind::OneShot => Ok(OneShotSession::new(
                &self.resolved,
                self.dialect.clone(),
                config,
            ) as Arc<dyn AgentSession>),
        }
    }

    async fn resume_session(
        &self,
        handle: &PersistenceHandle,
        config: SessionConfig,
    ) -> Result<Arc<dyn AgentSession>> {
        match self.dialect.session_kind() {
            SessionKind::Persistent => StreamJsonSession::spawn(
                &self.resolved,
                self.dialect.clone(),
                config,
                Some(&handle.native_handle),
            )
            .await
            .map(|s| s as Arc<dyn AgentSession>),
            SessionKind::OneShot => {
                let s = OneShotSession::new(&self.resolved, self.dialect.clone(), config);
                *s.shared.native_id.lock().await = Some(handle.native_handle.clone());
                Ok(s as Arc<dyn AgentSession>)
            }
        }
    }

    async fn list_importable_sessions(&self, cwd: &Path) -> Result<Vec<ImportableSession>> {
        self.dialect.list_importable(cwd).await
    }
}

/// One pending permission ask: the normalized request plus the wire id
/// the agent used (echoed back in the dialect's response frame).
struct PendingAsk {
    request: PermissionRequest,
    wire_id: String,
}

/// Shared fields for both session shapes.
struct Shared {
    provider: String,
    caps: Capabilities,
    events: broadcast::Sender<StreamEvent>,
    /// Native session id once the backend reports one.
    native_id: Mutex<Option<String>>,
    /// Pending permission asks keyed by our normalized request id.
    pending_asks: Mutex<HashMap<String, PendingAsk>>,
    /// Current turn id (uuid per prompt when the dialect has no native
    /// turn concept).
    current_turn: Mutex<Option<String>>,
}

impl Shared {
    fn new(provider: String, caps: Capabilities) -> Self {
        let (events, _) = broadcast::channel(512);
        Self {
            provider,
            caps,
            events,
            native_id: Mutex::new(None),
            pending_asks: Mutex::new(HashMap::new()),
            current_turn: Mutex::new(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Persistent session (bidirectional stdin prompts)
// ---------------------------------------------------------------------------

/// A live bidirectional stream-json session: one process, one conversation.
pub struct StreamJsonSession {
    pub transport: Arc<NdjsonTransport>,
    dialect: Arc<dyn StreamJsonDialect>,
    shared: Shared,
}

impl StreamJsonSession {
    pub async fn spawn(
        resolved: &ResolvedBackend,
        dialect: Arc<dyn StreamJsonDialect>,
        config: SessionConfig,
        resume: Option<&str>,
    ) -> Result<Arc<Self>> {
        let mut args = resolved.args.clone();
        args.extend(dialect.launch_args(&config, resume));

        let transport =
            NdjsonTransport::spawn(&resolved.command, &args, &HashMap::new(), &config.cwd).await?;

        let session = Arc::new(Self {
            transport: transport.clone(),
            dialect: dialect.clone(),
            shared: Shared::new(resolved.id.clone(), dialect.capabilities()),
        });

        // Frame dispatch task: translate wire frames → StreamEvent, and
        // fail the in-flight turn if the process dies without sending
        // its result frame (a dead CLI otherwise wedges the UI forever).
        {
            let ctx: Arc<dyn SessionCtx> = session.clone();
            let me: Weak<dyn SessionCtx> = Arc::downgrade(&ctx);
            let d = dialect.clone();
            let mut rx = transport.subscribe();
            let mut exited = transport.exited();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        r = rx.recv() => match r {
                            Ok(frame) => {
                                if let Some(me) = me.upgrade() {
                                    d.on_frame(&me, &frame).await;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        },
                        _ = exited.changed() => {
                            if *exited.borrow() { break; }
                        }
                    }
                }
                // The backend died. finish_turn is a no-op when the
                // dialect already closed the turn with a result frame.
                if let Some(me) = me.upgrade() {
                    me.finish_turn(StreamEventKind::TurnFailed {
                        error: "backend process exited without a result frame".to_string(),
                        code: None,
                    })
                    .await;
                }
            });
        }

        Ok(session)
    }

    /// Send a control op through the dialect's control channel.
    async fn control(&self, op: ControlOp) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        let Some(frame) = self.dialect.control_frame(&id, &op) else {
            bail!(
                "{} has no control channel for this operation",
                self.shared.provider
            );
        };
        if self.dialect.control_is_fire_and_forget() {
            self.transport.send(frame).await
        } else {
            request_roundtrip(&self.transport, id, frame, CONTROL_TIMEOUT).await?;
            Ok(())
        }
    }
}

#[async_trait]
impl SessionCtx for StreamJsonSession {
    fn emit(&self, ev: StreamEvent) {
        let _ = self.shared.events.send(ev);
    }

    async fn emit_timeline(&self, item: TimelineItem) {
        emit_timeline(&self.shared, item).await;
    }

    async fn set_native_handle(&self, id: String) {
        set_native_handle(&self.shared, id).await;
    }

    async fn register_ask(&self, ask: PermissionRequest, wire_id: String) {
        register_ask(&self.shared, ask, wire_id).await;
    }

    async fn resolve_wire(&self, id: &str, result: Result<Value, Value>) {
        self.transport.resolve(id, result).await;
    }

    async fn current_turn(&self) -> Option<String> {
        self.shared.current_turn.lock().await.clone()
    }

    async fn finish_turn(&self, kind: StreamEventKind) {
        finish_turn(&self.shared, kind).await;
    }

    async fn send_raw(&self, frame: Value) -> Result<()> {
        self.transport.send(frame).await
    }
}

#[async_trait]
impl AgentSession for StreamJsonSession {
    fn capabilities(&self) -> &Capabilities {
        &self.shared.caps
    }

    fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.shared.events.subscribe()
    }

    fn idle_secs(&self) -> u64 {
        self.transport.idle_secs()
    }

    async fn start_turn(&self, prompt: PromptInput) -> Result<String> {
        let turn_id = uuid::Uuid::new_v4().to_string();
        *self.shared.current_turn.lock().await = Some(turn_id.clone());
        let frame = self.dialect.prompt_frame(&prompt)?;
        self.transport.send(frame).await?;
        self.emit(StreamEvent::in_turn(
            turn_id.clone(),
            StreamEventKind::TurnStarted,
        ));
        Ok(turn_id)
    }

    async fn steer(&self, prompt: PromptInput, _expected_turn: &str) -> Result<SteerResult> {
        // stream-json dialects accept a new user message mid-turn; the
        // CLI queues it as steering input (amp marks the frame).
        let frame = self.dialect.steer_frame(&prompt)?;
        self.transport.send(frame).await?;
        Ok(SteerResult::Accepted)
    }

    async fn interrupt(&self) -> Result<()> {
        self.control(ControlOp::Interrupt).await
    }

    async fn close(&self) -> Result<()> {
        self.transport.shutdown().await;
        Ok(())
    }

    async fn respond_to_permission(
        &self,
        request_id: &str,
        response: PermissionResponse,
    ) -> Result<()> {
        respond_to_permission(&self.shared, &*self.dialect, request_id, response, |f| {
            self.transport.send(f)
        })
        .await
    }

    fn persistence_handle(&self) -> Option<PersistenceHandle> {
        let id = self.shared.native_id.try_lock().ok()?.clone()?;
        Some(PersistenceHandle {
            provider: self.shared.provider.clone(),
            native_handle: id,
            metadata: Value::Null,
        })
    }

    async fn set_mode(&self, mode: &str) -> Result<()> {
        self.control(ControlOp::SetMode(mode.to_string())).await?;
        self.emit(StreamEvent::new(StreamEventKind::ModeChanged {
            mode: Some(mode.to_string()),
        }));
        Ok(())
    }

    async fn set_model(&self, model: &str) -> Result<()> {
        self.control(ControlOp::SetModel(model.to_string())).await?;
        self.emit(StreamEvent::new(StreamEventKind::ModelChanged {
            model: model.to_string(),
        }));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// One-shot session (prompt per process, resume flag per turn)
// ---------------------------------------------------------------------------

/// A session whose CLI only accepts one prompt per process. Each turn
/// spawns a fresh process with the dialect's resume flag; the native
/// session id carries the conversation forward. Permission asks still
/// relay while a turn's process is alive.
pub struct OneShotSession {
    resolved: ResolvedBackend,
    dialect: Arc<dyn StreamJsonDialect>,
    config: SessionConfig,
    shared: Shared,
    /// The live turn's process, if a turn is in flight.
    active: Mutex<Option<Arc<NdjsonTransport>>>,
    /// model/mode set via set_model/set_mode — applied to the next spawn.
    overrides: Mutex<(Option<String>, Option<String>)>,
    /// Set when interrupt() kills the turn process — distinguishes a
    /// deliberate cancel from an unexpected exit.
    interrupted: AtomicBool,
    /// Last activity timestamp for the idle sweep.
    last_used: AtomicU64,
    /// Self-reference for the per-turn pump task.
    me: OnceLock<Weak<Self>>,
}

impl OneShotSession {
    pub fn new(
        resolved: &ResolvedBackend,
        dialect: Arc<dyn StreamJsonDialect>,
        config: SessionConfig,
    ) -> Arc<Self> {
        let session = Arc::new(Self {
            resolved: resolved.clone(),
            dialect: dialect.clone(),
            config,
            shared: Shared::new(resolved.id.clone(), dialect.capabilities()),
            active: Mutex::new(None),
            overrides: Mutex::new((None, None)),
            interrupted: AtomicBool::new(false),
            last_used: AtomicU64::new(epoch_secs()),
            me: OnceLock::new(),
        });
        let _ = session.me.set(Arc::downgrade(&session));
        session
    }

    fn touch(&self) {
        self.last_used.store(epoch_secs(), Ordering::Relaxed);
    }
}

fn epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[async_trait]
impl SessionCtx for OneShotSession {
    fn emit(&self, ev: StreamEvent) {
        let _ = self.shared.events.send(ev);
    }

    async fn emit_timeline(&self, item: TimelineItem) {
        emit_timeline(&self.shared, item).await;
    }

    async fn set_native_handle(&self, id: String) {
        set_native_handle(&self.shared, id).await;
    }

    async fn register_ask(&self, ask: PermissionRequest, wire_id: String) {
        register_ask(&self.shared, ask, wire_id).await;
    }

    async fn resolve_wire(&self, id: &str, result: Result<Value, Value>) {
        if let Some(t) = self.active.lock().await.as_ref() {
            t.resolve(id, result).await;
        }
    }

    async fn current_turn(&self) -> Option<String> {
        self.shared.current_turn.lock().await.clone()
    }

    async fn finish_turn(&self, kind: StreamEventKind) {
        finish_turn(&self.shared, kind).await;
    }

    async fn send_raw(&self, frame: Value) -> Result<()> {
        let t = self.active.lock().await;
        match t.as_ref() {
            Some(t) => t.send(frame).await,
            None => bail!("no live process"),
        }
    }
}

#[async_trait]
impl AgentSession for OneShotSession {
    fn capabilities(&self) -> &Capabilities {
        &self.shared.caps
    }

    fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.shared.events.subscribe()
    }

    fn idle_secs(&self) -> u64 {
        epoch_secs().saturating_sub(self.last_used.load(Ordering::Relaxed))
    }

    async fn start_turn(&self, prompt: PromptInput) -> Result<String> {
        if self.shared.current_turn.lock().await.is_some() {
            bail!("turn already in flight");
        }
        self.touch();
        self.interrupted.store(false, Ordering::Relaxed);

        // Apply set_model/set_mode overrides on top of the base config.
        let (model, mode) = self.overrides.lock().await.clone();
        let mut config = self.config.clone();
        if let Some(m) = model {
            config.model = Some(m);
        }
        if let Some(m) = mode {
            config.mode = Some(m);
        }

        let resume = self.shared.native_id.lock().await.clone();
        let mut args = self.resolved.args.clone();
        args.extend(self.dialect.launch_args(&config, resume.as_deref()));
        args.extend(self.dialect.prompt_args(&prompt)?);

        let transport =
            NdjsonTransport::spawn(&self.resolved.command, &args, &HashMap::new(), &config.cwd)
                .await?;
        *self.active.lock().await = Some(transport.clone());

        let turn_id = uuid::Uuid::new_v4().to_string();
        *self.shared.current_turn.lock().await = Some(turn_id.clone());

        // Pump: translate frames until the process exits. A process that
        // dies without a result frame ends the turn as failed/canceled.
        {
            let me = self.me.get().and_then(Weak::upgrade);
            if let Some(me) = me {
                let ctx: Arc<dyn SessionCtx> = me.clone();
                let d = self.dialect.clone();
                let mut rx = transport.subscribe();
                let mut exited = transport.exited();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            r = rx.recv() => match r {
                                Ok(frame) => {
                                    me.touch();
                                    d.on_frame(&ctx, &frame).await;
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(broadcast::error::RecvError::Closed) => break,
                            },
                            _ = exited.changed() => {
                                if *exited.borrow() { break; }
                            }
                        }
                    }
                    // Process ended. If the dialect already finished the
                    // turn this is a no-op; otherwise report the exit.
                    if ctx.current_turn().await.is_some() {
                        let kind = if me.interrupted.load(Ordering::Relaxed) {
                            StreamEventKind::TurnCanceled {
                                reason: "interrupted".to_string(),
                            }
                        } else {
                            StreamEventKind::TurnFailed {
                                error: "process exited without a result frame".to_string(),
                                code: None,
                            }
                        };
                        ctx.finish_turn(kind).await;
                    }
                    *me.active.lock().await = None;
                });
            }
        }

        self.emit(StreamEvent::in_turn(
            turn_id.clone(),
            StreamEventKind::TurnStarted,
        ));
        Ok(turn_id)
    }

    async fn steer(&self, _prompt: PromptInput, _expected_turn: &str) -> Result<SteerResult> {
        Ok(SteerResult::Unavailable)
    }

    async fn interrupt(&self) -> Result<()> {
        self.interrupted.store(true, Ordering::Relaxed);
        if let Some(t) = self.active.lock().await.as_ref() {
            t.shutdown().await;
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        if let Some(t) = self.active.lock().await.take() {
            t.shutdown().await;
        }
        Ok(())
    }

    async fn respond_to_permission(
        &self,
        request_id: &str,
        response: PermissionResponse,
    ) -> Result<()> {
        let active = self.active.lock().await.clone();
        let Some(t) = active else {
            bail!("no live process");
        };
        respond_to_permission(&self.shared, &*self.dialect, request_id, response, |f| {
            t.send(f)
        })
        .await
    }

    fn persistence_handle(&self) -> Option<PersistenceHandle> {
        let id = self.shared.native_id.try_lock().ok()?.clone()?;
        Some(PersistenceHandle {
            provider: self.shared.provider.clone(),
            native_handle: id,
            metadata: Value::Null,
        })
    }

    async fn set_mode(&self, mode: &str) -> Result<()> {
        self.overrides.lock().await.1 = Some(mode.to_string());
        self.emit(StreamEvent::new(StreamEventKind::ModeChanged {
            mode: Some(mode.to_string()),
        }));
        Ok(())
    }

    async fn set_model(&self, model: &str) -> Result<()> {
        self.overrides.lock().await.0 = Some(model.to_string());
        self.emit(StreamEvent::new(StreamEventKind::ModelChanged {
            model: model.to_string(),
        }));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

async fn emit_timeline(shared: &Shared, item: TimelineItem) {
    let turn = shared.current_turn.lock().await.clone();
    let _ = shared.events.send(StreamEvent {
        turn_id: turn,
        kind: StreamEventKind::Timeline(item),
    });
}

async fn set_native_handle(shared: &Shared, id: String) {
    let mut slot = shared.native_id.lock().await;
    if slot.is_none() {
        *slot = Some(id.clone());
        drop(slot);
        let _ = shared
            .events
            .send(StreamEvent::new(StreamEventKind::ThreadStarted {
                native_handle: id,
            }));
    }
}

async fn register_ask(shared: &Shared, ask: PermissionRequest, wire_id: String) {
    shared.pending_asks.lock().await.insert(
        ask.id.clone(),
        PendingAsk {
            request: ask.clone(),
            wire_id,
        },
    );
    let turn = shared.current_turn.lock().await.clone();
    let _ = shared.events.send(StreamEvent {
        turn_id: turn,
        kind: StreamEventKind::PermissionRequested(ask),
    });
    let _ = shared
        .events
        .send(StreamEvent::new(StreamEventKind::AttentionRequired {
            reason: AttentionReason::Permission,
        }));
}

async fn finish_turn(shared: &Shared, kind: StreamEventKind) {
    let turn = shared.current_turn.lock().await.take();
    if turn.is_none() {
        return; // already finished (e.g. result frame then process exit)
    }
    let _ = shared.events.send(StreamEvent {
        turn_id: turn,
        kind,
    });
    let _ = shared
        .events
        .send(StreamEvent::new(StreamEventKind::AttentionRequired {
            reason: AttentionReason::Finished,
        }));
}

async fn respond_to_permission<F, Fut>(
    shared: &Shared,
    dialect: &dyn StreamJsonDialect,
    request_id: &str,
    response: PermissionResponse,
    send: F,
) -> Result<()>
where
    F: FnOnce(Value) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let Some(ask) = shared.pending_asks.lock().await.remove(request_id) else {
        bail!("no pending permission {request_id}");
    };
    let frame = dialect.permission_response_frame(&ask.request, &ask.wire_id, &response);
    send(frame).await?;
    let _ = shared
        .events
        .send(StreamEvent::new(StreamEventKind::PermissionResolved {
            request_id: request_id.to_string(),
        }));
    Ok(())
}
#[cfg(test)]
pub(crate) mod tests {
    //! A recording SessionCtx for dialect fixture tests — no process.
    use super::*;
    use serde_json::json;

    use tokio::sync::Mutex as TokioMutex;

    pub struct TestCtx {
        pub events: TokioMutex<Vec<StreamEvent>>,
        pub asks: TokioMutex<Vec<PermissionRequest>>,
        pub native: TokioMutex<Option<String>>,
        pub sent: TokioMutex<Vec<Value>>,
        pub turn: TokioMutex<Option<String>>,
    }

    impl TestCtx {
        pub fn new() -> Arc<Self> {
            Arc::new(Self {
                events: TokioMutex::new(vec![]),
                asks: TokioMutex::new(vec![]),
                native: TokioMutex::new(None),
                sent: TokioMutex::new(vec![]),
                turn: TokioMutex::new(Some("t1".to_string())),
            })
        }

        pub fn as_ctx(self: &Arc<Self>) -> Arc<dyn SessionCtx> {
            self.clone()
        }
    }

    #[async_trait]
    impl SessionCtx for TestCtx {
        fn emit(&self, ev: StreamEvent) {
            let _ = self.events.try_lock().map(|mut e| e.push(ev));
        }
        async fn emit_timeline(&self, item: TimelineItem) {
            self.events.lock().await.push(StreamEvent {
                turn_id: self.turn.lock().await.clone(),
                kind: StreamEventKind::Timeline(item),
            });
        }
        async fn set_native_handle(&self, id: String) {
            *self.native.lock().await = Some(id);
        }
        async fn register_ask(&self, ask: PermissionRequest, _wire_id: String) {
            self.asks.lock().await.push(ask);
        }
        async fn resolve_wire(&self, _id: &str, _result: Result<Value, Value>) {}
        async fn current_turn(&self) -> Option<String> {
            self.turn.lock().await.clone()
        }
        async fn finish_turn(&self, kind: StreamEventKind) {
            *self.turn.lock().await = None;
            self.events.lock().await.push(StreamEvent {
                turn_id: None,
                kind,
            });
        }
        async fn send_raw(&self, frame: Value) -> Result<()> {
            self.sent.lock().await.push(frame);
            Ok(())
        }
    }

    /// A dialect whose CLI dies without ever sending a result frame —
    /// exercises the exit pump against a real subprocess.
    struct DyingDialect;

    #[async_trait]
    impl StreamJsonDialect for DyingDialect {
        fn launch_args(&self, _config: &SessionConfig, _resume: Option<&str>) -> Vec<String> {
            vec![]
        }
        async fn on_frame(&self, _ctx: &Arc<dyn SessionCtx>, _frame: &Value) {}
        fn prompt_frame(&self, _prompt: &PromptInput) -> Result<Value> {
            Ok(json!({"type": "user"}))
        }
        fn permission_response_frame(
            &self,
            _ask: &PermissionRequest,
            _wire_id: &str,
            _response: &PermissionResponse,
        ) -> Value {
            json!({})
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }
    }

    #[tokio::test]
    async fn persistent_session_fails_turn_when_process_dies() {
        use crate::backend::registry::ResolvedBackend;

        let resolved = ResolvedBackend {
            id: "dying".into(),
            title: "Dying".into(),
            command: "sh".into(),
            args: vec!["-c".into(), "sleep 1".into()],
            env: vec![],
            auth_hint: String::new(),
            detected: true,
        };
        let session = StreamJsonSession::spawn(
            &resolved,
            Arc::new(DyingDialect),
            SessionConfig {
                cwd: std::env::temp_dir(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        let mut events = session.subscribe();
        session
            .start_turn(PromptInput::Text("hello".into()))
            .await
            .unwrap();

        // TurnStarted arrives immediately; ~1s later the process exits
        // with no result frame — the pump must fail the turn, not
        // block forever on the still-open channel.
        loop {
            let ev = tokio::time::timeout(Duration::from_secs(15), events.recv())
                .await
                .expect("timed out waiting for TurnFailed");
            let ev = ev.expect("event channel closed");
            if matches!(ev.kind, StreamEventKind::TurnFailed { .. }) {
                return; // regression passes
            }
        }
    }

    // ------------------------------------------------------------------
    // Dialect fixture tests, keyed by dialect. These moved out of the
    // amp/kimi/qwen files when their copy-pasted frame parsers were
    // replaced by ClaudeFrameParser — they pin each dialect's exact
    // translation through the shared code.
    // ------------------------------------------------------------------

    use crate::backend::amp::AmpDialect;
    use crate::backend::claude::ClaudeDialect;
    use crate::backend::kimi::KimiDialect;
    use crate::backend::qwen::QwenDialect;

    #[tokio::test]
    async fn amp_init_captures_thread_id() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        AmpDialect::dialect()
            .on_frame(
                &ctx,
                &json!({"type":"system","subtype":"init","session_id":"T-abc-123"}),
            )
            .await;
        assert_eq!(t.native.lock().await.as_deref(), Some("T-abc-123"));
    }

    #[tokio::test]
    async fn amp_assistant_text_and_tool_use() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        AmpDialect::dialect()
            .on_frame(
                &ctx,
                &json!({
                    "type":"assistant",
                    "message":{"role":"assistant","content":[
                        {"type":"text","text":"reading file"},
                        {"type":"tool_use","id":"tu1","name":"Read","input":{"file_path":"/x.rs"}}
                    ]}
                }),
            )
            .await;
        let events = t.events.lock().await;
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0].kind,
            StreamEventKind::Timeline(TimelineItem::AssistantMessage { .. })
        ));
        assert!(matches!(
            events[1].kind,
            StreamEventKind::Timeline(TimelineItem::ToolCall(_))
        ));
    }

    #[tokio::test]
    async fn amp_result_finishes_with_usage() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        AmpDialect::dialect()
            .on_frame(
                &ctx,
                &json!({
                    "type":"result","subtype":"success","is_error":false,
                    "usage":{"input_tokens":10,"output_tokens":5,"max_tokens":200000}
                }),
            )
            .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::TurnCompleted { usage }) = events.last().map(|e| &e.kind) else {
            panic!("expected TurnCompleted");
        };
        let usage = usage.as_ref().unwrap();
        assert_eq!(usage.input_tokens, Some(10));
        // amp's quirk: the context window rides in usage.max_tokens and
        // no cost is reported.
        assert_eq!(usage.context_window, Some(200000));
        assert_eq!(usage.cost_usd, None);
    }

    #[tokio::test]
    async fn kimi_openai_style_tool_calls() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        KimiDialect::dialect()
            .on_frame(
                &ctx,
                &json!({
                    "type":"assistant","session_id":"k-1",
                    "message":{"role":"assistant","tool_calls":[
                        {"id":"c1","function":{"name":"read_file","arguments":"{\"path\":\"a.rs\"}"}}
                    ]}
                }),
            )
            .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::Timeline(TimelineItem::ToolCall(tc))) =
            events.first().map(|e| &e.kind)
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.name, "read_file");
        assert_eq!(tc.call_id, "c1");
    }

    #[tokio::test]
    async fn kimi_tool_result_frame_completes_call() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        KimiDialect::dialect()
            .on_frame(
                &ctx,
                &json!({"type":"tool","tool_call_id":"c1","content":"file contents"}),
            )
            .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::Timeline(TimelineItem::ToolCall(tc))) =
            events.first().map(|e| &e.kind)
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.status, ToolCallStatus::Completed);
    }

    #[tokio::test]
    async fn qwen_tool_result_via_user_frame() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        QwenDialect::dialect()
            .on_frame(
                &ctx,
                &json!({
                    "type":"user",
                    "message":{"role":"user","content":[
                        {"type":"tool_result","tool_use_id":"tu1","content":"ok","is_error":false}
                    ]}
                }),
            )
            .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::Timeline(TimelineItem::ToolCall(tc))) =
            events.first().map(|e| &e.kind)
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.status, ToolCallStatus::Completed);
    }

    /// The Task tool_use keeps its tool_call timeline item AND raises
    /// an omp-shaped Subagent start event keyed by the tool_use id.
    #[tokio::test]
    async fn claude_task_tool_use_raises_subagent_start() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        ClaudeDialect::dialect()
            .on_frame(
                &ctx,
                &json!({
                    "type":"assistant",
                    "message":{"role":"assistant","content":[
                        {"type":"tool_use","id":"task-1","name":"Task","input":{
                            "subagent_type":"code-reviewer",
                            "description":"review the parser",
                            "prompt":"look at streamjson.rs"
                        }}
                    ]}
                }),
            )
            .await;
        let events = t.events.lock().await;
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0].kind,
            StreamEventKind::Timeline(TimelineItem::ToolCall(_))
        ));
        let StreamEventKind::Subagent { event } = &events[1].kind else {
            panic!("expected subagent event, got {:?}", events[1].kind);
        };
        assert_eq!(event["type"], "subagent_lifecycle");
        assert_eq!(event["id"], "task-1");
        assert_eq!(event["name"], "code-reviewer");
        assert_eq!(event["description"], "review the parser");
        assert_eq!(event["status"], "running");
        assert_eq!(event["state"], "running");
        // The subagent event stays inside its turn like omp's do.
        assert_eq!(events[1].turn_id.as_deref(), Some("t1"));
    }

    /// A Task tool_result closes the loop: the terminal event repeats
    /// the remembered identity and reflects the outcome.
    #[tokio::test]
    async fn claude_task_result_raises_terminal_subagent_event() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = ClaudeDialect::dialect();
        d.on_frame(
            &ctx,
            &json!({
                "type":"assistant",
                "message":{"content":[
                    {"type":"tool_use","id":"task-9","name":"Task","input":{
                        "subagent_type":"general-purpose","description":"find the bug"
                    }}
                ]}
            }),
        )
        .await;
        d.on_frame(
            &ctx,
            &json!({
                "type":"user",
                "message":{"content":[
                    {"type":"tool_result","tool_use_id":"task-9","content":"found it","is_error":false}
                ]}
            }),
        )
        .await;
        let events = t.events.lock().await;
        // start pair (tool_call + subagent), then result pair
        assert_eq!(events.len(), 4);
        let StreamEventKind::Subagent { event } = &events[3].kind else {
            panic!("expected subagent event, got {:?}", events[3].kind);
        };
        assert_eq!(event["id"], "task-9");
        assert_eq!(event["name"], "general-purpose");
        assert_eq!(event["description"], "find the bug");
        assert_eq!(event["status"], "completed");
        assert_eq!(event["state"], "completed");
        // The tool_result's own timeline item is still there.
        assert!(matches!(
            events[2].kind,
            StreamEventKind::Timeline(TimelineItem::ToolCall(_))
        ));
    }

    #[tokio::test]
    async fn claude_failed_task_result_marks_subagent_failed() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = ClaudeDialect::dialect();
        d.on_frame(
            &ctx,
            &json!({
                "type":"assistant",
                "message":{"content":[
                    {"type":"tool_use","id":"task-x","name":"Task","input":{
                        "subagent_type":"general-purpose","description":"boom"
                    }}
                ]}
            }),
        )
        .await;
        d.on_frame(
            &ctx,
            &json!({
                "type":"user",
                "message":{"content":[
                    {"type":"tool_result","tool_use_id":"task-x","content":"exploded","is_error":true}
                ]}
            }),
        )
        .await;
        let events = t.events.lock().await;
        let StreamEventKind::Subagent { event } = &events[3].kind else {
            panic!("expected subagent event");
        };
        assert_eq!(event["status"], "failed");
    }

    /// Only Task calls raise subagent events — ordinary tool traffic
    /// must not grow any.
    #[tokio::test]
    async fn claude_plain_tool_traffic_raises_no_subagent_events() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = ClaudeDialect::dialect();
        d.on_frame(
            &ctx,
            &json!({
                "type":"assistant",
                "message":{"content":[
                    {"type":"tool_use","id":"bash-1","name":"Bash","input":{"command":"ls"}}
                ]}
            }),
        )
        .await;
        d.on_frame(
            &ctx,
            &json!({
                "type":"user",
                "message":{"content":[
                    {"type":"tool_result","tool_use_id":"bash-1","content":"files","is_error":false}
                ]}
            }),
        )
        .await;
        let events = t.events.lock().await;
        assert_eq!(events.len(), 2);
        assert!(
            events.iter().all(|e| {
                matches!(e.kind, StreamEventKind::Timeline(TimelineItem::ToolCall(_)))
            })
        );
    }
}
