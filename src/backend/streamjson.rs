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
//! frame translation, permission wire format, and control frames.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::Value;
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

        // Frame dispatch task: translate wire frames → StreamEvent.
        {
            let me: Arc<dyn SessionCtx> = session.clone();
            let d = dialect.clone();
            let mut rx = transport.subscribe();
            let t = transport.clone();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(frame) => d.on_frame(&me, &frame).await,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                    if !t.is_alive() {
                        break;
                    }
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
        // CLI queues it as steering input.
        let frame = self.dialect.prompt_frame(&prompt)?;
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
                tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok(frame) => {
                                me.touch();
                                d.on_frame(&ctx, &frame).await;
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                        if !transport.is_alive() {
                            break;
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
}
