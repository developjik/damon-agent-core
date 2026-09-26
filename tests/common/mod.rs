//! Shared test helpers: a Config wired to an in-process mock backend.
//! Not every test file uses every helper — dead code is expected.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use damon_core::backend::types::*;
use damon_core::backend::{AgentClient, AgentSession};
use damon_core::config::Config;
use tokio::sync::{Mutex, broadcast, oneshot};

/// Config whose default backend is the in-process mock — sessions echo
/// prompts, can raise a permission ask, and can hang until interrupted.
pub fn mock_config(auth_token: Option<&str>) -> Config {
    Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: auth_token.map(String::from),
        data_dir: None,
        tls_cert: None,
        tls_key: None,
        backends: Default::default(),
        default_backend: Some("mock".to_string()),
        relay: None,
        permission_timeout_secs: None,
        session_retention_days: None,
        // Idle reaping off by default — only the dedicated idle test
        // turns it on, so no other test's shared mock gets killed.
        agent_idle_secs: 0,
        max_sessions: None,
        allowed_dirs: Vec::new(),
    }
}

/// The in-process mock backend as a client — tests inject it into
/// `state.sessions` so `session.create` resolves the "mock" provider.
pub fn mock_client() -> Arc<dyn AgentClient> {
    Arc::new(MockClient)
}

/// In-process backend: echoes the prompt, optionally asks permission
/// (prompt containing "perm"), optionally hangs (prompt containing
/// "hang") until interrupted.
struct MockClient;

#[async_trait]
impl AgentClient for MockClient {
    fn provider(&self) -> &str {
        "mock"
    }
    fn capabilities(&self) -> &Capabilities {
        static CAPS: std::sync::OnceLock<Capabilities> = std::sync::OnceLock::new();
        CAPS.get_or_init(|| Capabilities {
            session_persistence: true,
            ..Default::default()
        })
    }
    async fn is_available(&self) -> bool {
        true
    }
    async fn fetch_catalog(&self, _cwd: Option<&std::path::Path>) -> Result<ProviderCatalog> {
        Ok(ProviderCatalog::default())
    }
    async fn create_session(&self, _config: SessionConfig) -> Result<Arc<dyn AgentSession>> {
        Ok(Arc::new(MockSession::new()))
    }
    async fn resume_session(
        &self,
        handle: &PersistenceHandle,
        _config: SessionConfig,
    ) -> Result<Arc<dyn AgentSession>> {
        let s = MockSession::new();
        *s.handle.lock().await = Some(handle.clone());
        Ok(Arc::new(s))
    }
}

struct PendingAsk {
    answer: oneshot::Sender<PermissionResponse>,
}

struct MockSession {
    events: broadcast::Sender<StreamEvent>,
    caps: Capabilities,
    handle: Mutex<Option<PersistenceHandle>>,
    pending: Mutex<HashMap<String, PendingAsk>>,
    interrupt: Mutex<Option<oneshot::Sender<()>>>,
    idle_since: Mutex<Instant>,
    seq: AtomicU64,
}

impl MockSession {
    fn new() -> Self {
        Self {
            events: broadcast::channel(256).0,
            caps: Capabilities {
                session_persistence: true,
                ..Default::default()
            },
            handle: Mutex::new(Some(PersistenceHandle {
                provider: "mock".into(),
                native_handle: uuid::Uuid::new_v4().to_string(),
                metadata: serde_json::Value::Null,
            })),
            pending: Mutex::new(HashMap::new()),
            interrupt: Mutex::new(None),
            idle_since: Mutex::new(Instant::now()),
            seq: AtomicU64::new(0),
        }
    }

    fn emit(&self, ev: StreamEvent) {
        let _ = self.events.send(ev);
    }
}

#[async_trait]
impl AgentSession for MockSession {
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }
    fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.events.subscribe()
    }

    async fn start_turn(&self, prompt: PromptInput) -> Result<String> {
        let turn = format!("t{}", self.seq.fetch_add(1, Ordering::Relaxed));
        *self.idle_since.lock().await = Instant::now();
        let text = match &prompt {
            PromptInput::Text(t) => t.clone(),
            PromptInput::Blocks(b) => b
                .iter()
                .filter_map(|b| match b {
                    PromptBlock::Text { text } => Some(text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" "),
        };
        self.emit(StreamEvent::in_turn(
            turn.clone(),
            StreamEventKind::TurnStarted,
        ));

        if text.contains("perm") {
            // Raise a permission ask and wait for the answer.
            let (tx, rx) = oneshot::channel();
            let req = PermissionRequest {
                id: format!("perm-{}", turn),
                kind: PermissionKind::Tool,
                name: "Bash".into(),
                title: Some("run command".into()),
                input: Some(serde_json::json!({"command": "echo hi"})),
                detail: None,
                actions: vec![
                    PermissionAction {
                        id: "allow".into(),
                        label: "Allow".into(),
                        behavior: PermissionBehavior::Allow,
                        variant: Some(ActionVariant::Primary),
                    },
                    PermissionAction {
                        id: "deny".into(),
                        label: "Deny".into(),
                        behavior: PermissionBehavior::Deny,
                        variant: Some(ActionVariant::Danger),
                    },
                ],
                suggestions: vec![],
            };
            self.pending
                .lock()
                .await
                .insert(req.id.clone(), PendingAsk { answer: tx });
            self.emit(StreamEvent::in_turn(
                turn.clone(),
                StreamEventKind::PermissionRequested(req),
            ));
            let resp = rx.await.unwrap_or(PermissionResponse::Deny {
                action_id: None,
                message: None,
                interrupt: false,
            });
            match resp {
                PermissionResponse::Allow { .. } => {
                    self.emit(StreamEvent::in_turn(
                        turn.clone(),
                        StreamEventKind::Timeline(TimelineItem::AssistantMessage {
                            text: "allowed".into(),
                        }),
                    ));
                }
                PermissionResponse::Deny { interrupt, .. } => {
                    self.emit(StreamEvent::in_turn(
                        turn.clone(),
                        StreamEventKind::Timeline(TimelineItem::AssistantMessage {
                            text: "denied".into(),
                        }),
                    ));
                    if interrupt {
                        // Deny + interrupt kills the whole turn — the
                        // daemon drives the interrupt right after the
                        // deny; model that the turn never completes.
                        self.emit(StreamEvent::in_turn(
                            turn.clone(),
                            StreamEventKind::TurnCanceled {
                                reason: "denied with interrupt".into(),
                            },
                        ));
                        return Ok(turn);
                    }
                }
            }
        } else if text.contains("hang") {
            // Hang mid-turn until interrupted. Like a real backend,
            // start_turn returns immediately and the turn ends via an
            // event when the interrupt lands — a blocking start_turn
            // would wrongly stall `turn.start` responses, including a
            // detached start's immediate reply.
            let (tx, rx) = oneshot::channel();
            *self.interrupt.lock().await = Some(tx);
            let parked_turn = turn.clone();
            let parked_events = self.events.clone();
            tokio::spawn(async move {
                let _ = rx.await;
                let _ = parked_events.send(StreamEvent::in_turn(
                    parked_turn,
                    StreamEventKind::TurnCanceled {
                        reason: "interrupted".into(),
                    },
                ));
            });
            return Ok(turn);
        } else if text.contains("fail") {
            // Fail the turn — exercises the durable error-marker path.
            self.emit(StreamEvent::in_turn(
                turn.clone(),
                StreamEventKind::TurnFailed {
                    error: "mock failure".into(),
                    code: Some("MOCK".into()),
                },
            ));
            return Ok(turn);
        } else {
            if text.contains("compact") {
                self.emit(StreamEvent::in_turn(
                    turn.clone(),
                    StreamEventKind::Timeline(TimelineItem::Compaction {
                        summary: "compacted 3 messages".into(),
                    }),
                ));
            }
            self.emit(StreamEvent::in_turn(
                turn.clone(),
                StreamEventKind::Timeline(TimelineItem::AssistantMessage {
                    text: format!("echo: {text}"),
                }),
            ));
        }

        self.emit(StreamEvent::in_turn(
            turn.clone(),
            StreamEventKind::TurnCompleted {
                usage: Some(Usage {
                    input_tokens: Some(10),
                    output_tokens: Some(5),
                    cost_usd: Some(0.001),
                    ..Default::default()
                }),
            },
        ));
        Ok(turn)
    }

    async fn interrupt(&self) -> Result<()> {
        if let Some(tx) = self.interrupt.lock().await.take() {
            let _ = tx.send(());
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn respond_to_permission(
        &self,
        request_id: &str,
        response: PermissionResponse,
    ) -> Result<()> {
        if let Some(p) = self.pending.lock().await.remove(request_id) {
            let _ = p.answer.send(response);
            self.emit(StreamEvent::new(StreamEventKind::PermissionResolved {
                request_id: request_id.to_string(),
            }));
        }
        Ok(())
    }

    fn persistence_handle(&self) -> Option<PersistenceHandle> {
        self.handle.try_lock().ok().and_then(|h| h.clone())
    }

    fn idle_secs(&self) -> u64 {
        self.idle_since
            .try_lock()
            .map(|t| t.elapsed().as_secs())
            .unwrap_or(0)
    }
}
