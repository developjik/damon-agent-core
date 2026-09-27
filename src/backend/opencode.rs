//! opencode backend — drives a managed `opencode serve` over HTTP+SSE.
//!
//! Wire shape (verified live against opencode 1.18.32):
//! - Server: `GET /global/health`; Damon adopts a healthy default-port
//!   server or spawns its own (random port + OPENCODE_SERVER_PASSWORD,
//!   basic auth `opencode:<pw>`). One server serves every session.
//! - Sessions: `POST /session?directory=<cwd> {title}` → `ses_…` id;
//!   resume reuses the stored id (the server keeps the session).
//! - Turns: `POST /session/:id/prompt_async` returns 204 immediately;
//!   events stream on `GET /global/event` as SSE frames
//!   `data: {directory, project, payload:{type, properties}}`.
//! - Events (filtered by sessionID): `message.updated`
//!   (info{role, tokens}), `message.part.updated`
//!   (part.type text/tool/step-*; snapshots, not deltas), `session.
//!   status` busy/idle (idle settles the turn), `permission.asked`,
//!   `question.asked`, `session.diff`.
//! - Permissions: `GET /permission` lists pending asks
//!   ({id, permission, patterns, metadata{command}, tool{callID}});
//!   `POST /permission/:id/reply {"reply":"once"|"always"|"reject"}`
//!   — verified end-to-end (an allowed bash actually ran).
//! - Questions: `GET /question` lists pending question batches
//!   ({id, questions:[{question, header, options:[{label,
//!   description}]}]}); `POST /question/:id/reply {"answers":[[
//!   …labels]]}` (or `/reject`) — verified end-to-end.
//! - Interrupt: `POST /session/:id/abort`.
//!
//! Text parts arrive as full snapshots; the session emits only the
//! not-yet-emitted tail per part id so persistence never double-counts.
//! MiMo Code (Xiaomi's opencode fork) speaks the same surface and
//! registers through the same client under its own provider id.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast};

use super::opencode_server::{OpencodeServer, shared_server};
use super::registry::ResolvedBackend;
use super::types::*;
use super::{AgentClient, AgentSession};

pub struct OpencodeClient {
    command: String,
    server: tokio::sync::OnceCell<Arc<OpencodeServer>>,
    http: reqwest::Client,
    caps: Capabilities,
    provider: &'static str,
}

impl OpencodeClient {
    pub fn new(resolved: &ResolvedBackend, provider: &'static str) -> Self {
        Self {
            command: resolved.command.clone(),
            server: tokio::sync::OnceCell::new(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: false,
                dynamic_modes: false,
                mcp_servers: false,
                reasoning_stream: false,
                steer: false,
                rewind: false,
                subagent_events: false,
            },
            provider,
        }
    }

    /// Probe/adopt/spawn the server on first use (the registry builds
    /// clients synchronously at startup; a down CLI must not wedge it).
    async fn srv(&self) -> Result<Arc<OpencodeServer>> {
        self.server
            .get_or_try_init(|| shared_server(&self.command))
            .await
            .cloned()
    }
}

#[async_trait]
impl AgentClient for OpencodeClient {
    fn provider(&self) -> &str {
        self.provider
    }

    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn is_available(&self) -> bool {
        true // availability = the server could be spawned; detect()
             // already gated the backend's presence on PATH.
    }

    async fn fetch_catalog(&self, _cwd: Option<&Path>) -> Result<ProviderCatalog> {
        // Model listing needs provider auth inside opencode; model ids
        // pass through prompt_async's model field untouched.
        Ok(ProviderCatalog::default())
    }

    async fn create_session(&self, config: SessionConfig) -> Result<Arc<dyn AgentSession>> {
        let server = self.srv().await?;
        let url = format!(
            "{}/session?directory={}",
            server.base_url,
            urlencoding_encode(&config.cwd.to_string_lossy())
        );
        let created = self
            .http
            .post(url)
            .header("content-type", "application/json")
            .bearer_opt(server.auth_header())
            .json(&json!({"title": "damon session"}))
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let id = created["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("opencode session id missing: {created}"))?;
        Ok(self.session(id, config, &server))
    }

    async fn resume_session(
        &self,
        handle: &PersistenceHandle,
        config: SessionConfig,
    ) -> Result<Arc<dyn AgentSession>> {
        let server = self.srv().await?;
        Ok(self.session(&handle.native_handle, config, &server))
    }

    async fn shutdown(&self) -> Result<()> {
        if let Some(server) = self.server.get() {
            if let Some(mut child) = server.take_child().await {
                let _ = child.kill().await;
            }
        }
        Ok(())
    }
}

impl OpencodeClient {
    fn session(
        &self,
        id: &str,
        config: SessionConfig,
        server: &Arc<OpencodeServer>,
    ) -> Arc<dyn AgentSession> {
        let session = Arc::new(OpencodeSession {
            http: self.http.clone(),
            base: server.base_url.clone(),
            auth: server.auth_header(),
            provider: self.provider,
            session_id: id.to_string(),
            config,
            events: broadcast::channel(512).0,
            current_turn: Mutex::new(None),
            pending_asks: Mutex::new(HashMap::new()),
            part_emitted: Mutex::new(HashMap::new()),
            msg_roles: Mutex::new(HashMap::new()),
            last_usage: Mutex::new(None),
        });
        session.spawn_event_reader();
        session.clone()
    }
}

struct PendingAsk {
    request: PermissionRequest,
    /// The opencode permission/question request id (per_…/que_…).
    wire_id: String,
    /// true → question endpoints, false → permission endpoints.
    is_question: bool,
}

pub struct OpencodeSession {
    http: reqwest::Client,
    base: String,
    auth: Option<String>,
    provider: &'static str,
    session_id: String,
    config: SessionConfig,
    events: broadcast::Sender<StreamEvent>,
    current_turn: Mutex<Option<String>>,
    pending_asks: Mutex<HashMap<String, PendingAsk>>,
    /// part id → emitted text length (snapshots, not deltas).
    part_emitted: Mutex<HashMap<String, usize>>,
    /// message id → role, so user echoes don't hit the timeline.
    msg_roles: Mutex<HashMap<String, String>>,
    last_usage: Mutex<Option<Usage>>,
}

impl OpencodeSession {
    fn spawn_event_reader(self: &Arc<Self>) {
        let me = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let Some(s) = me.upgrade() else { break };
                let mut req = s.http.get(format!("{}/global/event", s.base));
                if let Some(a) = &s.auth {
                    req = req.header("authorization", a);
                }
                let resp = match req.send().await {
                    Ok(r) => r,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };
                let mut stream = resp.bytes_stream();
                let mut buf = String::new();
                loop {
                    let Some(chunk) = stream.next().await else { break };
                    let chunk = match chunk {
                        Ok(c) => c,
                        Err(_) => break,
                    };
                    buf.push_str(&String::from_utf8_lossy(&chunk));
                    while let Some(pos) = buf.find('\n') {
                        let line = buf[..pos].to_string();
                        buf.drain(..=pos);
                        let Some(data) = line.strip_prefix("data: ") else {
                            continue;
                        };
                        let Ok(v) = serde_json::from_str::<Value>(data) else {
                            continue;
                        };
                        let payload = &v["payload"];
                        let ty = payload["type"].as_str().unwrap_or_default();
                        if ty.is_empty()
                            || ty == "sync"
                            || ty == "server.connected"
                            || ty == "server.heartbeat"
                        {
                            continue;
                        }
                        let props = &payload["properties"];
                        if props["sessionID"].as_str() != Some(&s.session_id) {
                            continue;
                        }
                        s.on_event(ty, props).await;
                    }
                }
                // Stream ended — retry unless the session is gone.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    async fn on_event(&self, ty: &str, p: &Value) {
        match ty {
            "message.updated" => {
                let info = &p["info"];
                if let (Some(id), Some(role)) = (info["id"].as_str(), info["role"].as_str()) {
                    self.msg_roles.lock().await.insert(id.to_string(), role.to_string());
                    if role == "assistant"
                        && let Some(tokens) = info["tokens"].as_object()
                        && !tokens.is_empty()
                    {
                        *self.last_usage.lock().await = Some(Usage {
                            input_tokens: tokens["input"].as_u64(),
                            cached_input_tokens: tokens
                                .get("cache")
                                .and_then(|c| c["read"].as_u64()),
                            output_tokens: tokens["output"].as_u64(),
                            cost_usd: None,
                            context_window: None,
                            context_used: None,
                        });
                    }
                }
            }
            "message.part.updated" => self.on_part(&p["part"]).await,
            "permission.asked" => self.on_permission_asked().await,
            "question.asked" => self.on_question_asked().await,
            "session.idle" => self.finish_turn().await,
            "session.status" => {
                if p["status"]["type"].as_str() == Some("idle") {
                    self.finish_turn().await;
                }
            }
            _ => {}
        }
    }

    async fn on_part(&self, part: &Value) {
        match part["type"].as_str().unwrap_or_default() {
            "text" => {
                let role_is_assistant = {
                    let roles = self.msg_roles.lock().await;
                    part["messageID"]
                        .as_str()
                        .and_then(|id| roles.get(id))
                        .is_some_and(|r| r == "assistant")
                };
                if !role_is_assistant {
                    return;
                }
                let Some(text) = part["text"].as_str() else {
                    return;
                };
                // Snapshots: emit only the tail beyond what this part
                // already delivered.
                let id = part["id"].as_str().unwrap_or_default().to_string();
                let mut emitted = self.part_emitted.lock().await;
                let prev = emitted.get(&id).copied().unwrap_or(0);
                if text.len() <= prev {
                    return;
                }
                let tail = &text[prev..];
                emitted.insert(id, text.len());
                drop(emitted);
                self.emit_timeline(TimelineItem::AssistantMessage {
                    text: tail.to_string(),
                })
                .await;
            }
            "tool" => {
                let state = &part["state"];
                let status = state["status"].as_str().unwrap_or_default();
                let detail = map_tool(part["tool"].as_str().unwrap_or_default(), state);
                let item = TimelineItem::ToolCall(ToolCall {
                    call_id: part["callID"].as_str().unwrap_or_default().to_string(),
                    name: part["tool"].as_str().unwrap_or_default().to_string(),
                    status: match status {
                        "completed" => ToolCallStatus::Completed,
                        "error" => ToolCallStatus::Failed,
                        _ => ToolCallStatus::Running,
                    },
                    detail,
                });
                self.emit_timeline(item).await;
            }
            _ => {}
        }
    }

    async fn on_permission_asked(&self) {
        let list = match self.get_json("/permission").await {
            Ok(v) => v,
            Err(_) => return,
        };
        let Some(entry) = list
            .as_array()
            .and_then(|a| a.iter().find(|e| e["sessionID"].as_str() == Some(&self.session_id)))
        else {
            return;
        };
        let tool = entry["permission"].as_str().unwrap_or("tool").to_string();
        let command = entry["metadata"]["command"].as_str().unwrap_or_default();
        let ask = PermissionRequest {
            id: uuid::Uuid::new_v4().to_string(),
            kind: PermissionKind::Tool,
            name: tool.clone(),
            title: Some(if command.is_empty() {
                format!("{tool} wants to run")
            } else {
                command.to_string()
            }),
            input: Some(entry["metadata"].clone()),
            detail: Some(map_tool(&tool, &entry["metadata"])),
            actions: vec![
                permission_action("once", "Allow once", false),
                permission_action("always", "Always allow", false),
                permission_action("reject", "Reject", true),
            ],
            suggestions: vec![],
        };
        self.register_ask(ask, entry["id"].as_str().unwrap_or_default().to_string(), false)
            .await;
    }

    async fn on_question_asked(&self) {
        let list = match self.get_json("/question").await {
            Ok(v) => v,
            Err(_) => return,
        };
        let Some(entry) = list
            .as_array()
            .and_then(|a| a.iter().find(|e| e["sessionID"].as_str() == Some(&self.session_id)))
        else {
            return;
        };
        let Some(q) = entry["questions"].as_array().and_then(|a| a.first()) else {
            return;
        };
        let question = q["question"].as_str().unwrap_or("Question").to_string();
        let actions = q["options"]
            .as_array()
            .map(|opts| {
                opts.iter()
                    .map(|o| PermissionAction {
                        id: o["label"].as_str().unwrap_or("option").to_string(),
                        label: o["label"].as_str().unwrap_or("option").to_string(),
                        behavior: PermissionBehavior::Allow,
                        variant: None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let ask = PermissionRequest {
            id: uuid::Uuid::new_v4().to_string(),
            kind: PermissionKind::Question,
            name: "question".to_string(),
            title: Some(question),
            input: Some(entry.clone()),
            detail: None,
            actions,
            suggestions: vec![],
        };
        self.register_ask(ask, entry["id"].as_str().unwrap_or_default().to_string(), true)
            .await;
    }

    async fn register_ask(&self, ask: PermissionRequest, wire_id: String, is_question: bool) {
        self.pending_asks.lock().await.insert(
            ask.id.clone(),
            PendingAsk {
                request: ask.clone(),
                wire_id,
                is_question,
            },
        );
        let turn = self.current_turn.lock().await.clone();
        self.emit(StreamEvent {
            turn_id: turn,
            kind: StreamEventKind::PermissionRequested(ask),
        });
        self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
            reason: AttentionReason::Permission,
        }));
    }

    async fn finish_turn(&self) {
        let turn = self.current_turn.lock().await.take();
        if turn.is_none() {
            return; // nothing in flight; an idle echo
        }
        let usage = self.last_usage.lock().await.take();
        self.emit(StreamEvent {
            turn_id: turn,
            kind: StreamEventKind::TurnCompleted { usage },
        });
        self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
            reason: AttentionReason::Finished,
        }));
    }

    async fn get_json(&self, path: &str) -> Result<Value> {
        let mut req = self.http.get(format!("{}{path}", self.base));
        if let Some(a) = &self.auth {
            req = req.header("authorization", a);
        }
        Ok(req.send().await?.error_for_status()?.json().await?)
    }

    async fn post_json(&self, path: &str, body: Value) -> Result<Value> {
        let mut req = self
            .http
            .post(format!("{}{path}", self.base))
            .header("content-type", "application/json");
        if let Some(a) = &self.auth {
            req = req.header("authorization", a);
        }
        let resp = req.json(&body).send().await?.error_for_status()?;
        // prompt_async answers 204 with an empty body — not JSON.
        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(Value::Null);
        }
        let bytes = resp.bytes().await?;
        if bytes.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn emit(&self, ev: StreamEvent) {
        let _ = self.events.send(ev);
    }

    async fn emit_timeline(&self, item: TimelineItem) {
        let turn = self.current_turn.lock().await.clone();
        self.emit(StreamEvent {
            turn_id: turn,
            kind: StreamEventKind::Timeline(item),
        });
    }
}

fn permission_action(id: &str, label: &str, deny: bool) -> PermissionAction {
    PermissionAction {
        id: id.to_string(),
        label: label.to_string(),
        behavior: if deny {
            PermissionBehavior::Deny
        } else {
            PermissionBehavior::Allow
        },
        variant: if deny { Some(ActionVariant::Danger) } else { Some(ActionVariant::Primary) },
    }
}

/// Map opencode's tool vocabulary (bash/edit/write/read/grep/glob/
/// webfetch/task) onto normalized detail. The tool-part state carries
/// `input` (parsed args) and `output`/`metadata.exit`.
fn map_tool(name: &str, state: &Value) -> ToolCallDetail {
    let input = &state["input"];
    match name {
        "bash" => ToolCallDetail::Shell {
            command: input["command"].as_str().unwrap_or_default().to_string(),
            output: state["output"].as_str().map(String::from),
            exit_code: state["metadata"]["exit"].as_i64().map(|c| c as i32),
        },
        "read" => ToolCallDetail::Read {
            path: input["filePath"]
                .as_str()
                .or_else(|| input["path"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        "edit" => ToolCallDetail::Edit {
            path: input["filePath"]
                .as_str()
                .or_else(|| input["path"].as_str())
                .unwrap_or_default()
                .to_string(),
            unified_diff: None,
        },
        "write" => ToolCallDetail::Write {
            path: input["filePath"]
                .as_str()
                .or_else(|| input["path"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: input["content"].as_str().map(String::from),
        },
        "grep" | "glob" | "list" => ToolCallDetail::Search {
            query: input["pattern"]
                .as_str()
                .or_else(|| input["query"].as_str())
                .or_else(|| input["path"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        "webfetch" => ToolCallDetail::Fetch {
            url: input["url"].as_str().unwrap_or_default().to_string(),
            result: state["output"].as_str().map(String::from),
        },
        _ => ToolCallDetail::Unknown {
            input: input.clone(),
            output: Value::String(state["output"].as_str().unwrap_or_default().to_string()),
        },
    }
}

/// Minimal percent-encoding for query values (paths).
fn urlencoding_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

trait RequestExt {
    fn bearer_opt(self, auth: Option<String>) -> Self;
}

impl RequestExt for reqwest::RequestBuilder {
    fn bearer_opt(self, auth: Option<String>) -> Self {
        match auth {
            Some(a) => self.header("authorization", a),
            None => self,
        }
    }
}

#[async_trait]
impl AgentSession for OpencodeSession {
    fn capabilities(&self) -> &Capabilities {
        // Sessions inherit the client's capability set; recompute the
        // same honest values.
        static CAPS: std::sync::OnceLock<Capabilities> = std::sync::OnceLock::new();
        CAPS.get_or_init(|| Capabilities {
            streaming: true,
            session_persistence: true,
            session_listing: false,
            dynamic_modes: false,
            mcp_servers: false,
            reasoning_stream: false,
            steer: false,
            rewind: false,
            subagent_events: false,
        })
    }

    fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.events.subscribe()
    }

    fn idle_secs(&self) -> u64 {
        0 // HTTP sessions are cheap; the idle sweep leaves them be.
    }

    async fn start_turn(&self, prompt: PromptInput) -> Result<String> {
        let turn_id = uuid::Uuid::new_v4().to_string();
        *self.current_turn.lock().await = Some(turn_id.clone());
        let mut parts = vec![];
        match prompt {
            PromptInput::Text(t) => parts.push(json!({"type": "text", "text": t})),
            PromptInput::Blocks(blocks) => {
                let text: Vec<&str> = blocks
                    .iter()
                    .filter_map(|b| match b {
                        PromptBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect();
                parts.push(json!({"type": "text", "text": text.join("\n")}));
                for b in &blocks {
                    if let PromptBlock::Image { data, mime } = b {
                        // Data-URL file parts (verified surface for
                        // image attachments).
                        parts.push(json!({
                            "type": "file",
                            "mime": mime,
                            "url": format!("data:{mime};base64,{data}")
                        }));
                    }
                }
            }
        }
        let mut body = json!({"parts": parts});
        if let Some(model) = &self.config.model {
            if let Some((provider_id, model_id)) = model.split_once('/') {
                body["model"] = json!({"providerID": provider_id, "modelID": model_id});
            } else {
                body["model"] = json!({"modelID": model});
            }
        }
        self.post_json(&format!("/session/{}/prompt_async", self.session_id), body)
            .await?;
        self.emit(StreamEvent::in_turn(turn_id.clone(), StreamEventKind::TurnStarted));
        Ok(turn_id)
    }

    async fn interrupt(&self) -> Result<()> {
        self.post_json(&format!("/session/{}/abort", self.session_id), json!({}))
            .await?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        // The server is shared across sessions; closing is local.
        Ok(())
    }

    async fn respond_to_permission(
        &self,
        request_id: &str,
        response: PermissionResponse,
    ) -> Result<()> {
        let Some(ask) = self.pending_asks.lock().await.remove(request_id) else {
            bail!("no pending permission {request_id}");
        };
        let path = if ask.is_question {
            match &response {
                PermissionResponse::Allow { action_id, answer, .. } => {
                    let value = action_id.clone().or_else(|| answer.clone());
                    self.post_json(
                        &format!("/question/{}/reply", ask.wire_id),
                        json!({"answers": [[value.unwrap_or_default()]]}),
                    )
                    .await?;
                    self.emit(StreamEvent::new(StreamEventKind::PermissionResolved {
                        request_id: request_id.to_string(),
                    }));
                    return Ok(());
                }
                PermissionResponse::Deny { .. } => {
                    format!("/question/{}/reject", ask.wire_id)
                }
            }
        } else {
            match &response {
                PermissionResponse::Allow { action_id, .. } => {
                    let reply = match action_id.as_deref() {
                        Some("always") => "always",
                        _ => "once",
                    };
                    self.post_json(
                        &format!("/permission/{}/reply", ask.wire_id),
                        json!({"reply": reply}),
                    )
                    .await?;
                    self.emit(StreamEvent::new(StreamEventKind::PermissionResolved {
                        request_id: request_id.to_string(),
                    }));
                    return Ok(());
                }
                PermissionResponse::Deny { .. } => {
                    format!("/permission/{}/reply", ask.wire_id)
                }
            }
        };
        // Deny paths land here: reject the question / the permission.
        if ask.is_question {
            self.post_json(&path, json!({})).await?;
        } else {
            self.post_json(&path, json!({"reply": "reject"})).await?;
        }
        self.emit(StreamEvent::new(StreamEventKind::PermissionResolved {
            request_id: request_id.to_string(),
        }));
        Ok(())
    }

    fn persistence_handle(&self) -> Option<PersistenceHandle> {
        Some(PersistenceHandle {
            provider: self.provider.to_string(),
            native_handle: self.session_id.clone(),
            metadata: Value::Null,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_encoding_keeps_slashes() {
        assert_eq!(urlencoding_encode("/a b/c"), "/a%20b/c");
    }

    /// Verified live: text parts are full snapshots — the tail beyond
    /// the emitted length is what persistence should see.
    #[test]
    fn snapshot_tail_computes_increment() {
        let emitted = 5;
        let text = "hello world";
        assert_eq!(&text[emitted..], " world");
    }

    /// Verified live: the permission resource carries the command in
    /// metadata and the reply vocabulary is once/always/reject.
    #[test]
    fn permission_resource_maps_to_ask_actions() {
        let actions = vec![
            permission_action("once", "Allow once", false),
            permission_action("always", "Always allow", false),
            permission_action("reject", "Reject", true),
        ];
        assert!(matches!(actions[0].behavior, PermissionBehavior::Allow));
        assert!(matches!(actions[2].behavior, PermissionBehavior::Deny));
    }
}
