//! Qwen Code dialect — `qwen -p` in bidirectional stream-json mode.
//!
//! Wire shape (verified live against qwen-code 0.24.6, driving the
//! real binary against a fake OpenAI endpoint):
//! - Handshake: the client must send a `control_request initialize`
//!   BEFORE the first prompt. Without it the control system stays
//!   down and permission asks park forever (a write_file approval sat
//!   silent past 75 s in verification). The CLI answers with its
//!   capabilities (can_handle_can_use_tool, can_set_model,
//!   can_set_permission_mode, can_set_effort, ...).
//! - stdout: Claude-shaped frames — `system.init` (session_id, tools,
//!   model, permission_mode), `stream_event`-wrapped Anthropic SSE
//!   (text_delta / input_json_delta; needs --include-partial-messages),
//!   `assistant` messages, `user` frames carrying tool_result blocks,
//!   and one `result` envelope per turn with usage
//!   (input/output/cache_read_input_tokens). Failures report
//!   `error.message` (an object, unlike claude's string field).
//! - stdin: `{"type":"user",...}` prompts; `control_request
//!   can_use_tool` asks answered by `control_response` with
//!   `{behavior:"allow"|"deny"}` — verified end-to-end (an allowed
//!   write_file actually wrote the file).
//!
//! Not supported on this wire: image input — stdin content blocks are
//! text/thinking/tool_use/tool_result only (checked against the CLI's
//! input schema and live), so `prompt_frame` rejects image blocks
//! loudly instead of dropping them. Session import reads the CLI's own
//! `~/.qwen/projects/<cwd-slug>/chats/<session-id>.jsonl` transcripts.
//!
//! One process = one conversation. Resume spawns a fresh process with
//! `--resume <session_id>`.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};

use super::streamjson::{
    ClaudeFrameParser, ClaudeQuirks, ControlOp, SessionCtx, StreamJsonDialect,
};
use super::transport::{NdjsonTransport, request_roundtrip};
use super::types::*;

pub struct QwenDialect {
    parser: ClaudeFrameParser,
}

impl QwenDialect {
    pub fn dialect() -> Arc<dyn StreamJsonDialect> {
        Arc::new(Self {
            parser: ClaudeFrameParser::new(ClaudeQuirks {
                tool_detail: map_tool_input,
                // error is an object ({message}), usage may be absent on
                // failed turns; no cost or context window observed.
                error_field_first: true,
                usage_optional: true,
                ..ClaudeQuirks::default()
            }),
        })
    }
}

#[async_trait]
impl StreamJsonDialect for QwenDialect {
    fn launch_args(&self, config: &SessionConfig, resume: Option<&str>) -> Vec<String> {
        let mut args = vec![];
        if let Some(mode) = &config.mode {
            args.push("--approval-mode".to_string());
            args.push(mode.clone());
        }
        if let Some(model) = &config.model {
            args.push("--model".to_string());
            args.push(model.clone());
        }
        if let Some(id) = resume {
            args.push("--resume".to_string());
            args.push(id.to_string());
        }
        args
    }

    async fn on_frame(&self, session: &Arc<dyn SessionCtx>, f: &Value) {
        let ty = f["type"].as_str().unwrap_or_default();
        match ty {
            "control_request" => self.on_control_request(session, f).await,
            "control_response" => {
                // Answer to a control request WE sent (initialize,
                // interrupt, set_model, set_permission_mode).
                let id = f["response"]["request_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                let resp = &f["response"]["response"];
                let result = if resp["subtype"].as_str() == Some("error")
                    || f["response"]["subtype"].as_str() == Some("error")
                {
                    Err(resp.clone())
                } else {
                    Ok(resp.clone())
                };
                session.resolve_wire(&id, result).await;
            }
            _ => self.parser.on_frame(session, f).await,
        }
    }

    fn prompt_frame(&self, prompt: &PromptInput) -> Result<Value> {
        let content = match prompt {
            PromptInput::Text(t) => json!(t),
            PromptInput::Blocks(blocks) => {
                let mut parts = vec![];
                for b in blocks {
                    match b {
                        PromptBlock::Text { text } => {
                            parts.push(json!({"type": "text", "text": text}))
                        }
                        PromptBlock::Image { .. } => bail!(
                            "qwen stream-json input accepts text blocks only \
                             (verified against 0.24.6); image attachments are \
                             not supported on this wire"
                        ),
                    }
                }
                json!(parts)
            }
        };
        Ok(json!({
            "type": "user",
            "message": {"role": "user", "content": content}
        }))
    }

    fn permission_response_frame(
        &self,
        ask: &PermissionRequest,
        wire_id: &str,
        response: &PermissionResponse,
    ) -> Value {
        let resp = match response {
            PermissionResponse::Allow { updated_input, .. } => json!({
                "behavior": "allow",
                "updatedInput": updated_input.clone().unwrap_or_else(|| {
                    ask.input.clone().unwrap_or(Value::Null)
                }),
            }),
            PermissionResponse::Deny { message, .. } => json!({
                "behavior": "deny",
                "message": message.clone().unwrap_or_else(|| "denied by user".to_string()),
            }),
        };
        json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": wire_id,
                "response": resp
            }
        })
    }

    fn control_frame(&self, request_id: &str, op: &ControlOp) -> Option<Value> {
        let request = match op {
            ControlOp::Interrupt => json!({"subtype": "interrupt"}),
            ControlOp::SetModel(model) => json!({"subtype": "set_model", "model": model}),
            ControlOp::SetMode(mode) => {
                json!({"subtype": "set_permission_mode", "mode": mode})
            }
        };
        Some(json!({
            "type": "control_request",
            "request_id": request_id,
            "request": request
        }))
    }

    /// The initialize handshake — see the module doc. Control subtypes
    /// verified against qwen-code 0.24.6's protocol.ts.
    async fn handshake(
        &self,
        transport: &Arc<NdjsonTransport>,
        _config: &SessionConfig,
    ) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        request_roundtrip(
            transport,
            id.clone(),
            json!({
                "type": "control_request",
                "request_id": id,
                "request": {"subtype": "initialize"}
            }),
            Duration::from_secs(20),
        )
        .await?;
        Ok(())
    }

    fn catalog(&self) -> ProviderCatalog {
        // No model-listing wire on a cold process (get_available_models
        // exists as a control request but needs a live session); model
        // ids pass through set_model untouched, so an empty list still
        // allows explicit model selection at session.create.
        ProviderCatalog {
            models: vec![],
            modes: ["default", "plan", "auto-edit", "auto", "yolo"]
                .iter()
                .map(|m| ModeDef {
                    id: m.to_string(),
                    name: m.to_string(),
                    description: None,
                })
                .collect(),
            default_mode: Some("default".to_string()),
        }
    }

    /// Qwen records every session as
    /// `~/.qwen/projects/<cwd-slug>/chats/<session-id>.jsonl`.
    async fn list_importable(&self, cwd: &Path) -> Result<Vec<ImportableSession>> {
        let home = directories::BaseDirs::new()
            .map(|b| b.home_dir().to_path_buf())
            .unwrap_or_default();
        let slug = cwd.to_string_lossy().replace('/', "-");
        let dir = home.join(".qwen").join("projects").join(slug).join("chats");
        let mut out = vec![];
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(_) => return Ok(out),
        };
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let modified = entry
                .metadata()
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs());
            out.push(ImportableSession {
                handle: PersistenceHandle {
                    provider: "qwen".to_string(),
                    native_handle: stem.to_string(),
                    metadata: json!({"transcript": path.to_string_lossy()}),
                },
                title: transcript_title(&path).await,
                cwd: Some(cwd.to_path_buf()),
                modified_at: modified,
            });
        }
        Ok(out)
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            streaming: true,
            session_persistence: true,
            session_listing: true,
            dynamic_modes: true,
            mcp_servers: false,
            reasoning_stream: true,
            steer: true,
            rewind: false,
            subagent_events: false,
        }
    }
}

impl QwenDialect {
    /// Agent → daemon control requests. `can_use_tool` becomes a
    /// PermissionRequest (ask_user_question questions ride the same
    /// channel); anything else gets a minimal success response so the
    /// agent isn't blocked on a handshake we don't implement.
    async fn on_control_request(&self, session: &Arc<dyn SessionCtx>, f: &Value) {
        let request_id = f["request_id"].as_str().unwrap_or_default().to_string();
        let req = &f["request"];
        match req["subtype"].as_str().unwrap_or_default() {
            "can_use_tool" => {
                let tool = req["tool_name"].as_str().unwrap_or_default().to_string();
                let input = req["input"].clone();
                let ask = PermissionRequest {
                    id: uuid::Uuid::new_v4().to_string(),
                    kind: PermissionKind::Tool,
                    name: tool.clone(),
                    title: Some(format!("{tool} wants to run")),
                    input: Some(input.clone()),
                    detail: Some(map_tool_input(&tool, &input)),
                    actions: vec![
                        PermissionAction {
                            id: "allow".to_string(),
                            label: "Allow".to_string(),
                            behavior: PermissionBehavior::Allow,
                            variant: Some(ActionVariant::Primary),
                        },
                        PermissionAction {
                            id: "deny".to_string(),
                            label: "Deny".to_string(),
                            behavior: PermissionBehavior::Deny,
                            variant: Some(ActionVariant::Danger),
                        },
                    ],
                    suggestions: req["permission_suggestions"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default(),
                };
                session.register_ask(ask, request_id).await;
            }
            _ => {
                let _ = session
                    .send_raw(json!({
                        "type": "control_response",
                        "response": {
                            "subtype": "success",
                            "request_id": request_id,
                            "response": {}
                        }
                    }))
                    .await;
            }
        }
    }
}

/// Map qwen's snake_case tool vocabulary (observed in the init frame's
/// tools list) to normalized detail. Unknown tools keep the raw input
/// so nothing is silently dropped.
fn map_tool_input(name: &str, input: &Value) -> ToolCallDetail {
    match name {
        "run_shell_command" => ToolCallDetail::Shell {
            command: input["command"].as_str().unwrap_or_default().to_string(),
            output: None,
            exit_code: None,
        },
        "read_file" => ToolCallDetail::Read {
            path: input["file_path"]
                .as_str()
                .or_else(|| input["absolute_path"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        "edit" => ToolCallDetail::Edit {
            path: input["file_path"]
                .as_str()
                .or_else(|| input["absolute_path"].as_str())
                .unwrap_or_default()
                .to_string(),
            unified_diff: None,
        },
        "write_file" => ToolCallDetail::Write {
            path: input["file_path"]
                .as_str()
                .or_else(|| input["absolute_path"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: input["content"].as_str().map(|s| s.to_string()),
        },
        "grep_search" | "glob" => ToolCallDetail::Search {
            query: input["pattern"]
                .as_str()
                .or_else(|| input["query"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        "web_fetch" => ToolCallDetail::Fetch {
            url: input["url"].as_str().unwrap_or_default().to_string(),
            result: None,
        },
        _ => ToolCallDetail::Unknown {
            input: input.clone(),
            output: Value::Null,
        },
    }
}

/// First user message of a chats jsonl, whitespace-collapsed and
/// truncated — mirrors the claude transcript-title reader over qwen's
/// session record shape (`{"type":"user","message":{...}}`).
async fn transcript_title(path: &Path) -> Option<String> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let f = tokio::fs::File::open(path).await.ok()?;
    let mut lines = BufReader::new(f).lines();
    for _ in 0..80 {
        let line = lines.next_line().await.ok()?;
        let Some(line) = line else { break };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        if v["type"] != "user" {
            continue;
        }
        let content = &v["message"]["content"];
        let text = match content {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Array(parts) => Some(
                parts
                    .iter()
                    .filter_map(|p| p["text"].as_str())
                    .collect::<Vec<_>>()
                    .join(" "),
            ),
            _ => None,
        }?;
        let collapsed: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
        if collapsed.is_empty() {
            continue;
        }
        let cut = collapsed.floor_char_boundary(80);
        return Some(collapsed[..cut].to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::streamjson::tests::TestCtx;
    use crate::backend::types::StreamEventKind;

    #[test]
    fn capabilities_match_the_verified_wire() {
        let c = QwenDialect::dialect().capabilities();
        // Permission relay, steering, streaming, resume verified live;
        // images are not (text-only stdin), so nothing is overstated.
        assert!(c.streaming && c.reasoning_stream && c.steer && c.session_persistence);
        assert!(!c.mcp_servers);
        assert!(!c.subagent_events);
    }

    #[test]
    fn control_frames_use_the_verified_subtypes() {
        let d = QwenDialect::dialect();
        let f = d.control_frame("r1", &ControlOp::Interrupt).unwrap();
        assert_eq!(f["request"]["subtype"], "interrupt");
        let f = d
            .control_frame("r2", &ControlOp::SetModel("qwen3.7-max".into()))
            .unwrap();
        assert_eq!(f["request"]["subtype"], "set_model");
        assert_eq!(f["request"]["model"], "qwen3.7-max");
        let f = d
            .control_frame("r3", &ControlOp::SetMode("plan".into()))
            .unwrap();
        assert_eq!(f["request"]["subtype"], "set_permission_mode");
        assert_eq!(f["request"]["mode"], "plan");
    }

    #[test]
    fn prompt_frame_text_only_and_images_error_loudly() {
        let d = QwenDialect::dialect();
        let f = d
            .prompt_frame(&PromptInput::Blocks(vec![PromptBlock::Text {
                text: "hi".to_string(),
            }]))
            .unwrap();
        assert_eq!(f["message"]["content"][0]["type"], "text");
        assert!(
            d.prompt_frame(&PromptInput::Blocks(vec![PromptBlock::Image {
                data: "AAAA".to_string(),
                mime: "image/png".to_string(),
            }]))
            .is_err()
        );
    }

    /// qwen routes control_responses by the `subtype` discriminator —
    /// without "success" an allow is parsed as an error and the tool is
    /// cancelled (found live in the damon<->qwen E2E; claude does not
    /// require it).
    #[test]
    fn permission_response_carries_the_success_discriminator() {
        let d = QwenDialect::dialect();
        let ask = PermissionRequest {
            id: "a1".to_string(),
            kind: PermissionKind::Tool,
            name: "write_file".to_string(),
            title: None,
            input: Some(json!({"file_path": "/tmp/x"})),
            detail: None,
            actions: vec![],
            suggestions: vec![],
        };
        let f = d.permission_response_frame(
            &ask,
            "req-9",
            &PermissionResponse::Allow {
                action_id: None,
                updated_input: None,
                answer: None,
            },
        );
        assert_eq!(f["response"]["subtype"], "success");
        assert_eq!(f["response"]["request_id"], "req-9");
        assert_eq!(f["response"]["response"]["behavior"], "allow");
        assert_eq!(
            f["response"]["response"]["updatedInput"]["file_path"],
            "/tmp/x"
        );
    }

    #[tokio::test]
    async fn can_use_tool_registers_a_permission_ask() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        QwenDialect::dialect()
            .on_frame(
                &ctx,
                &json!({
                    "type": "control_request",
                    "request_id": "req-1",
                    "request": {
                        "subtype": "can_use_tool",
                        "tool_name": "write_file",
                        "tool_use_id": "call_1",
                        "input": {"file_path": "/tmp/x.txt", "content": "hi"},
                        "permission_suggestions": null,
                        "blocked_path": null
                    }
                }),
            )
            .await;
        let (ask, wire_id) = t.pending_ask().await.expect("ask registered");
        assert_eq!(ask.name, "write_file");
        assert_eq!(wire_id, "req-1");
        assert!(matches!(ask.detail, Some(ToolCallDetail::Write { .. })));
    }

    /// The live-observed failure envelope: error is an object with a
    /// message field, usage still present.
    #[tokio::test]
    async fn result_error_object_yields_the_message() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        QwenDialect::dialect()
            .on_frame(
                &ctx,
                &json!({
                    "type": "result",
                    "subtype": "error_during_execution",
                    "session_id": "s1",
                    "is_error": true,
                    "usage": {"input_tokens": 3, "output_tokens": 0},
                    "error": {"message": "workspace routing discovery unauthorized (401)"}
                }),
            )
            .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::TurnFailed { error, .. }) = events.last().map(|e| &e.kind) else {
            panic!(
                "expected TurnFailed, got {:?}",
                events.last().map(|e| &e.kind)
            );
        };
        assert_eq!(error, "workspace routing discovery unauthorized (401)");
    }
}
