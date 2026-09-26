//! Claude Code dialect — `claude -p` in bidirectional stream-json mode
//! (`--output-format stream-json --input-format stream-json`).
//!
//! Wire shape (observed against claude 2.1.270):
//! - stdout: `system.init` (session_id, tools, model, permissionMode),
//!   `assistant` messages (content blocks incl. tool_use), `user`
//!   messages carrying tool_result blocks, `system.*` housekeeping
//!   (hooks, thinking_tokens), and a final `result` frame per turn.
//! - stdin: `{"type":"user","message":{...}}` prompts, plus
//!   `control_request`/`control_response` frames for the SDK control
//!   protocol (can_use_tool permission asks, interrupt, set_model,
//!   set_permission_mode).
//!
//! Task tool_use/tool_result pairs additionally raise Subagent
//! lifecycle events (omp-shaped: name/description/status/state) so
//! consumers see subagents uniformly across backends; the tool_call
//! timeline items are still emitted alongside.
//!
//! One process = one conversation. Resume spawns a fresh process with
//! `--resume <session_id>`; Claude's own transcript file is the durable
//! record.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{Value, json};

use super::streamjson::{
    ClaudeFrameParser, ClaudeQuirks, ControlOp, SessionCtx, StreamJsonDialect,
};
use super::types::*;

pub struct ClaudeDialect {
    /// Shared Claude-frame translator; the claude preset adds the
    /// Task→Subagent lifecycle events and vendor tool mapping.
    parser: ClaudeFrameParser,
}

impl ClaudeDialect {
    pub fn dialect() -> Arc<dyn StreamJsonDialect> {
        Arc::new(Self {
            parser: ClaudeFrameParser::new(ClaudeQuirks {
                tool_detail: map_tool_input,
                ..ClaudeQuirks::claude()
            }),
        })
    }
}

#[async_trait]
impl StreamJsonDialect for ClaudeDialect {
    fn launch_args(&self, config: &SessionConfig, resume: Option<&str>) -> Vec<String> {
        let mut args = vec![];
        if let Some(mode) = &config.mode {
            args.push("--permission-mode".to_string());
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
        if !config.mcp_servers.is_empty() {
            // Claude takes MCP servers as a JSON config flag.
            let servers: Value = config
                .mcp_servers
                .iter()
                .map(|(name, s)| {
                    (
                        name.clone(),
                        json!({"command": s.command, "args": s.args, "env": s.env}),
                    )
                })
                .collect::<serde_json::Map<String, Value>>()
                .into();
            args.push("--mcp-config".to_string());
            args.push(json!({"mcpServers": servers}).to_string());
        }
        args
    }

    async fn on_frame(&self, session: &Arc<dyn SessionCtx>, f: &Value) {
        let ty = f["type"].as_str().unwrap_or_default();
        match ty {
            "control_request" => self.on_control_request(session, f).await,
            "control_response" => {
                // Answer to a control request WE sent (interrupt, set_model).
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
            // system/assistant/user/result share the family parser —
            // the claude preset adds Task→Subagent lifecycle events on
            // top of the common block translation.
            _ => self.parser.on_frame(session, f).await,
        }
    }

    fn prompt_frame(&self, prompt: &PromptInput) -> Result<Value> {
        let content = match prompt {
            PromptInput::Text(t) => json!(t),
            PromptInput::Blocks(blocks) => json!(
                blocks
                    .iter()
                    .map(|b| match b {
                        PromptBlock::Text { text } => json!({"type": "text", "text": text}),
                        PromptBlock::Image { data, mime } => json!({
                            "type": "image",
                            "source": {"type": "base64", "media_type": mime, "data": data}
                        }),
                    })
                    .collect::<Vec<_>>()
            ),
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
                "updatedInput": updated_input.clone().unwrap_or_else(|| ask.input.clone().unwrap_or(Value::Null)),
            }),
            PermissionResponse::Deny { message, .. } => json!({
                "behavior": "deny",
                "message": message.clone().unwrap_or_else(|| "denied by user".to_string()),
            }),
        };
        json!({
            "type": "control_response",
            "response": {"request_id": wire_id, "response": resp}
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

    fn catalog(&self) -> ProviderCatalog {
        // Claude has no model-listing wire call; the init frame reports
        // the active model. The CLI's documented alias names are stable
        // across versions, so the picker offers those — full model ids
        // (claude-sonnet-4-5…) also work via set_model passthrough, they
        // just can't be enumerated without a listing wire.
        ProviderCatalog {
            models: ["sonnet", "opus", "opusplan"]
                .iter()
                .map(|m| ModelDef {
                    id: m.to_string(),
                    name: format!("{} (alias)", (*m).to_uppercase()),
                    selectable: true,
                })
                .collect(),
            modes: ["default", "acceptEdits", "plan", "bypassPermissions"]
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

    /// Claude persists every session as `~/.claude/projects/<cwd-slug>/*.jsonl`.
    async fn list_importable(&self, cwd: &Path) -> Result<Vec<ImportableSession>> {
        let home = directories::BaseDirs::new()
            .map(|b| b.home_dir().to_path_buf())
            .unwrap_or_default();
        let slug = cwd.to_string_lossy().replace('/', "-");
        let dir = home.join(".claude").join("projects").join(slug);
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
                    provider: "claude".to_string(),
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
            mcp_servers: true,
            reasoning_stream: true,
            steer: true,
            rewind: false,
            subagent_events: true,
        }
    }
}

/// The first user message of a transcript jsonl, whitespace-collapsed
/// and truncated — the closest thing to a title a Claude session file
/// carries. Reads a bounded prefix, so importing a directory of huge
/// transcripts stays cheap; returns None when no user line surfaces.
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

impl ClaudeDialect {
    /// Agent → daemon control requests. `can_use_tool` becomes a
    /// PermissionRequest; anything else gets a minimal success response
    /// so the agent isn't blocked on a handshake we don't implement.
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
                // Unknown control request — answer success so the agent
                // isn't stuck waiting on a handshake we don't implement.
                let _ = session
                    .send_raw(json!({
                        "type": "control_response",
                        "response": {"request_id": request_id, "response": {}}
                    }))
                    .await;
            }
        }
    }
}

/// Map a tool_use input to normalized detail. Unknown tools keep the raw
/// input so nothing is silently dropped.
fn map_tool_input(name: &str, input: &Value) -> ToolCallDetail {
    match name {
        "Bash" => ToolCallDetail::Shell {
            command: input["command"].as_str().unwrap_or_default().to_string(),
            output: None,
            exit_code: None,
        },
        "Read" => ToolCallDetail::Read {
            path: input["file_path"].as_str().unwrap_or_default().to_string(),
            content: None,
        },
        "Edit" | "MultiEdit" => ToolCallDetail::Edit {
            path: input["file_path"].as_str().unwrap_or_default().to_string(),
            unified_diff: None,
        },
        "Write" => ToolCallDetail::Write {
            path: input["file_path"].as_str().unwrap_or_default().to_string(),
            content: input["content"].as_str().map(|s| s.to_string()),
        },
        "Grep" | "Glob" | "WebSearch" => ToolCallDetail::Search {
            query: input["pattern"]
                .as_str()
                .or_else(|| input["query"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        "WebFetch" => ToolCallDetail::Fetch {
            url: input["url"].as_str().unwrap_or_default().to_string(),
            result: None,
        },
        "Task" => ToolCallDetail::SubAgent {
            description: input["description"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            log: String::new(),
        },
        "TodoWrite" => ToolCallDetail::Plan {
            text: input["todos"].to_string(),
        },
        _ => ToolCallDetail::Unknown {
            input: input.clone(),
            output: Value::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_match_what_claude_actually_does() {
        // Task tool_use/tool_result pairs raise Subagent events; rewind
        // is unimplemented in every backend.
        let c = ClaudeDialect::dialect().capabilities();
        assert!(c.subagent_events);
        assert!(!c.rewind);
        assert!(c.mcp_servers);
        assert!(c.steer);
    }

    #[tokio::test]
    async fn transcript_title_reads_first_user_message() {
        let dir = std::env::temp_dir().join(format!("damon-claude-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s1.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"summary\",\"summary\":\"line 1\"}\n",
                "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"fix  the\\n timeout bug\"}}\n",
                "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"ok\"}}\n",
            ),
        )
        .unwrap();
        // Whitespace collapses, not truncated at 80.
        assert_eq!(
            transcript_title(&path).await.as_deref(),
            Some("fix the timeout bug")
        );

        // Array-form content: text parts join.
        let path2 = dir.join("s2.jsonl");
        std::fs::write(
            &path2,
            "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"hello\"},{\"type\":\"text\",\"text\":\"world\"}]}}\n",
        )
        .unwrap();
        assert_eq!(
            transcript_title(&path2).await.as_deref(),
            Some("hello world")
        );

        // Long first message truncates at a char boundary (80 bytes).
        let path3 = dir.join("s3.jsonl");
        let long = "가".repeat(100);
        std::fs::write(
            &path3,
            format!(
                "{{\"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"{long}\"}}}}\n"
            ),
        )
        .unwrap();
        let t = transcript_title(&path3).await.unwrap();
        assert!(
            t.chars().count() == 26,
            "80-byte cap floors to a char boundary (78/3): {}",
            t.chars().count()
        );

        // No user line at all.
        let path4 = dir.join("s4.jsonl");
        std::fs::write(
            &path4,
            "{\"type\":\"assistant\",\"message\":{\"content\":\"hi\"}}\n",
        )
        .unwrap();
        assert_eq!(transcript_title(&path4).await, None);
        std::fs::remove_dir_all(&dir).ok();
    }
}
