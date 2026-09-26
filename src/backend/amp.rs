//! Amp dialect — `amp --execute --stream-json --stream-json-input`.
//!
//! Amp's stream-json is Claude Code compatible: same `system.init`,
//! `assistant`/`user`/`result` frames, same `tool_use`/`tool_result`
//! content blocks. Differences:
//! - session ids look like `T-<uuid>`
//! - input frames accept `steer: true` for mid-turn steering
//! - `result` frames carry `usage` but no `total_cost_usd`
//! - resume is `amp threads continue <id>` (a subcommand, not a flag)
//! - `--stream-json-thinking` adds `thinking`/`redacted_thinking` blocks
//!
//! UNVERIFIED: no `amp` binary on the dev machine — permission asks and
//! the interrupt control frame are written in Claude's shape and may
//! need adjustment against a real install.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{Value, json};

use super::streamjson::{
    ClaudeFrameParser, ClaudeQuirks, ControlOp, SessionCtx, StreamJsonDialect,
};
use super::types::*;

pub struct AmpDialect {
    /// Shared Claude-frame translator; amp's preset keeps the vendor
    /// tool mapping and its usage/error-text quirks.
    parser: ClaudeFrameParser,
}

impl AmpDialect {
    pub fn dialect() -> Arc<dyn StreamJsonDialect> {
        Arc::new(Self {
            parser: ClaudeFrameParser::new(ClaudeQuirks {
                tool_detail: map_tool_input,
                ..ClaudeQuirks::amp()
            }),
        })
    }
}

/// Base args for an execute-mode stream-json session.
fn exec_args() -> Vec<String> {
    [
        "--execute",
        "--stream-json",
        "--stream-json-input",
        "--stream-json-thinking",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

#[async_trait]
impl StreamJsonDialect for AmpDialect {
    fn launch_args(&self, config: &SessionConfig, resume: Option<&str>) -> Vec<String> {
        let mut args = vec![];
        if let Some(id) = resume {
            // `amp threads continue <id>` reattaches to a thread.
            args.extend(["threads", "continue"].iter().map(|s| s.to_string()));
            args.push(id.to_string());
        }
        args.extend(exec_args());
        if let Some(model) = &config.model {
            args.push("--model".to_string());
            args.push(model.clone());
        }
        args
    }

    async fn on_frame(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        let ty = f["type"].as_str().unwrap_or_default();
        // Amp-only: system error subtypes end the turn as failed.
        // Everything else Claude-shaped goes through the shared parser.
        if ty == "system"
            && matches!(
                f["subtype"].as_str(),
                Some("error_max_turns") | Some("error_during_execution")
            )
        {
            ctx.finish_turn(StreamEventKind::TurnFailed {
                error: f["error"].as_str().unwrap_or("amp error").to_string(),
                code: f["subtype"].as_str().map(|s| s.to_string()),
            })
            .await;
            return;
        }
        match ty {
            // UNVERIFIED: amp may emit Claude-shaped control_request
            // frames for permission asks; handle them the same way.
            "control_request" => self.on_control_request(ctx, f).await,
            "control_response" => {
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
                ctx.resolve_wire(&id, result).await;
            }
            _ => self.parser.on_frame(ctx, f).await,
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

    fn steer_frame(&self, prompt: &PromptInput) -> Result<Value> {
        // amp's stream-json input marks mid-turn messages with
        // `steer: true` so they queue as steering, not a new prompt.
        let mut frame = self.prompt_frame(prompt)?;
        frame["steer"] = json!(true);
        Ok(frame)
    }

    fn permission_response_frame(
        &self,
        ask: &PermissionRequest,
        wire_id: &str,
        response: &PermissionResponse,
    ) -> Value {
        // Claude-shaped control_response; UNVERIFIED against real amp.
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
        // UNVERIFIED: Claude-shaped control requests. Fire-and-forget
        // (below) so a dialect that ignores them can't wedge the caller.
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

    fn control_is_fire_and_forget(&self) -> bool {
        true
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            streaming: true,
            session_persistence: true,
            session_listing: false,
            dynamic_modes: false,
            mcp_servers: false,
            reasoning_stream: true,
            steer: true,
            rewind: false,
            subagent_events: false,
        }
    }
}

impl AmpDialect {
    /// UNVERIFIED: assumes Claude's can_use_tool control_request shape.
    async fn on_control_request(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
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
                ctx.register_ask(ask, request_id).await;
            }
            _ => {
                let _ = ctx
                    .send_raw(json!({
                        "type": "control_response",
                        "response": {"request_id": request_id, "response": {}}
                    }))
                    .await;
            }
        }
    }
}

/// Amp tool names differ from Claude's (`create_file`, `edit_file`,
/// `finder`, `oracle`, `todo_write`, …) — map what we know, keep the
/// raw input for the rest.
fn map_tool_input(name: &str, input: &Value) -> ToolCallDetail {
    match name {
        "Bash" => ToolCallDetail::Shell {
            command: input["command"].as_str().unwrap_or_default().to_string(),
            output: None,
            exit_code: None,
        },
        "Read" | "read_mcp_resource" => ToolCallDetail::Read {
            path: input["path"]
                .as_str()
                .or_else(|| input["file_path"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        "edit_file" | "Edit" => ToolCallDetail::Edit {
            path: input["path"]
                .as_str()
                .or_else(|| input["file_path"].as_str())
                .unwrap_or_default()
                .to_string(),
            unified_diff: None,
        },
        "create_file" | "Write" => ToolCallDetail::Write {
            path: input["path"]
                .as_str()
                .or_else(|| input["file_path"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: input["content"]
                .as_str()
                .or_else(|| input["fileText"].as_str())
                .map(|s| s.to_string()),
        },
        "Grep" | "glob" | "finder" | "web_search" => ToolCallDetail::Search {
            query: input["pattern"]
                .as_str()
                .or_else(|| input["query"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        "read_web_page" | "WebFetch" => ToolCallDetail::Fetch {
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
        "todo_write" | "TodoWrite" => ToolCallDetail::Plan {
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
    use crate::backend::streamjson::tests::TestCtx;
    use serde_json::json;

    #[tokio::test]
    async fn permission_ask_registers_and_replies() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = AmpDialect::dialect();
        d.on_frame(
            &ctx,
            &json!({
                "type":"control_request","request_id":"r1",
                "request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"ls"}}
            }),
        )
        .await;
        assert_eq!(t.asks.lock().await.len(), 1);
        // The response frame shape is Claude-compatible.
        let frame = d.permission_response_frame(
            &t.asks.lock().await[0],
            "r1",
            &PermissionResponse::Allow {
                action_id: None,
                updated_input: None,
                answer: None,
            },
        );
        assert_eq!(frame["type"], "control_response");
    }

    #[test]
    fn steer_frame_is_marked() {
        let d = AmpDialect::dialect();
        let f = d.steer_frame(&PromptInput::Text("go left".into())).unwrap();
        assert_eq!(f["steer"], json!(true));
        assert_eq!(f["message"]["content"], json!("go left"));
        // Prompt frames stay unmarked — only steering is.
        let p = d
            .prompt_frame(&PromptInput::Text("go left".into()))
            .unwrap();
        assert!(p.get("steer").is_none());
    }

    #[test]
    fn capabilities_match_what_amp_actually_does() {
        // No Subagent events are emitted by this dialect; rewind is
        // unimplemented in every backend.
        let c = AmpDialect::dialect().capabilities();
        assert!(!c.subagent_events);
        assert!(!c.rewind);
        assert!(!c.mcp_servers);
        assert!(c.steer);
    }

    #[test]
    fn launch_args_resume_uses_threads_continue() {
        let d = AmpDialect::dialect();
        let args = d.launch_args(&SessionConfig::default(), Some("T-9"));
        assert_eq!(&args[..3], &["threads", "continue", "T-9"]);
        assert!(args.contains(&"--stream-json-input".to_string()));
    }
}
