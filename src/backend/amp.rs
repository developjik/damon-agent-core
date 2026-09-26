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

use super::streamjson::{ControlOp, SessionCtx, StreamJsonDialect};
use super::types::*;

pub struct AmpDialect;

impl AmpDialect {
    pub fn dialect() -> Arc<dyn StreamJsonDialect> {
        Arc::new(Self)
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
        match ty {
            "system" => {
                match f["subtype"].as_str().unwrap_or_default() {
                    "init" => {
                        let id = f["session_id"].as_str().unwrap_or_default().to_string();
                        ctx.set_native_handle(id).await;
                    }
                    // system error subtypes end the turn as failed.
                    "error_max_turns" | "error_during_execution" => {
                        ctx.finish_turn(StreamEventKind::TurnFailed {
                            error: f["error"].as_str().unwrap_or("amp error").to_string(),
                            code: f["subtype"].as_str().map(|s| s.to_string()),
                        })
                        .await;
                    }
                    _ => {}
                }
            }
            "assistant" => self.on_assistant(ctx, f).await,
            "user" => self.on_user(ctx, f).await,
            "result" => self.on_result(ctx, f).await,
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
            _ => {}
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
            subagent_events: true,
        }
    }
}

impl AmpDialect {
    async fn on_assistant(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        let Some(blocks) = f["message"]["content"].as_array() else {
            return;
        };
        for block in blocks {
            let item = match block["type"].as_str().unwrap_or_default() {
                "text" => TimelineItem::AssistantMessage {
                    text: block["text"].as_str().unwrap_or_default().to_string(),
                },
                "thinking" => TimelineItem::Reasoning {
                    text: block["thinking"].as_str().unwrap_or_default().to_string(),
                },
                "tool_use" => TimelineItem::ToolCall(ToolCall {
                    call_id: block["id"].as_str().unwrap_or_default().to_string(),
                    name: block["name"].as_str().unwrap_or_default().to_string(),
                    status: ToolCallStatus::Running,
                    detail: map_tool_input(
                        block["name"].as_str().unwrap_or_default(),
                        &block["input"],
                    ),
                }),
                _ => TimelineItem::Unknown { raw: block.clone() },
            };
            ctx.emit_timeline(item).await;
        }
    }

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
                call_id,
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
        }
    }

    async fn on_result(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        let usage = Usage {
            input_tokens: f["usage"]["input_tokens"].as_u64(),
            cached_input_tokens: f["usage"]["cache_read_input_tokens"].as_u64(),
            output_tokens: f["usage"]["output_tokens"].as_u64(),
            cost_usd: None,
            context_window: f["usage"]["max_tokens"].as_u64(),
            context_used: None,
        };
        let kind = if f["is_error"].as_bool().unwrap_or(false) {
            StreamEventKind::TurnFailed {
                error: f["error"]
                    .as_str()
                    .or_else(|| f["result"].as_str())
                    .unwrap_or("unknown error")
                    .to_string(),
                code: f["subtype"].as_str().map(|s| s.to_string()),
            }
        } else {
            StreamEventKind::TurnCompleted { usage: Some(usage) }
        };
        ctx.finish_turn(kind).await;
    }

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
    async fn init_captures_thread_id() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = AmpDialect;
        d.on_frame(
            &ctx,
            &json!({"type":"system","subtype":"init","session_id":"T-abc-123"}),
        )
        .await;
        assert_eq!(t.native.lock().await.as_deref(), Some("T-abc-123"));
    }

    #[tokio::test]
    async fn assistant_text_and_tool_use() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = AmpDialect;
        d.on_frame(
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
    async fn result_finishes_with_usage() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = AmpDialect;
        d.on_frame(
            &ctx,
            &json!({
                "type":"result","subtype":"success","is_error":false,
                "usage":{"input_tokens":10,"output_tokens":5}
            }),
        )
        .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::TurnCompleted { usage }) = events.last().map(|e| &e.kind) else {
            panic!("expected TurnCompleted");
        };
        assert_eq!(usage.as_ref().unwrap().input_tokens, Some(10));
    }

    #[tokio::test]
    async fn permission_ask_registers_and_replies() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = AmpDialect;
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
            },
        );
        assert_eq!(frame["type"], "control_response");
    }

    #[test]
    fn launch_args_resume_uses_threads_continue() {
        let d = AmpDialect;
        let args = d.launch_args(&SessionConfig::default(), Some("T-9"));
        assert_eq!(&args[..3], &["threads", "continue", "T-9"]);
        assert!(args.contains(&"--stream-json-input".to_string()));
    }
}
