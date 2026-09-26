//! Cursor Agent dialect — `cursor-agent -p --output-format stream-json`.
//!
//! One-shot: the CLI takes the prompt as a positional arg and exits per
//! turn; `--resume <chatId>` continues the chat in a fresh process.
//! Frame shape differs from Claude:
//! - `system.init` carries `session_id`, `model`, `permissionMode`
//! - `assistant` frames carry complete messages (no partial deltas
//!   unless `--stream-partial-output`, which we don't enable)
//! - `tool_call` frames: `{type:"tool_call", subtype:"started"|"completed",
//!   call_id, tool_call:{readToolCall|writeToolCall|function:{...}}}`
//! - `result` has no `usage`/`total_cost_usd`
//! - no stdin input mode, no control channel: permissions are launch-time.
//!   Per current docs, print mode already has full write access; `--force`
//!   auto-approves anything not denied by permission tokens
//!   (`permissions.allow`/`deny` in cli-config.json / cli.json).
//!
//! UNVERIFIED: no `cursor-agent` binary on the dev machine.

use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};

use super::streamjson::{SessionCtx, SessionKind, StreamJsonDialect};
use super::types::*;

pub struct CursorDialect;

impl CursorDialect {
    pub fn dialect() -> Arc<dyn StreamJsonDialect> {
        Arc::new(Self)
    }
}

#[async_trait]
impl StreamJsonDialect for CursorDialect {
    fn session_kind(&self) -> SessionKind {
        SessionKind::OneShot
    }

    fn launch_args(&self, config: &SessionConfig, resume: Option<&str>) -> Vec<String> {
        let mut args = vec![];
        if let Some(id) = resume {
            args.push("--resume".to_string());
            args.push(id.to_string());
        }
        if let Some(model) = &config.model {
            args.push("--model".to_string());
            args.push(model.clone());
        }
        // Damon modes map to cursor's --mode/--force surface. Only
        // bypassPermissions maps to --force: acceptEdits would promise
        // edit-only auto-approval, which cursor has no flag for — granting
        // --force under that name silently escalates beyond it.
        match config.mode.as_deref() {
            Some("plan") => {
                args.push("--mode".to_string());
                args.push("plan".to_string());
            }
            Some("bypassPermissions") => args.push("--force".to_string()),
            _ => {}
        }
        args
    }

    fn prompt_args(&self, prompt: &PromptInput) -> Result<Vec<String>> {
        match prompt {
            // Positional prompt arg.
            PromptInput::Text(t) => Ok(vec![t.clone()]),
            PromptInput::Blocks(_) => {
                bail!("cursor headless takes text prompts only — reference file paths instead")
            }
        }
    }

    async fn on_frame(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        match f["type"].as_str().unwrap_or_default() {
            "system" if f["subtype"].as_str() == Some("init") => {
                let id = f["session_id"].as_str().unwrap_or_default().to_string();
                ctx.set_native_handle(id).await;
            }
            "assistant" => {
                let Some(blocks) = f["message"]["content"].as_array() else {
                    return;
                };
                for block in blocks {
                    if block["type"].as_str() == Some("text") {
                        ctx.emit_timeline(TimelineItem::AssistantMessage {
                            text: block["text"].as_str().unwrap_or_default().to_string(),
                        })
                        .await;
                    }
                }
            }
            "tool_call" => self.on_tool_call(ctx, f).await,
            "result" => {
                let kind = if f["is_error"].as_bool().unwrap_or(false) {
                    StreamEventKind::TurnFailed {
                        error: f["result"].as_str().unwrap_or("unknown error").to_string(),
                        code: f["subtype"].as_str().map(|s| s.to_string()),
                    }
                } else {
                    StreamEventKind::TurnCompleted { usage: None }
                };
                ctx.finish_turn(kind).await;
            }
            _ => {}
        }
    }

    fn permission_response_frame(
        &self,
        _ask: &PermissionRequest,
        _wire_id: &str,
        _response: &PermissionResponse,
    ) -> Value {
        // No control channel — cursor permissions are launch-time flags.
        json!({})
    }

    fn catalog(&self) -> ProviderCatalog {
        ProviderCatalog {
            models: vec![],
            modes: ["default", "plan", "bypassPermissions"]
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

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            streaming: true,
            session_persistence: true,
            session_listing: false,
            dynamic_modes: true,
            mcp_servers: false,
            reasoning_stream: false,
            steer: false,
            rewind: false,
            subagent_events: false,
        }
    }
}

impl CursorDialect {
    /// `tool_call` frames: `{subtype: started|completed, call_id,
    /// tool_call: {<name>ToolCall: {args, result?}}}`.
    async fn on_tool_call(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        let call_id = f["call_id"].as_str().unwrap_or_default().to_string();
        let started = f["subtype"].as_str() == Some("started");
        let tc = &f["tool_call"];
        // The payload is a single-key object: {"readToolCall": {...}} or
        // {"function": {"name": ..., "arguments": ...}}.
        let (name, args, result) = if let Some(obj) = tc.as_object() {
            if let Some(func) = obj.get("function") {
                let name = func["name"].as_str().unwrap_or_default().to_string();
                let args = func["arguments"].clone();
                (name, args, func.get("result").cloned())
            } else if let Some((key, val)) = obj.iter().next() {
                let name = key.strip_suffix("ToolCall").unwrap_or(key).to_string();
                (name, val["args"].clone(), val.get("result").cloned())
            } else {
                (String::new(), Value::Null, None)
            }
        } else {
            (String::new(), Value::Null, None)
        };

        let item = TimelineItem::ToolCall(ToolCall {
            call_id,
            name: name.clone(),
            status: if started {
                ToolCallStatus::Running
            } else {
                ToolCallStatus::Completed
            },
            detail: map_cursor_tool(&name, &args, result.as_ref()),
        });
        ctx.emit_timeline(item).await;
    }
}

fn map_cursor_tool(name: &str, args: &Value, result: Option<&Value>) -> ToolCallDetail {
    let out = result.cloned().unwrap_or(Value::Null);
    match name {
        "read" => ToolCallDetail::Read {
            path: args["path"].as_str().unwrap_or_default().to_string(),
            content: result
                .and_then(|r| r["success"]["content"].as_str())
                .map(|s| s.to_string()),
        },
        "write" => ToolCallDetail::Write {
            path: args["path"].as_str().unwrap_or_default().to_string(),
            content: args["fileText"].as_str().map(|s| s.to_string()),
        },
        "edit" | "strReplace" | "delete" => ToolCallDetail::Edit {
            path: args["path"].as_str().unwrap_or_default().to_string(),
            unified_diff: None,
        },
        "shell" | "runTerminalCommand" | "run_terminal_cmd" => ToolCallDetail::Shell {
            command: args["command"].as_str().unwrap_or_default().to_string(),
            output: result
                .and_then(|r| r["success"]["output"].as_str())
                .map(|s| s.to_string()),
            exit_code: None,
        },
        "grep" | "glob" | "codebaseSearch" | "search" => ToolCallDetail::Search {
            query: args["pattern"]
                .as_str()
                .or_else(|| args["query"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        "fetch" | "webSearch" => ToolCallDetail::Fetch {
            url: args["url"].as_str().unwrap_or_default().to_string(),
            result: None,
        },
        "todo" | "todoWrite" => ToolCallDetail::Plan {
            text: args.to_string(),
        },
        _ => ToolCallDetail::Unknown {
            input: args.clone(),
            output: out,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::streamjson::tests::TestCtx;
    use serde_json::json;

    #[tokio::test]
    async fn init_captures_session_id() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = CursorDialect;
        d.on_frame(&ctx, &json!({"type":"system","subtype":"init","session_id":"abc-123","model":"Claude 4 Sonnet"})).await;
        assert_eq!(t.native.lock().await.as_deref(), Some("abc-123"));
    }

    #[tokio::test]
    async fn tool_call_started_maps_read() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = CursorDialect;
        d.on_frame(
            &ctx,
            &json!({
                "type":"tool_call","subtype":"started","call_id":"t1",
                "tool_call":{"readToolCall":{"args":{"path":"README.md"}}}
            }),
        )
        .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::Timeline(TimelineItem::ToolCall(t))) =
            events.first().map(|e| &e.kind)
        else {
            panic!("expected tool call event, got {events:?}");
        };
        assert_eq!(t.name, "read");
        assert!(matches!(t.detail, ToolCallDetail::Read { .. }));
        assert_eq!(t.status, ToolCallStatus::Running);
    }

    #[tokio::test]
    async fn tool_call_completed_carries_result() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = CursorDialect;
        d.on_frame(
            &ctx,
            &json!({
                "type":"tool_call","subtype":"completed","call_id":"t1",
                "tool_call":{"writeToolCall":{"args":{"path":"out.txt","fileText":"hi"},
                    "result":{"success":{"linesCreated":1,"fileSize":2}}}}
            }),
        )
        .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::Timeline(TimelineItem::ToolCall(t))) =
            events.first().map(|e| &e.kind)
        else {
            panic!("expected tool call event");
        };
        assert_eq!(t.status, ToolCallStatus::Completed);
        assert!(matches!(t.detail, ToolCallDetail::Write { .. }));
    }

    #[tokio::test]
    async fn result_finishes_turn() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = CursorDialect;
        d.on_frame(&ctx, &json!({"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s1"})).await;
        let events = t.events.lock().await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e.kind, StreamEventKind::TurnCompleted { .. }))
        );
        assert!(t.turn.lock().await.is_none());
    }

    #[test]
    fn prompt_args_text_only() {
        let d = CursorDialect;
        assert_eq!(
            d.prompt_args(&PromptInput::text("hello")).unwrap(),
            vec!["hello"]
        );
        assert!(d.prompt_args(&PromptInput::Blocks(vec![])).is_err());
    }

    #[test]
    fn launch_args_resume_and_mode() {
        let d = CursorDialect;
        let cfg = SessionConfig {
            mode: Some("plan".to_string()),
            model: Some("gpt-5".to_string()),
            ..Default::default()
        };
        let args = d.launch_args(&cfg, Some("chat-9"));
        assert!(args.contains(&"--resume".to_string()));
        assert!(args.contains(&"chat-9".to_string()));
        assert!(args.contains(&"--mode".to_string()));
        assert!(args.contains(&"plan".to_string()));
        assert!(args.contains(&"--model".to_string()));
    }

    #[test]
    fn launch_args_force_only_for_bypass() {
        let d = CursorDialect;
        let bypass = SessionConfig {
            mode: Some("bypassPermissions".to_string()),
            ..Default::default()
        };
        assert!(
            d.launch_args(&bypass, None)
                .contains(&"--force".to_string())
        );

        // acceptEdits promises edit-only auto-approval; cursor has no such
        // flag, so it must not fall through to --force.
        let edits = SessionConfig {
            mode: Some("acceptEdits".to_string()),
            ..Default::default()
        };
        assert!(!d.launch_args(&edits, None).contains(&"--force".to_string()));
    }

    #[test]
    fn catalog_advertises_real_mode_surface() {
        let modes: Vec<String> = CursorDialect
            .catalog()
            .modes
            .into_iter()
            .map(|m| m.id)
            .collect();
        assert!(modes.contains(&"bypassPermissions".to_string()));
        assert!(!modes.contains(&"acceptEdits".to_string()));
    }
}
