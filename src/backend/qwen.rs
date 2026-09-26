//! Qwen Code dialect — `qwen -p --output-format stream-json`.
//!
//! One-shot: `-p` runs a single prompt and exits; `--resume <id>`
//! continues a project-scoped session in a fresh process. Qwen's
//! stream-json output mimics Claude's frame shape (`system` with
//! `session_start` subtype, `assistant`, `result`) since both derive
//! from the same headless format.
//!
//! `--input-format stream-json` exists but is marked "under
//! construction" upstream — we use the one-shot path instead.
//!
//! UNVERIFIED: no `qwen` binary on the dev machine. Approval behavior in
//! headless mode is unclear — `--approval-mode` exists (default/yolo/
//! auto_edit/plan); we pass it through from the session mode.

use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};

use super::streamjson::{SessionCtx, SessionKind, StreamJsonDialect};
use super::types::*;

pub struct QwenDialect;

impl QwenDialect {
    pub fn dialect() -> Arc<dyn StreamJsonDialect> {
        Arc::new(Self)
    }
}

#[async_trait]
impl StreamJsonDialect for QwenDialect {
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
        // Damon modes → qwen --approval-mode.
        match config.mode.as_deref() {
            Some("plan") => args.extend(["--approval-mode", "plan"].map(|s| s.to_string())),
            Some("acceptEdits") => {
                args.extend(["--approval-mode", "auto_edit"].map(|s| s.to_string()))
            }
            Some("bypassPermissions") => {
                args.extend(["--approval-mode", "yolo"].map(|s| s.to_string()))
            }
            _ => {}
        }
        args
    }

    fn prompt_args(&self, prompt: &PromptInput) -> Result<Vec<String>> {
        match prompt {
            PromptInput::Text(t) => Ok(vec![t.clone()]),
            PromptInput::Blocks(_) => {
                bail!("qwen headless takes text prompts only")
            }
        }
    }

    async fn on_frame(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        if let Some(id) = f["session_id"].as_str() {
            ctx.set_native_handle(id.to_string()).await;
        }
        match f["type"].as_str().unwrap_or_default() {
            "assistant" => {
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
                            detail: ToolCallDetail::Unknown {
                                input: block["input"].clone(),
                                output: Value::Null,
                            },
                        }),
                        _ => TimelineItem::Unknown { raw: block.clone() },
                    };
                    ctx.emit_timeline(item).await;
                }
            }
            "user" => {
                // tool_result blocks arrive as user messages.
                let Some(blocks) = f["message"]["content"].as_array() else {
                    return;
                };
                for block in blocks {
                    if block["type"].as_str() != Some("tool_result") {
                        continue;
                    }
                    let is_error = block["is_error"].as_bool().unwrap_or(false);
                    let output = match &block["content"] {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    ctx.emit_timeline(TimelineItem::ToolCall(ToolCall {
                        call_id: block["tool_use_id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
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
            "result" => {
                let usage = if f["usage"].is_null() {
                    None
                } else {
                    Some(Usage {
                        input_tokens: f["usage"]["input_tokens"].as_u64(),
                        cached_input_tokens: f["usage"]["cache_read_input_tokens"].as_u64(),
                        output_tokens: f["usage"]["output_tokens"].as_u64(),
                        cost_usd: f["total_cost_usd"].as_f64(),
                        context_window: None,
                        context_used: None,
                    })
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
                    StreamEventKind::TurnCompleted { usage }
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
        // No verified permission wire in headless mode.
        json!({})
    }

    fn catalog(&self) -> ProviderCatalog {
        ProviderCatalog {
            models: vec![],
            modes: ["default", "plan", "acceptEdits", "bypassPermissions"]
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
            reasoning_stream: true,
            steer: false,
            rewind: false,
            subagent_events: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::streamjson::tests::TestCtx;
    use serde_json::json;

    #[tokio::test]
    async fn session_start_captures_id() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = QwenDialect;
        d.on_frame(
            &ctx,
            &json!({"type":"system","subtype":"session_start","session_id":"q-7"}),
        )
        .await;
        assert_eq!(t.native.lock().await.as_deref(), Some("q-7"));
    }

    #[tokio::test]
    async fn tool_result_via_user_frame() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = QwenDialect;
        d.on_frame(
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

    #[test]
    fn launch_args_map_modes() {
        let d = QwenDialect;
        let cfg = SessionConfig {
            mode: Some("bypassPermissions".to_string()),
            ..Default::default()
        };
        let args = d.launch_args(&cfg, None);
        assert!(args.contains(&"--approval-mode".to_string()));
        assert!(args.contains(&"yolo".to_string()));
    }
}
