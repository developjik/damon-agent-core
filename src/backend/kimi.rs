//! Kimi Code dialect — `kimi -p --output-format stream-json`.
//!
//! One-shot: `-p` runs a single prompt and exits; `--session <id>`
//! (alias `-r`/`--resume`) continues a chat in a fresh process.
//! `-p` mode auto-approves regular tool calls ("auto" permission
//! policy) — there is no interactive permission relay in headless mode.
//!
//! Frame shape (from kimi-cli docs + stream-json example):
//! - assistant messages with `tool_calls` (OpenAI-style), followed by
//!   tool messages; thinking is NOT written to JSONL
//! - frames carry `session_id` — the resume token
//!
//! UNVERIFIED: no `kimi` binary on the dev machine — the exact frame
//! schema is inferred from docs and may need adjustment.

use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};

use super::streamjson::{SessionCtx, SessionKind, StreamJsonDialect};
use super::types::*;

pub struct KimiDialect;

impl KimiDialect {
    pub fn dialect() -> Arc<dyn StreamJsonDialect> {
        Arc::new(Self)
    }
}

#[async_trait]
impl StreamJsonDialect for KimiDialect {
    fn session_kind(&self) -> SessionKind {
        SessionKind::OneShot
    }

    fn launch_args(&self, config: &SessionConfig, resume: Option<&str>) -> Vec<String> {
        let mut args = vec![];
        if let Some(id) = resume {
            args.push("--session".to_string());
            args.push(id.to_string());
        }
        if let Some(model) = &config.model {
            args.push("--model".to_string());
            args.push(model.clone());
        }
        // kimi -p rejects --yolo/--auto/--plan (flag conflict rules) —
        // headless runs always use the auto permission policy.
        args
    }

    fn prompt_args(&self, prompt: &PromptInput) -> Result<Vec<String>> {
        match prompt {
            PromptInput::Text(t) => Ok(vec![t.clone()]),
            PromptInput::Blocks(_) => {
                bail!("kimi headless takes text prompts only")
            }
        }
    }

    async fn on_frame(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        // Capture the session id from any frame that carries one.
        if let Some(id) = f["session_id"].as_str() {
            ctx.set_native_handle(id.to_string()).await;
        }
        match f["type"].as_str().unwrap_or_default() {
            "assistant" => self.on_assistant(ctx, f).await,
            "tool" => self.on_tool(ctx, f).await,
            "result" => {
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
                    StreamEventKind::TurnCompleted {
                        usage: parse_usage(&f["usage"]),
                    }
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
        // Headless kimi auto-approves — no permission wire.
        json!({})
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            streaming: true,
            session_persistence: true,
            session_listing: false,
            dynamic_modes: false,
            mcp_servers: false,
            reasoning_stream: false,
            steer: false,
            rewind: false,
            subagent_events: false,
        }
    }
}

impl KimiDialect {
    /// Assistant frames may carry `content` blocks (Claude-shaped) or
    /// OpenAI-style `tool_calls` — handle both.
    async fn on_assistant(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        let msg = &f["message"];
        if let Some(blocks) = msg["content"].as_array() {
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
        // OpenAI-style tool_calls array.
        if let Some(calls) = msg["tool_calls"].as_array() {
            for call in calls {
                let name = call["function"]["name"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                let input = call["function"]["arguments"].clone();
                ctx.emit_timeline(TimelineItem::ToolCall(ToolCall {
                    call_id: call["id"].as_str().unwrap_or_default().to_string(),
                    name,
                    status: ToolCallStatus::Running,
                    detail: ToolCallDetail::Unknown {
                        input,
                        output: Value::Null,
                    },
                }))
                .await;
            }
        }
    }

    /// Tool result frames (OpenAI-style `tool` role messages).
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
}

fn parse_usage(u: &Value) -> Option<Usage> {
    if u.is_null() {
        return None;
    }
    Some(Usage {
        input_tokens: u["input_tokens"].as_u64(),
        cached_input_tokens: u["cache_read_input_tokens"].as_u64(),
        output_tokens: u["output_tokens"].as_u64(),
        cost_usd: None,
        context_window: None,
        context_used: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::streamjson::tests::TestCtx;
    use serde_json::json;

    #[tokio::test]
    async fn session_id_captured_from_any_frame() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = KimiDialect;
        d.on_frame(&ctx, &json!({"type":"assistant","session_id":"k-42","message":{"content":[{"type":"text","text":"hi"}]}})).await;
        assert_eq!(t.native.lock().await.as_deref(), Some("k-42"));
    }

    #[tokio::test]
    async fn openai_style_tool_calls() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = KimiDialect;
        d.on_frame(
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
    async fn tool_result_frame_completes_call() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = KimiDialect;
        d.on_frame(
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

    #[test]
    fn launch_args_resume_uses_session_flag() {
        let d = KimiDialect;
        let args = d.launch_args(&SessionConfig::default(), Some("k-9"));
        assert_eq!(args, vec!["--session", "k-9"]);
    }
}
