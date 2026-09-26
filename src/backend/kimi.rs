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

use super::streamjson::{
    ClaudeFrameParser, ClaudeQuirks, SessionCtx, SessionKind, StreamJsonDialect,
};
use super::types::*;

pub struct KimiDialect {
    /// Shared Claude-frame translator; kimi's preset swaps tool-result
    /// framing to OpenAI-style `tool` frames and hybrid tool_calls.
    parser: ClaudeFrameParser,
}

impl KimiDialect {
    pub fn dialect() -> Arc<dyn StreamJsonDialect> {
        Arc::new(Self {
            parser: ClaudeFrameParser::new(ClaudeQuirks::kimi()),
        })
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
        self.parser.on_frame(ctx, f).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::streamjson::tests::TestCtx;
    use serde_json::json;

    #[tokio::test]
    async fn session_id_captured_from_any_frame() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = KimiDialect::dialect();
        d.on_frame(&ctx, &json!({"type":"assistant","session_id":"k-42","message":{"content":[{"type":"text","text":"hi"}]}})).await;
        assert_eq!(t.native.lock().await.as_deref(), Some("k-42"));
    }

    #[test]
    fn launch_args_resume_uses_session_flag() {
        let d = KimiDialect::dialect();
        let args = d.launch_args(&SessionConfig::default(), Some("k-9"));
        assert_eq!(args, vec!["--session", "k-9"]);
    }
}
