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

use super::streamjson::{
    ClaudeFrameParser, ClaudeQuirks, SessionCtx, SessionKind, StreamJsonDialect,
};
use super::types::*;

pub struct QwenDialect {
    /// Shared Claude-frame translator; qwen's preset reports optional
    /// usage with the frame's total_cost_usd.
    parser: ClaudeFrameParser,
}

impl QwenDialect {
    pub fn dialect() -> Arc<dyn StreamJsonDialect> {
        Arc::new(Self {
            parser: ClaudeFrameParser::new(ClaudeQuirks::qwen()),
        })
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
        self.parser.on_frame(ctx, f).await;
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
        let d = QwenDialect::dialect();
        d.on_frame(
            &ctx,
            &json!({"type":"system","subtype":"session_start","session_id":"q-7"}),
        )
        .await;
        assert_eq!(t.native.lock().await.as_deref(), Some("q-7"));
    }

    #[test]
    fn launch_args_map_modes() {
        let d = QwenDialect::dialect();
        let cfg = SessionConfig {
            mode: Some("bypassPermissions".to_string()),
            ..Default::default()
        };
        let args = d.launch_args(&cfg, None);
        assert!(args.contains(&"--approval-mode".to_string()));
        assert!(args.contains(&"yolo".to_string()));
    }
}
