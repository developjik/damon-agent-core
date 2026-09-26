//! Gemini CLI dialect — `gemini --prompt <text> --output-format stream-json`.
//!
//! One-shot: headless mode runs a single prompt per process and exits;
//! `--resume <session-id>` continues that session in a fresh process
//! (gemini -r "abc123" "query"), so every Damon turn respawns. The
//! `session_id` from the init event is the persistence handle.
//!
//! Wire: newline-delimited JSONL events, shapes verbatim from
//! google-gemini/gemini-cli `packages/core/src/output/types.ts` and the
//! emission sites in `packages/cli/src/nonInteractiveCli.ts`:
//! - `init` — session id + model
//! - `message` — assistant text arrives ONLY as `delta: true` chunks;
//!   there is no assembled final-message event
//! - `tool_use` / `tool_result` — callId-keyed tool lifecycle
//! - `error` — explicitly non-fatal; `result` stays authoritative
//! - `result` — terminal frame, status success|error, carries stats
//!
//! No permission relay exists headless: stream-json is output-only
//! (there is no `--input-format`), so Damon cannot answer approvals.
//! We therefore default `--approval-mode yolo` — the documented,
//! non-deprecated form of `-y` — since a daemon-driven session that
//! cannot execute tools is useless. `plan` and `acceptEdits` map to
//! their native approval-mode siblings like every other dialect.
//!
//! The CLI reads piped stdin as prompt context but gives up after 500ms
//! of silence (`readStdin.ts`), so the transport's open, never-written
//! stdin costs a fixed startup delay per turn and cannot hang a run.
//!
//! Exit codes 42 (input error) and 53 (turn limit) arrive alongside a
//! `result` status:error frame, which carries the typed error already.
//!
//! UNVERIFIED: no `gemini` binary on the dev machine — frame shapes
//! are taken from the sources above; behavior may need adjustment on a
//! machine with the CLI installed.

use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};

use super::streamjson::{SessionCtx, SessionKind, StreamJsonDialect};
use super::types::*;

pub struct GeminiDialect;

impl GeminiDialect {
    pub fn dialect() -> Arc<dyn StreamJsonDialect> {
        Arc::new(Self)
    }
}

#[async_trait]
impl StreamJsonDialect for GeminiDialect {
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
        // Damon modes → gemini --approval-mode (default/auto_edit/yolo/
        // plan). Unset and "default" fall through to yolo: headless runs
        // have no permission wire to answer, and without auto-approval a
        // daemon session can never execute a tool.
        let mode = match config.mode.as_deref() {
            Some("plan") => "plan",
            Some("acceptEdits") => "auto_edit",
            _ => "yolo",
        };
        args.extend(["--approval-mode", mode].map(|s| s.to_string()));
        args
    }

    fn prompt_args(&self, prompt: &PromptInput) -> Result<Vec<String>> {
        match prompt {
            // --prompt (not a bare positional) is the documented headless
            // trigger and forces non-interactive mode even under a TTY.
            PromptInput::Text(t) => Ok(vec!["--prompt".to_string(), t.clone()]),
            PromptInput::Blocks(_) => {
                bail!("gemini headless takes text prompts only")
            }
        }
    }

    async fn on_frame(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        // Only init carries the session id, but accepting it from any
        // frame is free and matches the other one-shot dialects.
        if let Some(id) = f["session_id"].as_str() {
            ctx.set_native_handle(id.to_string()).await;
        }
        match f["type"].as_str().unwrap_or_default() {
            "message" => self.on_message(ctx, f).await,
            "tool_use" => {
                ctx.emit_timeline(TimelineItem::ToolCall(ToolCall {
                    call_id: f["tool_id"].as_str().unwrap_or_default().to_string(),
                    name: f["tool_name"].as_str().unwrap_or_default().to_string(),
                    status: ToolCallStatus::Running,
                    detail: ToolCallDetail::Unknown {
                        input: f["parameters"].clone(),
                        output: Value::Null,
                    },
                }))
                .await;
            }
            "tool_result" => {
                // Structured error message beats the plain output field.
                let output = f["error"]["message"]
                    .as_str()
                    .or_else(|| f["output"].as_str())
                    .unwrap_or_default()
                    .to_string();
                ctx.emit_timeline(TimelineItem::ToolCall(ToolCall {
                    call_id: f["tool_id"].as_str().unwrap_or_default().to_string(),
                    name: String::new(),
                    status: if f["status"].as_str() == Some("error") {
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
            "error" if f["severity"].as_str() == Some("error") => {
                // The error channel is non-fatal by spec — the result
                // frame owns the turn outcome, so this must never finish
                // the turn. Warnings (loop detection, blocked-agent
                // notices) are advisory and dropped; real errors are
                // preserved on the timeline so nothing silently vanishes
                // from the transcript.
                ctx.emit_timeline(TimelineItem::Unknown { raw: f.clone() })
                    .await;
            }
            "result" => {
                let kind = if f["status"].as_str() == Some("error") {
                    StreamEventKind::TurnFailed {
                        error: f["error"]["message"]
                            .as_str()
                            .unwrap_or("unknown error")
                            .to_string(),
                        code: f["error"]["type"].as_str().map(|s| s.to_string()),
                    }
                } else {
                    StreamEventKind::TurnCompleted {
                        usage: parse_stats(&f["stats"]),
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
        // No permission wire headless — approval is settled by
        // --approval-mode before the process starts.
        json!({})
    }

    fn catalog(&self) -> ProviderCatalog {
        // Mode descriptions tell the truth the id alone can't: gemini
        // headless has no permission wire, so BOTH "default" and
        // "bypassPermissions" run with auto-approval (--approval-mode
        // yolo) — a picker that silently promises "ask me first" would
        // be a lie. Only plan/auto_edit change approval behavior.
        let describe = |m: &str| {
            Some(match m {
                "default" => "auto-approves everything — headless gemini has no permission wire",
                "bypassPermissions" => "auto-approves everything (gemini yolo)",
                "acceptEdits" => "auto-approves edits (gemini auto_edit)",
                "plan" => "planning only (gemini plan)",
                _ => "",
            })
            .filter(|s| !s.is_empty())
        };
        ProviderCatalog {
            models: vec![],
            modes: ["default", "plan", "acceptEdits", "bypassPermissions"]
                .iter()
                .map(|m| ModeDef {
                    id: m.to_string(),
                    name: m.to_string(),
                    description: describe(m).map(String::from),
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
            // The stream-json schema has no reasoning/thinking event —
            // model thought never crosses the wire.
            reasoning_stream: false,
            steer: false,
            rewind: false,
            subagent_events: false,
        }
    }
}

impl GeminiDialect {
    async fn on_message(&self, ctx: &Arc<dyn SessionCtx>, f: &Value) {
        // The CLI echoes our own --prompt value as a role:user message;
        // the daemon already recorded that text when the turn was queued.
        if f["role"].as_str() != Some("assistant") {
            return;
        }
        let Some(text) = f["content"].as_str() else {
            return;
        };
        if text.is_empty() {
            return;
        }
        // Assistant text only ever arrives as delta:true fragments —
        // nonInteractiveCli.ts emits no assembled final message. Each
        // fragment becomes its own AssistantMessage; the transcript
        // writer and the channel bridge concatenate consecutive items,
        // the same convention codex and omp follow for their deltas.
        ctx.emit_timeline(TimelineItem::AssistantMessage {
            text: text.to_string(),
        })
        .await;
    }
}

/// Map the result frame's StreamStats (stream-json-formatter.ts
/// convertToStreamStats) onto the normalized Usage. Gemini reports
/// token counts only — no cost and no context-window numbers — and
/// `input` (the non-cached input breakdown) has no normalized slot.
fn parse_stats(s: &Value) -> Option<Usage> {
    if s.is_null() {
        return None;
    }
    Some(Usage {
        input_tokens: s["input_tokens"].as_u64(),
        cached_input_tokens: s["cached"].as_u64(),
        output_tokens: s["output_tokens"].as_u64(),
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

    // Fixtures below are copied verbatim from the shapes the CLI emits
    // (stream-json-formatter.test.ts, nonInteractiveCli.ts, errors.ts).

    #[tokio::test]
    async fn init_frame_captures_session_handle() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        GeminiDialect
            .on_frame(
                &ctx,
                &json!({
                    "type":"init",
                    "timestamp":"2025-10-10T12:00:00.000Z",
                    "session_id":"test-session-123",
                    "model":"gemini-2.0-flash-exp"
                }),
            )
            .await;
        assert_eq!(t.native.lock().await.as_deref(), Some("test-session-123"));
    }

    #[tokio::test]
    async fn assistant_deltas_each_become_a_message() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = GeminiDialect;
        // Emission shape from nonInteractiveCli.ts Content events: role
        // assistant, delta true, one chunk per streamed fragment.
        for chunk in ["The answer", " is ", "42."] {
            d.on_frame(
                &ctx,
                &json!({
                    "type":"message",
                    "timestamp":"2025-10-10T12:00:00.000Z",
                    "role":"assistant",
                    "content":chunk,
                    "delta":true
                }),
            )
            .await;
        }
        let events = t.events.lock().await;
        let joined: String = events
            .iter()
            .filter_map(|e| match &e.kind {
                StreamEventKind::Timeline(TimelineItem::AssistantMessage { text }) => {
                    Some(text.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(joined, "The answer is 42.");
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn user_echo_is_not_duplicated() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        GeminiDialect
            .on_frame(
                &ctx,
                &json!({
                    "type":"message",
                    "timestamp":"2025-10-10T12:00:00.000Z",
                    "role":"user",
                    "content":"What is 2+2?"
                }),
            )
            .await;
        assert!(t.events.lock().await.is_empty());
    }

    #[tokio::test]
    async fn tool_use_and_result_map_the_call_lifecycle() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = GeminiDialect;
        d.on_frame(
            &ctx,
            &json!({
                "type":"tool_use",
                "timestamp":"2025-10-10T12:00:00.000Z",
                "tool_name":"Read",
                "tool_id":"read-123",
                "parameters":{"file_path":"src/main.rs"}
            }),
        )
        .await;
        d.on_frame(
            &ctx,
            &json!({
                "type":"tool_result",
                "timestamp":"2025-10-10T12:00:00.000Z",
                "tool_id":"read-123",
                "status":"success",
                "output":"fn main() {}"
            }),
        )
        .await;
        let events = t.events.lock().await;
        let (running, done) = match (
            events.first().map(|e| &e.kind),
            events.get(1).map(|e| &e.kind),
        ) {
            (
                Some(StreamEventKind::Timeline(TimelineItem::ToolCall(a))),
                Some(StreamEventKind::Timeline(TimelineItem::ToolCall(b))),
            ) => (a, b),
            other => panic!("expected two tool calls, got {other:?}"),
        };
        assert_eq!(running.name, "Read");
        assert_eq!(running.call_id, "read-123");
        assert_eq!(running.status, ToolCallStatus::Running);
        assert_eq!(running.detail_input()["file_path"], "src/main.rs");
        assert_eq!(done.call_id, "read-123");
        assert_eq!(done.status, ToolCallStatus::Completed);
        assert_eq!(done.detail_output(), "fn main() {}");
    }

    #[tokio::test]
    async fn tool_result_error_marks_call_failed_with_message() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        // Emission shape from nonInteractiveCli.ts: error status carries
        // {type, message}; the message must win over a stale output.
        GeminiDialect
            .on_frame(
                &ctx,
                &json!({
                    "type":"tool_result",
                    "timestamp":"2025-10-10T12:00:00.000Z",
                    "tool_id":"shell-1",
                    "status":"error",
                    "error":{
                        "type":"TOOL_EXECUTION_ERROR",
                        "message":"command not found: makr"
                    }
                }),
            )
            .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::Timeline(TimelineItem::ToolCall(tc))) =
            events.first().map(|e| &e.kind)
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.status, ToolCallStatus::Failed);
        assert_eq!(tc.detail_output(), "command not found: makr");
    }

    #[tokio::test]
    async fn error_events_never_finish_the_turn() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        let d = GeminiDialect;
        // Warnings (e.g. loop detection) are advisory — dropped.
        d.on_frame(
            &ctx,
            &json!({
                "type":"error",
                "timestamp":"2025-10-10T12:00:00.000Z",
                "severity":"warning",
                "message":"Loop detected, stopping execution"
            }),
        )
        .await;
        assert!(t.events.lock().await.is_empty());
        // Real errors are preserved on the timeline, not turned into a
        // failure — result owns the outcome.
        d.on_frame(
            &ctx,
            &json!({
                "type":"error",
                "timestamp":"2025-10-10T12:00:00.000Z",
                "severity":"error",
                "message":"Model stream error"
            }),
        )
        .await;
        let events = t.events.lock().await;
        assert!(matches!(
            events.first().map(|e| &e.kind),
            Some(StreamEventKind::Timeline(TimelineItem::Unknown { .. }))
        ));
        // Turn must still be live (finish_turn would have cleared it).
        assert_eq!(t.turn.lock().await.as_deref(), Some("t1"));
    }

    #[tokio::test]
    async fn result_success_completes_turn_with_usage() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        GeminiDialect
            .on_frame(
                &ctx,
                &json!({
                    "type":"result",
                    "timestamp":"2025-10-10T12:00:00.000Z",
                    "status":"success",
                    "stats":{
                        "total_tokens":100,
                        "input_tokens":50,
                        "output_tokens":50,
                        "cached":0,
                        "input":50,
                        "duration_ms":1200,
                        "tool_calls":2,
                        "models":{}
                    }
                }),
            )
            .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::TurnCompleted { usage }) = events.first().map(|e| &e.kind) else {
            panic!("expected TurnCompleted, got {:?}", events.first());
        };
        let usage = usage.as_ref().expect("usage present");
        assert_eq!(usage.input_tokens, Some(50));
        assert_eq!(usage.output_tokens, Some(50));
        assert_eq!(usage.cached_input_tokens, Some(0));
        assert_eq!(usage.cost_usd, None);
    }

    #[tokio::test]
    async fn result_error_fails_turn_with_typed_code() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        // Shape from utils/errors.ts: {type, message} on status error.
        GeminiDialect
            .on_frame(
                &ctx,
                &json!({
                    "type":"result",
                    "timestamp":"2025-10-10T12:00:00.000Z",
                    "status":"error",
                    "error":{
                        "type":"MaxSessionTurnsError",
                        "message":"Maximum session turns exceeded"
                    },
                    "stats":{
                        "total_tokens":100,
                        "input_tokens":50,
                        "output_tokens":50,
                        "cached":0,
                        "input":50,
                        "duration_ms":1200,
                        "tool_calls":0,
                        "models":{}
                    }
                }),
            )
            .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::TurnFailed { error, code }) = events.first().map(|e| &e.kind)
        else {
            panic!("expected TurnFailed");
        };
        assert_eq!(error, "Maximum session turns exceeded");
        assert_eq!(code.as_deref(), Some("MaxSessionTurnsError"));
    }

    #[tokio::test]
    async fn result_without_stats_completes_with_no_usage() {
        let t = TestCtx::new();
        let ctx = t.as_ctx();
        GeminiDialect
            .on_frame(
                &ctx,
                &json!({
                    "type":"result",
                    "timestamp":"2025-10-10T12:00:00.000Z",
                    "status":"success"
                }),
            )
            .await;
        let events = t.events.lock().await;
        let Some(StreamEventKind::TurnCompleted { usage }) = events.first().map(|e| &e.kind) else {
            panic!("expected TurnCompleted");
        };
        assert!(usage.is_none());
    }

    #[test]
    fn launch_args_map_modes_resume_and_model() {
        let d = GeminiDialect;
        let base = SessionConfig::default();
        let args = d.launch_args(&base, None);
        assert_eq!(args, vec!["--approval-mode", "yolo"]);

        let plan = SessionConfig {
            mode: Some("plan".to_string()),
            ..Default::default()
        };
        assert_eq!(d.launch_args(&plan, None), vec!["--approval-mode", "plan"]);

        let edits = SessionConfig {
            mode: Some("acceptEdits".to_string()),
            ..Default::default()
        };
        assert_eq!(
            d.launch_args(&edits, None),
            vec!["--approval-mode", "auto_edit"]
        );

        let with_model = SessionConfig {
            model: Some("flash".to_string()),
            mode: Some("bypassPermissions".to_string()),
            ..Default::default()
        };
        assert_eq!(
            d.launch_args(&with_model, Some("d7f5")),
            vec![
                "--resume",
                "d7f5",
                "--model",
                "flash",
                "--approval-mode",
                "yolo"
            ]
        );
    }

    #[test]
    fn prompt_args_use_the_prompt_flag() {
        let d = GeminiDialect;
        assert_eq!(
            d.prompt_args(&PromptInput::Text("hi there".into()))
                .unwrap(),
            vec!["--prompt", "hi there"]
        );
        assert!(d.prompt_args(&PromptInput::Blocks(vec![])).is_err());
    }

    /// End-to-end through the real one-shot machinery: a fake `gemini`
    /// executable emits verbatim frames; the turn must observe the
    /// composed launch line (registry base + dialect args + prompt) and
    /// produce the full normalized event sequence. Stands in for a live
    /// CLI on machines without one (see UNVERIFIED above).
    #[tokio::test]
    async fn one_shot_session_runs_a_fake_cli_end_to_end() {
        use crate::backend::AgentSession;
        use crate::backend::registry::ResolvedBackend;
        use crate::backend::streamjson::OneShotSession;
        use std::time::Duration;

        let dir = std::env::temp_dir().join(format!("damon-gemini-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let args_file = dir.join("argv");
        let script = dir.join("gemini");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {args}\ncat <<'FRAMES'\n",
                args = args_file.display()
            )
            + r#"{"type":"init","timestamp":"2025-10-10T12:00:00.000Z","session_id":"s-e2e","model":"gemini-2.5-pro"}
{"type":"message","timestamp":"2025-10-10T12:00:00.000Z","role":"assistant","content":"Hello","delta":true}
{"type":"message","timestamp":"2025-10-10T12:00:00.000Z","role":"assistant","content":" world","delta":true}
{"type":"result","timestamp":"2025-10-10T12:00:01.000Z","status":"success","stats":{"total_tokens":100,"input_tokens":60,"output_tokens":40,"cached":10,"input":50,"duration_ms":900,"tool_calls":0,"models":{}}}
FRAMES"#,
        )
        .unwrap();
        make_executable(&script);

        let resolved = ResolvedBackend {
            id: "gemini".into(),
            title: "Gemini CLI (Google)".into(),
            command: script.to_string_lossy().to_string(),
            // The registry's catalog base line, as resolve_backends
            // would hand it to the session.
            args: vec![
                "--output-format".into(),
                "stream-json".into(),
                "--skip-trust".into(),
            ],
            env: vec![],
            auth_hint: String::new(),
            detected: true,
        };
        let session = OneShotSession::new(
            &resolved,
            GeminiDialect::dialect(),
            SessionConfig {
                cwd: dir.clone(),
                ..Default::default()
            },
        );
        let mut events = session.subscribe();
        session
            .start_turn(PromptInput::Text("say hi".into()))
            .await
            .unwrap();

        let mut thread = None;
        let mut text = String::new();
        let completed;
        loop {
            let ev = tokio::time::timeout(Duration::from_secs(15), events.recv())
                .await
                .expect("timed out")
                .expect("event channel closed");
            match ev.kind {
                StreamEventKind::TurnStarted => {}
                StreamEventKind::ThreadStarted { native_handle } => {
                    thread = Some(native_handle);
                }
                StreamEventKind::Timeline(TimelineItem::AssistantMessage { text: t }) => {
                    text.push_str(&t);
                }
                StreamEventKind::TurnCompleted { usage } => {
                    assert_eq!(usage.unwrap().input_tokens, Some(60));
                    completed = true;
                    break;
                }
                kind => panic!("unexpected event: {kind:?}"),
            }
        }
        assert!(completed);
        assert_eq!(thread.as_deref(), Some("s-e2e"));
        assert_eq!(text, "Hello world");

        // The process must have seen the exact composed launch line.
        let argv = std::fs::read_to_string(&args_file).unwrap();
        let argv: Vec<&str> = argv.lines().collect();
        assert_eq!(
            argv,
            vec![
                "--output-format",
                "stream-json",
                "--skip-trust",
                "--approval-mode",
                "yolo",
                "--prompt",
                "say hi"
            ]
        );
    }

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(path, perms).unwrap();
    }

    #[cfg(not(unix))]
    fn make_executable(_path: &std::path::Path) {}

    /// Test-only accessors so assertions read intent, not pattern noise.
    trait ToolCallTestExt {
        fn detail_input(&self) -> &Value;
        fn detail_output(&self) -> &str;
    }

    impl ToolCallTestExt for ToolCall {
        fn detail_input(&self) -> &Value {
            match &self.detail {
                ToolCallDetail::Unknown { input, .. } => input,
                _ => panic!("expected unknown detail"),
            }
        }

        fn detail_output(&self) -> &str {
            match &self.detail {
                ToolCallDetail::Unknown { output, .. } => output.as_str().unwrap_or_default(),
                _ => panic!("expected unknown detail"),
            }
        }
    }
    #[test]
    fn catalog_describes_auto_approving_modes_honestly() {
        let c = GeminiDialect.catalog();
        let by_id = |id: &str| {
            c.modes
                .iter()
                .find(|m| m.id == id)
                .unwrap_or_else(|| panic!("mode {id} missing"))
                .description
                .clone()
                .unwrap_or_default()
        };
        // "default" must not promise it will ask - headless gemini
        // auto-approves; the description says so.
        assert!(by_id("default").contains("auto-approves"));
        assert!(by_id("bypassPermissions").contains("auto-approves"));
        assert!(by_id("acceptEdits").contains("auto_edit"));
        assert!(by_id("plan").contains("plan"));
    }
}
