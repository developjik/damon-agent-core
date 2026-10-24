//! Normalized backend model — the shapes every agent backend translates
//! its native protocol into. Provider-agnostic: Claude stream-json, Codex
//! app-server, and OMP rpc-mode all produce these types.
//!
//! Design borrowed from Paseo's agent-sdk-types: a two-level split
//! (AgentClient = the brand, AgentSession = one live conversation), a
//! discriminated ToolCallDetail with an `unknown` escape hatch so a
//! mapping miss degrades rendering instead of dropping data, and
//! capability flags so surfaces hide what a backend cannot do.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Backend identifier: "claude" | "codex" | "omp" (or a configured custom id).
pub type ProviderId = String;

/// Provider-agnostic session creation config. Each backend maps these to
/// its native launch line / handshake fields.
#[derive(Clone, Debug, Default)]
pub struct SessionConfig {
    /// Working directory the session runs in. Backends reject empty or
    /// relative paths; the daemon supplies an absolute one.
    pub cwd: PathBuf,
    pub model: Option<String>,
    /// Provider mode: plan / default / full-access etc.
    pub mode: Option<String>,
    /// MCP servers handed to the agent in its native config shape.
    pub mcp_servers: HashMap<String, McpServerConfig>,
    /// Extra directories granted beyond the cwd (user-granted roots).
    /// Backends with a native widening flag take them at launch
    /// (claude: one `--add-dir` per root); the rest ignore them — the
    /// daemon-side file jail checks the same list regardless, and no
    /// backend fakes support it lacks.
    pub granted_roots: Vec<PathBuf>,
    /// Per-session tool allowlist (mission read-only stages). Backends
    /// with a native flag enforce it at launch (claude:
    /// `--allowedTools t1,t2`); the rest ignore it — mission start
    /// refuses read-only nodes on backends whose capability says they
    /// cannot enforce the list, so nothing silently downgrades.
    pub allowed_tools: Vec<String>,
}

/// One stdio MCP server, normalized. Backends translate to their own
/// config shape (Claude: --mcp-config, Codex: config, OMP: settings).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct McpServerConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

/// Resume token pointing at the provider's own durable session. The
/// provider's transcript is authoritative; Damon's store keeps this as a
/// bookmark plus a search index.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistenceHandle {
    pub provider: ProviderId,
    /// Claude session_id, Codex thread id, OMP session file path.
    pub native_handle: String,
    #[serde(default)]
    pub metadata: Value,
}

/// What a backend can do. Surfaces hide features a backend lacks instead
/// of assuming uniformity.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Capabilities {
    pub streaming: bool,
    pub session_persistence: bool,
    /// Can enumerate native sessions created outside the daemon.
    pub session_listing: bool,
    /// Mode switching at runtime (plan/default/full-access).
    pub dynamic_modes: bool,
    pub mcp_servers: bool,
    /// Reasoning/thinking content streams separately from text.
    pub reasoning_stream: bool,
    /// Mid-turn steering messages.
    pub steer: bool,
    /// Rewind/revert conversation or files.
    pub rewind: bool,
    /// Emits subagent lifecycle/progress events.
    pub subagent_events: bool,
    /// Can enforce a per-session tool allowlist at launch. Mission
    /// read-only agent nodes require this — damon never downgrades a
    /// read-only stage to full access.
    pub tool_allowlist: bool,
}

/// A model a backend offers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelDef {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub selectable: bool,
}

/// A permission/behavior mode a backend offers (plan, default, …).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModeDef {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

/// Models + modes discovered together — one upstream probe per backend.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ProviderCatalog {
    pub models: Vec<ModelDef>,
    pub modes: Vec<ModeDef>,
    pub default_mode: Option<String>,
}

/// A native session created outside the daemon, importable.
#[derive(Clone, Debug, Serialize)]
pub struct ImportableSession {
    pub handle: PersistenceHandle,
    pub title: Option<String>,
    pub cwd: Option<PathBuf>,
    /// Epoch seconds, when known.
    pub modified_at: Option<u64>,
    /// Native file size in bytes, when the source is a file — part of
    /// the scanner's stat signature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

// ---------------------------------------------------------------------------
// Prompt input
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PromptInput {
    Text(String),
    Blocks(Vec<PromptBlock>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PromptBlock {
    Text {
        text: String,
    },
    /// base64 data + mime type.
    Image {
        data: String,
        mime: String,
    },
}

impl PromptInput {
    pub fn text(s: impl Into<String>) -> Self {
        PromptInput::Text(s.into())
    }
}

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cost_usd: Option<f64>,
    pub context_window: Option<u64>,
    pub context_used: Option<u64>,
}

// ---------------------------------------------------------------------------
// Stream events — the normalized event model
// ---------------------------------------------------------------------------

/// One normalized event from a backend. `turn_id` correlates events to
/// the turn that produced them; events outside a turn carry None.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamEvent {
    pub turn_id: Option<String>,
    /// Flattened: the event's `type` tag sits beside `turn_id`, so a
    /// Timeline item's own `kind` tag never collides with it.
    #[serde(flatten)]
    pub kind: StreamEventKind,
}

impl StreamEvent {
    pub fn new(kind: StreamEventKind) -> Self {
        Self {
            turn_id: None,
            kind,
        }
    }

    pub fn in_turn(turn_id: impl Into<String>, kind: StreamEventKind) -> Self {
        Self {
            turn_id: Some(turn_id.into()),
            kind,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEventKind {
    /// The native session/thread was established — carries the handle.
    ThreadStarted {
        native_handle: String,
    },
    TurnStarted,
    TurnCompleted {
        usage: Option<Usage>,
    },
    TurnFailed {
        error: String,
        code: Option<String>,
    },
    TurnCanceled {
        reason: String,
    },
    ModeChanged {
        mode: Option<String>,
    },
    ModelChanged {
        model: String,
    },
    /// One timeline entry (message, tool call, …).
    Timeline(TimelineItem),
    PermissionRequested(PermissionRequest),
    PermissionResolved {
        request_id: String,
    },
    /// The session wants user attention: a turn finished, errored, or a
    /// permission is pending. Drives channel/push notifications.
    AttentionRequired {
        reason: AttentionReason,
    },
    /// A subagent the backend spawned did something (OMP only today).
    Subagent {
        event: Value,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionReason {
    Finished,
    Permission,
}

// ---------------------------------------------------------------------------
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimelineItem {
    UserMessage {
        text: String,
    },
    AssistantMessage {
        text: String,
    },
    Reasoning {
        text: String,
    },
    ToolCall(ToolCall),
    Todo {
        items: Vec<TaskItem>,
    },
    Compaction {
        summary: String,
    },
    /// A turn failed. The daemon persists one such row when a backend
    /// reports `TurnFailed`; live consumers see the TurnFailed event
    /// itself, and this item replays the durable marker for late
    /// subscribers (store role `"error"`).
    Error {
        message: String,
    },
    /// Mapping miss — the raw provider payload is preserved so nothing
    /// is silently dropped from the transcript.
    Unknown {
        raw: Value,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskItem {
    pub content: String,
    pub status: TaskStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCall {
    pub call_id: String,
    pub name: String,
    pub status: ToolCallStatus,
    pub detail: ToolCallDetail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    Running,
    Completed,
    Failed,
}

/// Normalized tool-call rendering detail. `Unknown` preserves the raw
/// input/output when no mapper claims the call.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolCallDetail {
    Shell {
        command: String,
        output: Option<String>,
        exit_code: Option<i32>,
    },
    Read {
        path: String,
        content: Option<String>,
    },
    Edit {
        path: String,
        unified_diff: Option<String>,
    },
    Write {
        path: String,
        content: Option<String>,
    },
    Search {
        query: String,
        content: Option<String>,
    },
    Fetch {
        url: String,
        result: Option<String>,
    },
    SubAgent {
        description: String,
        log: String,
    },
    Plan {
        text: String,
    },
    Unknown {
        input: Value,
        output: Value,
    },
}

// ---------------------------------------------------------------------------
// Permissions
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionKind {
    Tool,
    Question,
    /// A plan-approval ask (claude's ExitPlanMode): the plan text rides
    /// `input.plan`. The daemon also records a durable `plan_reviews`
    /// row for these and gates approval through `plan.respond` — a
    /// backend without a plan tool simply never emits the kind, so no
    /// capability flag is needed.
    Plan,
}

/// One selectable option inside a [`QuestionField`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QuestionOption {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// One structured question inside a `kind: question` ask — the normalized
/// form of claude's AskUserQuestion input, opencode's question batches,
/// and omp select dialogs. An empty `options` list means free text.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QuestionField {
    pub question: String,
    /// Short category chip the agent offered (AskUserQuestion's `header`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<QuestionOption>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub multi_select: bool,
}

impl QuestionField {
    /// Parse the native question shape shared by claude's AskUserQuestion
    /// input and opencode's question batches:
    /// `{question, header?, options: [{label, description?}], multiSelect?}`.
    pub fn from_native(v: &Value) -> Option<Self> {
        let question = v
            .get("question")
            .and_then(Value::as_str)
            .map(str::to_string)?;
        let header = v
            .get("header")
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|h| !h.is_empty());
        let options = v
            .get("options")
            .and_then(Value::as_array)
            .map(|opts| {
                opts.iter()
                    .filter_map(|o| {
                        let label = o.get("label").and_then(Value::as_str)?;
                        Some(QuestionOption {
                            label: label.to_string(),
                            description: o
                                .get("description")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Some(Self {
            question,
            header,
            options,
            multi_select: v
                .get("multiSelect")
                .or_else(|| v.get("multi_select"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }
}

/// Parse a native questions array (AskUserQuestion's `input.questions`,
/// an opencode question batch) into normalized fields. A question
/// missing its `question` text is skipped rather than failing the ask.
pub fn parse_questions(arr: &[Value]) -> Vec<QuestionField> {
    arr.iter().filter_map(QuestionField::from_native).collect()
}

/// Display title for a question batch — the first question's text.
pub fn questions_title(questions: &[QuestionField]) -> String {
    questions
        .first()
        .map(|q| q.question.clone())
        .unwrap_or_else(|| "Question".to_string())
}

/// Pull one free-text answer out of the canonical `answers` shape — a
/// bare string, the first string value of the map (or first label of an
/// array value). For backends whose wire carries a single free-text
/// answer (codex input requests, omp `input` dialogs).
pub fn single_answer_text(answers: &Value) -> Option<String> {
    match answers {
        Value::String(s) => Some(s.clone()),
        Value::Object(map) => map.values().find_map(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Array(arr) => arr.first().and_then(Value::as_str).map(String::from),
            _ => None,
        }),
        Value::Array(arr) => arr.first().and_then(Value::as_str).map(String::from),
        _ => None,
    }
}

/// A permission ask, normalized. `actions` are the buttons the agent
/// itself offered — the daemon renders them verbatim rather than
/// inventing its own allow/deny pair.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub id: String,
    pub kind: PermissionKind,
    /// Tool or operation name ("Bash", "Edit", …).
    pub name: String,
    pub title: Option<String>,
    /// The raw input the agent wants approved (command, diff, …).
    pub input: Option<Value>,
    /// Best-effort normalized rendering of `input`.
    pub detail: Option<ToolCallDetail>,
    pub actions: Vec<PermissionAction>,
    /// "Always allow" style updates the agent suggested.
    pub suggestions: Vec<Value>,
    /// Structured questions for `kind: question` asks. Additive wire
    /// field: surfaces that don't know it keep rendering `actions`/`input`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub questions: Vec<QuestionField>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionAction {
    pub id: String,
    pub label: String,
    pub behavior: PermissionBehavior,
    pub variant: Option<ActionVariant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionBehavior {
    Allow,
    Deny,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionVariant {
    Primary,
    Secondary,
    Danger,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "behavior", rename_all = "snake_case")]
pub enum PermissionResponse {
    Allow {
        action_id: Option<String>,
        updated_input: Option<Value>,
        /// Free-text answer accompanying the allow decision. Consumed by
        /// backends whose wire accepts user input alongside the decision
        /// (e.g. codex questions). `Option` so existing allow payloads that
        /// omit it stay deserializable.
        answer: Option<String>,
        /// Question-card picks: `{question text → label | [labels] |
        /// typed string}` as submitted on `permission.respond`. An allow
        /// with answers submits the user's picks; an allow without them
        /// skips/dismisses the card.
        /// Structured answers to a `kind: question` ask's `questions`,
        /// canonical shape `{question text → label}` (multi-select →
        /// `[labels]`, free text → the typed string; a bare string is
        /// accepted for single-question asks). Each backend converts to
        /// its own wire shape. Additive: omitting it is an allow without
        /// answers — for questions that means "skip/dismiss".
        #[serde(default, skip_serializing_if = "Option::is_none")]
        answers: Option<Value>,
    },
    Deny {
        action_id: Option<String>,
        message: Option<String>,
        /// Interrupt the whole turn, not just this call.
        interrupt: bool,
    },
}

/// No pending ask for the given request id — already answered, settled
/// by the agent (`control_cancel_request`, turn end), or never existed.
/// The RPC layer maps this onto the typed `ASK_NOT_FOUND` wire code so
/// clients know a retry is pointless (a *delivery* failure, by contrast,
/// leaves the ask pending and surfaces as a plain internal error).
#[derive(Debug, thiserror::Error)]
#[error("no pending ask {0}")]
pub struct AskNotFound(pub String);

// ---------------------------------------------------------------------------
// Steering
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SteerResult {
    Accepted,
    /// Backend cannot steer — caller should queue as a follow-up turn.
    Unavailable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_questions_reads_the_native_shape() {
        let qs = parse_questions(&[
            json!({"question": "Which database?", "header": "DB",
                   "options": [{"label": "Postgres", "description": "relational"},
                               {"label": "SQLite"}],
                   "multiSelect": false}),
            json!({"question": "Notes?"}),
            // Missing question text — skipped, not fatal.
            json!({"options": [{"label": "orphan"}]}),
        ]);
        assert_eq!(qs.len(), 2);
        assert_eq!(qs[0].question, "Which database?");
        assert_eq!(qs[0].header.as_deref(), Some("DB"));
        assert_eq!(qs[0].options.len(), 2);
        assert_eq!(qs[0].options[0].description.as_deref(), Some("relational"));
        assert!(!qs[0].multi_select);
        // snake_case alias accepted alongside the camelCase wire form.
        assert!(!qs[1].multi_select);
        assert!(qs[1].options.is_empty());
    }

    #[test]
    fn single_answer_text_pulls_the_first_usable_answer() {
        assert_eq!(single_answer_text(&json!("bare")), Some("bare".into()));
        assert_eq!(
            single_answer_text(&json!({"Anything else?": "ship it"})),
            Some("ship it".into())
        );
        assert_eq!(
            single_answer_text(&json!({"pick": ["A", "B"]})),
            Some("A".into())
        );
        assert_eq!(single_answer_text(&json!({"n": 3})), None);
    }

    #[test]
    fn questions_title_takes_the_first_question() {
        let qs = vec![QuestionField {
            question: "first".into(),
            header: None,
            options: vec![],
            multi_select: false,
        }];
        assert_eq!(questions_title(&qs), "first");
        assert_eq!(questions_title(&[]), "Question");
    }
}
