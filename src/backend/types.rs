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
    },
    Deny {
        action_id: Option<String>,
        message: Option<String>,
        /// Interrupt the whole turn, not just this call.
        interrupt: bool,
    },
}

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
