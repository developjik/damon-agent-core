//! Mission DAG runner — resident, headless-first scheduling of staged
//! agent workflows (adapted from upstream desktop-cc-gui's mission
//! engine; the piece upstream keeps in localStorage and dies with the
//! webview — damon is the resident host it never had).
//!
//! Scope discipline (upstream's own plan doc: 最小实现而非通用工作流平台
//! — minimal implementation, not a general workflow platform):
//!  - node kinds `input | agent | human | output` ONLY (foreach is
//!    future work, named as such in the docs);
//!  - edges are condition-routed (`key=value`, `key!=value`; `and`
//!    binds over `or`; missing key ⇒ false) plus a `default` edge taken
//!    only when nothing else matched — the grammar and test semantics
//!    are ported from upstream's conditions.ts;
//!  - agent nodes run as real damon turns (one session per node per
//!    attempt), `readOnly` nodes require a backend that can enforce a
//!    tool allowlist — mission start REFUSES otherwise (never
//!    downgrades);
//!  - human nodes park tasks at `waiting_human` and create inbox
//!    items; `mission.respond` resolves them (one answer wins — the
//!    status CAS is the gate);
//!  - failure semantics are honest: auto-retries up to the flow's
//!    setting, then `failed`; a human `exclude` terminates the branch
//!    and cancels downstream (never pretending success); late async
//!    results are dropped after cancel via per-task execution tokens;
//!  - crash honesty: on boot, unfinished runs converge to `cancelled`
//!    + `interrupted` — never faked as still-running.
//!
//! Runs are daemon-resident: SQLite is the truth, never the client.
//! Every state transition persists BEFORE it fans out. Run status is
//! always derived from the task states — never a stale stored field.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::store::{MissionTaskTransition, Store};

/// Node kinds staged this round. Serialized lowercase (wire + store).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MissionNodeKind {
    Input,
    Agent,
    Human,
    Output,
}

impl MissionNodeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MissionNodeKind::Input => "input",
            MissionNodeKind::Agent => "agent",
            MissionNodeKind::Human => "human",
            MissionNodeKind::Output => "output",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "input" => Some(MissionNodeKind::Input),
            "agent" => Some(MissionNodeKind::Agent),
            "human" => Some(MissionNodeKind::Human),
            "output" => Some(MissionNodeKind::Output),
            _ => None,
        }
    }
}

/// Task states: queued → running → succeeded | failed | cancelled |
/// excluded; human nodes park at waiting_human. Terminal: succeeded,
/// failed, cancelled, excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissionTaskStatus {
    Queued,
    Running,
    WaitingHuman,
    Succeeded,
    Failed,
    Cancelled,
    Excluded,
}

impl MissionTaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            MissionTaskStatus::Queued => "queued",
            MissionTaskStatus::Running => "running",
            MissionTaskStatus::WaitingHuman => "waiting_human",
            MissionTaskStatus::Succeeded => "succeeded",
            MissionTaskStatus::Failed => "failed",
            MissionTaskStatus::Cancelled => "cancelled",
            MissionTaskStatus::Excluded => "excluded",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "queued" => MissionTaskStatus::Queued,
            "running" => MissionTaskStatus::Running,
            "waiting_human" => MissionTaskStatus::WaitingHuman,
            "succeeded" => MissionTaskStatus::Succeeded,
            "failed" => MissionTaskStatus::Failed,
            "cancelled" => MissionTaskStatus::Cancelled,
            "excluded" => MissionTaskStatus::Excluded,
            _ => return None,
        })
    }
    pub fn is_terminal(self) -> bool {
        !matches!(
            self,
            MissionTaskStatus::Queued
                | MissionTaskStatus::Running
                | MissionTaskStatus::WaitingHuman
        )
    }
}

// ---------------------------------------------------------------------------
// Conditions — the ported grammar (upstream conditions.ts)
// ---------------------------------------------------------------------------

/// `default` (or empty) marks the fallback edge, taken only when no
/// other outgoing condition matched — evaluated by the scheduler over
/// the whole edge group, never by the expression evaluator.
pub const DEFAULT_CONDITION: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedClause {
    pub key: String,
    pub negate: bool,
    pub value: String,
}

/// `and`-joined clause groups, OR'd together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedExpression {
    pub groups: Vec<Vec<ParsedClause>>,
}

/// `default`, empty, or whitespace-only = the default edge.
pub fn is_default_condition(condition: Option<&str>) -> bool {
    match condition {
        None => true,
        Some(c) => {
            let t = c.trim();
            t.is_empty() || t.eq_ignore_ascii_case(DEFAULT_CONDITION)
        }
    }
}

/// One clause: `key=value` or `key!=value`. The value must be a single
/// token without whitespace, so `risk=high or` tails never parse as a
/// value.
fn parse_clause(raw: &str) -> Option<ParsedClause> {
    let raw = raw.trim();
    // The earliest operator wins: `status!=ok` must read as
    // key=`status` op=`!=`, not key=`status!` — and `a=b!=c` must read
    // as key=`a` value=`b!=c` (values are single tokens; `=` inside a
    // value is data).
    let eq = raw.find('=');
    let neq = raw.find("!=");
    let (key, op, rest) = match (neq, eq) {
        (Some(n), Some(e)) if n + 1 == e => (raw[..n].trim_end(), "!=", &raw[e + 1..]),
        (Some(_), _) => return None, // `!=` without its `=` — malformed
        (_, Some(e)) => (raw[..e].trim_end(), "=", &raw[e + 1..]),
        (None, None) => return None,
    };
    if key.is_empty()
        || !key
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        || !key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return None;
    }
    let value = rest.trim();
    if value.is_empty() || value.chars().any(|c| c.is_whitespace()) {
        return None;
    }
    Some(ParsedClause {
        key: key.to_string(),
        negate: op == "!=",
        value: value.to_string(),
    })
}

/// `clause (or clause)*`, clause = and-joined comparisons. Any
/// malformed part → None (malformed never matches).
pub(crate) fn parse_condition(condition: &str) -> Option<ParsedExpression> {
    // Whitespace-token scan that splits on the case-insensitive `or`
    // keyword first, then each part on `and` — the two splits must
    // keep their separators distinct or `a=1 or b=2 and c=3` would
    // flatten into three orphan clauses.
    let mut or_parts: Vec<Vec<String>> = vec![Vec::new()];
    for token in condition.split_whitespace() {
        if token.eq_ignore_ascii_case("or") {
            or_parts.push(Vec::new());
        } else if token.eq_ignore_ascii_case("and") {
            // `and` is handled by the per-part split below.
            continue;
        } else {
            or_parts.last_mut().unwrap().push(token.to_string());
        }
    }
    let mut groups = Vec::new();
    for part in or_parts {
        if part.is_empty() {
            return None; // leading/trailing/double keyword
        }
        // Within an or-part every token was and-joined: each token is
        // one clause of the group.
        let group = part
            .iter()
            .filter_map(|c| parse_clause(c))
            .collect::<Vec<_>>();
        if group.len() != part.len() {
            return None; // a clause failed to parse — malformed never matches
        }
        groups.push(group);
    }
    (!groups.is_empty()).then_some(ParsedExpression { groups })
}

/// Context lookup: top-level keys first, then the explicit
/// `settings.` prefix over the run settings. Missing key ⇒ None.
fn resolve_value<'a>(context: &'a Value, settings: &'a Value, key: &str) -> Option<&'a Value> {
    if let Some(v) = context.get(key) {
        return Some(v);
    }
    if let Some(rest) = key.strip_prefix("settings.") {
        return settings.get(rest);
    }
    // Upstream flattens the run settings into the condition context:
    // bare keys read the settings too (explicit keys win).
    settings.get(key)
}

fn clause_matches(clause: &ParsedClause, context: &Value, settings: &Value) -> bool {
    let Some(raw) = resolve_value(context, settings, &clause.key) else {
        return false; // missing key ⇒ false — never assumed
    };
    if raw.is_null() {
        return false;
    }
    let equals = match raw {
        Value::String(s) => s == &clause.value,
        // Non-string values compare by their JSON text (`a=1` matches
        // the number 1) — upstream treats conditions as token strings.
        #[allow(clippy::cmp_owned)] // token-string semantics by design
        other => other.to_string() == *clause.value,
    };
    clause.negate != equals // negate XOR equals
}

/// Evaluate a non-default condition. Malformed expressions return
/// false (never match). `default` conditions are the caller's business.
pub fn evaluate_condition(condition: &str, context: &Value, settings: &Value) -> bool {
    if is_default_condition(Some(condition)) {
        return false;
    }
    let Some(parsed) = parse_condition(condition) else {
        return false;
    };
    parsed
        .groups
        .iter()
        .any(|group| group.iter().all(|c| clause_matches(c, context, settings)))
}

/// Validation helper for mission.create — default conditions are valid.
pub fn is_valid_condition(condition: Option<&str>) -> bool {
    match condition {
        None => true,
        Some(c) if is_default_condition(Some(c)) => true,
        Some(c) => parse_condition(c).is_some(),
    }
}

// ---------------------------------------------------------------------------
// Flow definition (validated shape)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MissionNode {
    pub id: String,
    pub kind: MissionNodeKind,
    pub title: String,
    /// agent: the instruction. input: the literal value (any JSON).
    pub prompt: Option<String>,
    /// agent: input value for input nodes.
    pub value: Option<Value>,
    /// agent: backend id + working directory + read-only flag.
    pub backend: Option<String>,
    pub cwd: Option<PathBuf>,
    pub read_only: bool,
    /// output: artifact label.
    pub artifact: Option<String>,
    /// agent: extra prompt-embedded key=value hints (optional).
    pub allowed_tools: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct MissionEdge {
    pub from: String,
    pub to: String,
    /// None/"default"/"" = the fallback edge.
    pub condition: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MissionSettings {
    /// Concurrent agent-node budget (human nodes never occupy a slot).
    pub concurrency: u32,
    /// Auto-retries per agent task before `failed`.
    pub retries: u32,
}

#[derive(Debug, Clone)]
pub struct MissionDefinition {
    pub nodes: Vec<MissionNode>,
    pub edges: Vec<MissionEdge>,
    pub settings: MissionSettings,
}

pub fn parse_definition(v: &Value) -> Result<MissionDefinition> {
    let mut nodes = Vec::new();
    for n in v["nodes"].as_array().context("nodes must be an array")? {
        let id = n["id"].as_str().context("node id required")?.to_string();
        let kind = MissionNodeKind::parse(n["kind"].as_str().context("node kind required")?)
            .context("unknown node kind (staged: input|agent|human|output)")?;
        nodes.push(MissionNode {
            title: n["title"].as_str().unwrap_or(&id).to_string(),
            id,
            kind,
            prompt: n["prompt"].as_str().map(String::from),
            value: n.get("value").cloned(),
            backend: n["backend"].as_str().map(String::from),
            cwd: n["cwd"].as_str().map(PathBuf::from),
            read_only: n["readOnly"].as_bool().unwrap_or(false),
            artifact: n["artifact"].as_str().map(String::from),
            allowed_tools: n["allowedTools"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|t| t.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
        });
    }
    if nodes.is_empty() {
        anyhow::bail!("flow needs at least one node");
    }
    let mut ids: HashSet<&String> = HashSet::new();
    for n in &nodes {
        if !ids.insert(&n.id) {
            anyhow::bail!("duplicate node id {}", n.id);
        }
    }
    let mut edges = Vec::new();
    for e in v["edges"].as_array().context("edges must be an array")? {
        let from = e["from"]
            .as_str()
            .context("edge from required")?
            .to_string();
        let to = e["to"].as_str().context("edge to required")?.to_string();
        if !ids.contains(&from) || !ids.contains(&to) {
            anyhow::bail!("edge references unknown node: {from} → {to}");
        }
        let condition = e["condition"].as_str().map(String::from);
        if let Some(c) = &condition
            && !is_valid_condition(Some(c))
        {
            anyhow::bail!("malformed edge condition {c:?} ({from} → {to})");
        }
        edges.push(MissionEdge {
            from,
            to,
            condition,
        });
    }
    let settings = &v["settings"];
    let definition = MissionDefinition {
        nodes,
        edges,
        settings: MissionSettings {
            concurrency: settings["concurrency"].as_u64().unwrap_or(2).max(1) as u32,
            retries: settings["retries"].as_u64().unwrap_or(0) as u32,
        },
    };
    validate_definition(&definition)?;
    Ok(definition)
}

/// Structural validation over the typed definition — the capability
/// checks (backend enforcement) happen at mission.start.
pub fn validate_definition(def: &MissionDefinition) -> Result<()> {
    let inputs = def
        .nodes
        .iter()
        .filter(|n| n.kind == MissionNodeKind::Input)
        .count();
    if inputs == 0 {
        anyhow::bail!("flow needs at least one input node");
    }
    for n in &def.nodes {
        match n.kind {
            MissionNodeKind::Agent => {
                if n.prompt.as_deref().unwrap_or_default().trim().is_empty() {
                    anyhow::bail!("agent node {} needs a prompt", n.id);
                }
                if n.backend.as_deref().unwrap_or_default().trim().is_empty() {
                    anyhow::bail!("agent node {} needs a backend", n.id);
                }
                if n.read_only && n.allowed_tools.is_empty() {
                    anyhow::bail!(
                        "read-only agent node {} needs allowedTools — damon refuses what it cannot enforce",
                        n.id
                    );
                }
            }
            MissionNodeKind::Input => {
                if n.value.is_none() {
                    anyhow::bail!("input node {} needs a value", n.id);
                }
            }
            MissionNodeKind::Human | MissionNodeKind::Output => {}
        }
    }
    // Root rule: every input node must have no incoming edges; every
    // non-input must have at least one incoming edge.
    for n in &def.nodes {
        let incoming = def.edges.iter().filter(|e| e.to == n.id).count();
        match n.kind {
            MissionNodeKind::Input => {
                if incoming > 0 {
                    anyhow::bail!("input node {} must not have incoming edges", n.id);
                }
            }
            _ => {
                if incoming == 0 {
                    anyhow::bail!("node {} has no incoming edges", n.id);
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Condition context + edge activation (ported scheduler semantics)
// ---------------------------------------------------------------------------

/// Output expansion: objects spread their keys, everything else lands
/// in `value` / `output`.
fn output_context(output: &Value) -> Value {
    if let Some(map) = output.as_object() {
        let mut m = map.clone();
        m.insert("output".to_string(), output.clone());
        Value::Object(m)
    } else {
        json!({"value": output, "output": output})
    }
}

/// Edge group activation for one succeeded source: non-default edges
/// evaluated against the context (upstream output + flow settings);
/// only when NOTHING matched does the default edge activate.
pub fn activated_edges(edges: &[MissionEdge], output: &Value, settings: &Value) -> Vec<usize> {
    let mut activated = Vec::new();
    let context = output_context(output);
    let mut matched = false;
    for (idx, edge) in edges.iter().enumerate() {
        if is_default_condition(edge.condition.as_deref()) {
            continue;
        }
        if evaluate_condition(edge.condition.as_deref().unwrap_or(""), &context, settings) {
            activated.push(idx);
            matched = true;
        }
    }
    if !matched {
        // Every default edge from the source fires when no explicit
        // sibling matched — parallel fan-out edges are each a fallback.
        for (idx, edge) in edges.iter().enumerate() {
            if is_default_condition(edge.condition.as_deref()) {
                activated.push(idx);
            }
        }
    }
    activated
}

// ---------------------------------------------------------------------------
// Prompt assembly (agent nodes ask for key=value lines for branching)
// ---------------------------------------------------------------------------

/// The agent prompt: the node's instruction, the upstream outputs as
/// JSON context, and the emission contract — `key=value` lines the
/// scheduler parses into the condition context for downstream edges.
pub fn build_agent_prompt(prompt: &str, upstream: &Value) -> String {
    format!(
        "{prompt}\n\n\
         Context from earlier steps (JSON):\n{}\n\n\
         When you finish, emit one `key=value` line per result key \
         (e.g. `status=ok`, `risk=low`) so the next step can branch on \
         your results. Keep each value a single token.",
        serde_json::to_string_pretty(upstream).unwrap_or_default()
    )
}

/// Parse `key=value` lines out of an agent's final message — the
/// condition context for downstream edges. Non-matching lines are
/// ignored; a message with no key=value lines falls back to
/// `{"output": text}`.
/// Extract the text of an assistant message's OpenAI-shaped `data`:
/// string content, or the joined text of content blocks (the shapes the
/// store's timeline rows carry).
pub fn assistant_text(data: &Value) -> Option<String> {
    match &data["content"] {
        Value::String(t) => Some(t.clone()),
        Value::Array(items) => {
            let mut parts = Vec::new();
            for item in items {
                if item["type"] == "text"
                    && let Some(t) = item["text"].as_str()
                {
                    parts.push(t.to_string());
                }
            }
            (!parts.is_empty()).then(|| parts.join("\n"))
        }
        _ => None,
    }
}

pub fn parse_agent_output(text: &str) -> Value {
    let mut map = serde_json::Map::new();
    for line in text.lines() {
        // Liberal shape: `key=value` pairs separated by spaces on one
        // line, or one pair per line. A pair is a whitespace-free token
        // whose key looks like an identifier.
        for pair in line.split_whitespace() {
            let Some(eq) = pair.find('=') else {
                continue;
            };
            let key = pair[..eq].trim();
            let value = pair[eq + 1..].trim();
            if !key.is_empty()
                && !value.is_empty()
                && !key.contains(' ')
                && key
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            {
                map.insert(key.to_string(), json!(value));
            }
        }
    }
    if map.is_empty() {
        json!({"output": text})
    } else {
        Value::Object(map)
    }
}

// ---------------------------------------------------------------------------
// Registry + events
// ---------------------------------------------------------------------------

/// Per-run channels: progress events for watchers plus the run's cancel
/// token and the human-respond command inbox.
pub struct MissionRunChannels {
    pub events: broadcast::Sender<Value>,
    pub cancel: CancellationToken,
    pub commands: mpsc::Sender<MissionCommand>,
}

/// A human decision on a waiting task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissionDecision {
    Approve,
    Feedback,
    Exclude,
}

#[derive(Debug, Clone)]
pub enum MissionCommand {
    /// Human decision on one node: approve | feedback(text) | exclude.
    Respond {
        node_id: String,
        decision: String,
        feedback: Option<String>,
    },
}

#[derive(Default)]
pub struct MissionRegistry {
    runs: Mutex<HashMap<String, MissionRunChannels>>,
}

impl MissionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        run_id: &str,
        events: broadcast::Sender<Value>,
        cancel: CancellationToken,
        commands: mpsc::Sender<MissionCommand>,
    ) {
        self.runs.lock().insert(
            run_id.to_string(),
            MissionRunChannels {
                events,
                cancel,
                commands,
            },
        );
    }

    pub fn finish(&self, run_id: &str) {
        self.runs.lock().remove(run_id);
    }

    pub fn cancel(&self, run_id: &str) -> bool {
        self.runs.lock().get(run_id).is_some_and(|c| {
            c.cancel.cancel();
            true
        })
    }

    pub fn send_command(&self, run_id: &str, cmd: MissionCommand) -> bool {
        self.runs
            .lock()
            .get(run_id)
            .and_then(|c| c.commands.try_send(cmd).ok())
            .is_some()
    }

    pub fn events_sender(&self, run_id: &str) -> Option<broadcast::Sender<Value>> {
        self.runs.lock().get(run_id).map(|c| c.events.clone())
    }

    /// Run ids with a live scheduler — boot recovery must never sweep
    /// these (they are this process's in-flight work, not crash
    /// orphans).
    pub fn live_run_ids(&self) -> Vec<String> {
        self.runs.lock().keys().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Scheduler - one task per run; settle() convergence; honest failures
// ---------------------------------------------------------------------------

/// One in-memory task instance (mirrors its persisted row).
#[derive(Debug, Clone)]
pub struct MissionTaskState {
    pub node_id: String,
    pub status: MissionTaskStatus,
    pub attempt: u32,
    pub output: Option<Value>,
    pub error: Option<String>,
}

/// Request for one agent-node run - a damon turn on a per-node session.
#[derive(Debug, Clone)]
pub struct MissionAgentRequest {
    pub backend: String,
    pub cwd: Option<PathBuf>,
    pub prompt: String,
    pub allowed_tools: Vec<String>,
    pub read_only: bool,
    pub cancel: CancellationToken,
}

/// Injected agent-node execution - implemented over the SessionManager
/// and turn driver in rpc.rs; tests substitute a stub (no processes,
/// no network).
#[async_trait]
pub trait MissionAgentRunner: Send + Sync {
    async fn run_agent_node(&self, req: MissionAgentRequest) -> Result<Value, String>;
}

/// The scheduler: loads the run's task rows, settles to convergence,
/// spawns agent executors, and persists every transition before
/// broadcasting it.
pub struct MissionScheduler {
    pub run_id: String,
    pub definition: MissionDefinition,
    pub store: Store,
    pub runner: Arc<dyn MissionAgentRunner>,
    pub events: broadcast::Sender<Value>,
    pub commands: mpsc::Receiver<MissionCommand>,
    pub cancel: CancellationToken,
    /// Per-node execution tokens: a cancelled/requeued node invalidates
    /// its in-flight executor result (late async results are dropped).
    tokens: HashMap<String, u64>,
    token_seq: u64,
}

impl MissionScheduler {
    pub fn new(
        run_id: String,
        definition: MissionDefinition,
        store: Store,
        runner: Arc<dyn MissionAgentRunner>,
        events: broadcast::Sender<Value>,
        commands: mpsc::Receiver<MissionCommand>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            run_id,
            definition,
            store,
            runner,
            events,
            commands,
            cancel,
            tokens: HashMap::new(),
            token_seq: 0,
        }
    }

    /// The convergence loop. Returns when every task is terminal or the
    /// run was cancelled.
    pub async fn run(mut self) -> Result<()> {
        let mut tasks = self.load_tasks().await?;
        let (completions_tx, mut completions_rx) =
            mpsc::unbounded_channel::<(String, u64, Result<Value, String>)>();
        loop {
            if self.cancel.is_cancelled() {
                self.cancel_all(&mut tasks).await?;
                self.finish_run().await?;
                return Ok(());
            }
            // Human respond commands. The inbox CAS already picked the
            // winner in the rpc layer; apply the decision to the DAG.
            while let Ok(cmd) = self.commands.try_recv() {
                let MissionCommand::Respond {
                    node_id,
                    decision,
                    feedback,
                } = cmd;
                self.apply_respond(&mut tasks, &node_id, &decision, feedback.as_deref())
                    .await?;
            }

            let spawns = self.settle(&mut tasks).await?;

            if self.all_terminal(&tasks) {
                self.finish_run().await?;
                return Ok(());
            }

            if spawns.is_empty() {
                // Nothing runnable - wait for a completion, a respond
                // command, or cancellation.
                tokio::select! {
                    Some((node_id, token, outcome)) = completions_rx.recv() => {
                        self.apply_completion(&mut tasks, &node_id, token, outcome).await?;
                    }
                    Some(cmd) = self.commands.recv() => {
                        let MissionCommand::Respond {
                            node_id,
                            decision,
                            feedback,
                        } = cmd;
                        self.apply_respond(&mut tasks, &node_id, &decision, feedback.as_deref())
                            .await?;
                    }
                    _ = self.cancel.cancelled() => {}
                }
                continue;
            }

            for (node_id, req) in spawns {
                let runner = self.runner.clone();
                let completions = completions_tx.clone();
                self.token_seq += 1;
                let token = self.token_seq;
                self.tokens.insert(node_id.clone(), token);
                tokio::spawn(async move {
                    let outcome = runner.run_agent_node(req).await;
                    let _ = completions.send((node_id, token, outcome));
                });
            }
            tokio::select! {
                Some((node_id, token, outcome)) = completions_rx.recv() => {
                    self.apply_completion(&mut tasks, &node_id, token, outcome).await?;
                }
                Some(cmd) = self.commands.recv() => {
                    let MissionCommand::Respond {
                        node_id,
                        decision,
                        feedback,
                    } = cmd;
                    self.apply_respond(&mut tasks, &node_id, &decision, feedback.as_deref())
                        .await?;
                }
                _ = self.cancel.cancelled() => {}
            }
        }
    }

    async fn load_tasks(&self) -> Result<Vec<MissionTaskState>> {
        let rows = self.store.mission_tasks(&self.run_id).await?;
        Ok(rows
            .into_iter()
            .map(
                |(node_id, status, attempt, output, error)| MissionTaskState {
                    node_id,
                    status: MissionTaskStatus::parse(&status)
                        .unwrap_or(MissionTaskStatus::Cancelled),
                    attempt,
                    output: output.and_then(|o| serde_json::from_str(&o).ok()),
                    error,
                },
            )
            .collect())
    }

    fn all_terminal(&self, tasks: &[MissionTaskState]) -> bool {
        tasks.iter().all(|t| {
            matches!(
                t.status,
                MissionTaskStatus::Succeeded
                    | MissionTaskStatus::Failed
                    | MissionTaskStatus::Cancelled
                    | MissionTaskStatus::Excluded
            )
        })
    }

    /// One convergence pass: cancel unreachable nodes, complete
    /// pass-through nodes, park human nodes, and return the agent nodes
    /// ready to spawn (within the concurrency budget).
    async fn settle(
        &mut self,
        tasks: &mut [MissionTaskState],
    ) -> Result<Vec<(String, MissionAgentRequest)>> {
        let mut spawns = Vec::new();
        let node = |id: &str| self.definition.nodes.iter().find(|n| n.id == id);

        loop {
            let mut changed = false;

            // Activated edges per succeeded source.
            let mut activated: HashSet<(String, String)> = HashSet::new(); // (from, to)
            let outgoing: Vec<MissionEdge> = self
                .definition
                .edges
                .iter()
                .filter(|e| {
                    tasks
                        .iter()
                        .any(|t| t.status == MissionTaskStatus::Succeeded && t.node_id == e.from)
                })
                .cloned()
                .collect();
            for e in &outgoing {
                let src = tasks
                    .iter()
                    .find(|t| t.status == MissionTaskStatus::Succeeded && t.node_id == e.from)
                    .unwrap();
                let from_edges: Vec<MissionEdge> = self
                    .definition
                    .edges
                    .iter()
                    .filter(|x| x.from == e.from)
                    .cloned()
                    .collect();
                let settings_value = json!({
                    "concurrency": self.definition.settings.concurrency,
                    "retries": self.definition.settings.retries,
                });
                let matched = activated_edges(
                    &from_edges,
                    src.output.as_ref().unwrap_or(&Value::Null),
                    &settings_value,
                )
                .into_iter()
                .any(|i| from_edges[i].to == e.to);
                if matched {
                    activated.insert((e.from.clone(), e.to.clone()));
                }
            }

            // Snapshot statuses so the per-node pass can read them
            // while holding a mutable borrow for the transition.
            let statuses: HashMap<String, MissionTaskStatus> = tasks
                .iter()
                .map(|x| (x.node_id.clone(), x.status))
                .collect();
            let status_of = |id: &str| {
                statuses
                    .get(id)
                    .copied()
                    .unwrap_or(MissionTaskStatus::Queued)
            };
            for idx in 0..tasks.len() {
                if tasks[idx].status != MissionTaskStatus::Queued {
                    continue;
                }
                let node_id = tasks[idx].node_id.clone();
                let incoming: Vec<MissionEdge> = self
                    .definition
                    .edges
                    .iter()
                    .filter(|e| e.to == node_id)
                    .cloned()
                    .collect();
                let pending: Vec<_> = incoming
                    .iter()
                    .filter(|e| {
                        !matches!(
                            status_of(&e.from),
                            MissionTaskStatus::Succeeded
                                | MissionTaskStatus::Failed
                                | MissionTaskStatus::Cancelled
                                | MissionTaskStatus::Excluded
                        )
                    })
                    .collect();
                if !pending.is_empty() {
                    continue; // upstream still running
                }
                // A root node (no incoming edges) is activated from the
                // start — the run's entry point.
                let any_activated = incoming.is_empty()
                    || incoming
                        .iter()
                        .any(|e| activated.contains(&(e.from.clone(), e.to.clone())));
                if !any_activated {
                    // Every incoming branch resolved without reaching
                    // this node - unreachable, not an error.
                    tasks[idx].status = MissionTaskStatus::Cancelled;
                    self.persist(
                        &node_id,
                        MissionTaskTransition {
                            status: "cancelled".to_string(),
                            attempt: tasks[idx].attempt,
                            output: None,
                            error: None,
                            stamp_started: false,
                            stamp_ended: true,
                        },
                    )
                    .await?;
                    self.broadcast(node(node_id.as_str()), &tasks[idx]);
                    changed = true;
                    continue;
                }
                match node(&node_id).map(|n| &n.kind) {
                    Some(MissionNodeKind::Input) => {
                        // Pass-through: the input node succeeds with its
                        // value (input is data, not work).
                        let value = node(&node_id)
                            .and_then(|n| n.value.clone())
                            .unwrap_or(Value::Null);
                        tasks[idx].status = MissionTaskStatus::Succeeded;
                        tasks[idx].output = Some(value.clone());
                        self.persist(
                            &node_id,
                            MissionTaskTransition {
                                status: "succeeded".to_string(),
                                attempt: tasks[idx].attempt,
                                output: Some(value.to_string().to_string()),
                                error: None,
                                stamp_started: true,
                                stamp_ended: true,
                            },
                        )
                        .await?;
                        self.broadcast(node(node_id.as_str()), &tasks[idx]);
                        changed = true;
                    }
                    Some(MissionNodeKind::Output) => {
                        // Aggregates upstream outputs and finishes.
                        let mut upstream = serde_json::Map::new();
                        for e in &incoming {
                            if let Some(src) = tasks.iter().find(|x| x.node_id == e.from)
                                && src.status == MissionTaskStatus::Succeeded
                                && let Some(o) = &src.output
                            {
                                upstream.insert(e.from.clone(), o.clone());
                            }
                        }
                        let value = Value::Object(upstream);
                        tasks[idx].status = MissionTaskStatus::Succeeded;
                        tasks[idx].output = Some(value.clone());
                        self.persist(
                            &node_id,
                            MissionTaskTransition {
                                status: "succeeded".to_string(),
                                attempt: tasks[idx].attempt,
                                output: Some(value.to_string().to_string()),
                                error: None,
                                stamp_started: true,
                                stamp_ended: true,
                            },
                        )
                        .await?;
                        self.broadcast(node(node_id.as_str()), &tasks[idx]);
                        changed = true;
                    }
                    Some(MissionNodeKind::Human) => {
                        tasks[idx].status = MissionTaskStatus::WaitingHuman;
                        self.persist(
                            &node_id,
                            MissionTaskTransition {
                                status: "waiting_human".to_string(),
                                attempt: tasks[idx].attempt,
                                output: None,
                                error: None,
                                stamp_started: true,
                                stamp_ended: false,
                            },
                        )
                        .await?;
                        self.store
                            .insert_mission_inbox(&self.run_id, &node_id)
                            .await?;
                        self.broadcast(node(node_id.as_str()), &tasks[idx]);
                        changed = true;
                    }
                    Some(MissionNodeKind::Agent) => {
                        let n = node(&node_id).cloned().unwrap();
                        let max_running =
                            (self.definition.settings.concurrency as usize).clamp(1, 64);
                        let running = tasks
                            .iter()
                            .filter(|x| x.status == MissionTaskStatus::Running)
                            .count();
                        if running >= max_running {
                            continue; // concurrency budget full
                        }
                        // Upstream outputs flattened into the context.
                        let mut upstream = serde_json::Map::new();
                        for e in &incoming {
                            if let Some(src) = tasks.iter().find(|x| x.node_id == e.from)
                                && src.status == MissionTaskStatus::Succeeded
                                && let Some(o) = &src.output
                            {
                                upstream.insert(e.from.clone(), o.clone());
                            }
                        }
                        let prompt = build_agent_prompt(
                            n.prompt.as_deref().unwrap_or(n.title.as_str()),
                            &Value::Object(upstream),
                        );
                        tasks[idx].status = MissionTaskStatus::Running;
                        self.persist(
                            &node_id,
                            MissionTaskTransition {
                                status: "running".to_string(),
                                attempt: tasks[idx].attempt,
                                output: None,
                                error: None,
                                stamp_started: true,
                                stamp_ended: false,
                            },
                        )
                        .await?;
                        let req = MissionAgentRequest {
                            backend: n.backend.unwrap_or_default(),
                            cwd: n.cwd.clone(),
                            prompt,
                            allowed_tools: n.allowed_tools.clone(),
                            read_only: n.read_only,
                            cancel: self.cancel.child_token(),
                        };
                        spawns.push((node_id.clone(), req));
                        self.broadcast(node(node_id.as_str()), &tasks[idx]);
                        changed = true;
                    }
                    _ => {}
                }
            }
            if !changed {
                break;
            }
        }
        Ok(spawns)
    }

    async fn apply_completion(
        &mut self,
        tasks: &mut [MissionTaskState],
        node_id: &str,
        token: u64,
        outcome: Result<Value, String>,
    ) -> Result<()> {
        // Late result from a cancelled/requeued execution - dropped.
        if self.tokens.get(node_id).copied() != Some(token) {
            return Ok(());
        }
        // The run was cancelled — late executor results are dropped;
        // cancel_all already settled the task rows.
        if self.cancel.is_cancelled() {
            return Ok(());
        }
        let Some(t) = tasks.iter_mut().find(|t| t.node_id == node_id) else {
            return Ok(());
        };
        if t.status != MissionTaskStatus::Running {
            return Ok(());
        }
        let n = match self.definition.nodes.iter().find(|n| n.id == node_id) {
            Some(n) => n.clone(),
            None => return Ok(()),
        };
        match outcome {
            Ok(output) => {
                t.status = MissionTaskStatus::Succeeded;
                t.output = Some(output.clone());
                t.error = None;
                self.persist(
                    node_id,
                    MissionTaskTransition {
                        status: "succeeded".to_string(),
                        attempt: t.attempt,
                        output: Some(output.to_string().to_string()),
                        error: None,
                        stamp_started: false,
                        stamp_ended: true,
                    },
                )
                .await?;
            }
            Err(err) => {
                let retries = self.definition.settings.retries.min(10);
                t.error = Some(err.clone());
                if t.attempt <= retries {
                    t.attempt += 1;
                    t.status = MissionTaskStatus::Queued;
                    self.persist(
                        node_id,
                        MissionTaskTransition {
                            status: "queued".to_string(),
                            attempt: t.attempt,
                            output: None,
                            error: Some(err.to_string()),
                            stamp_started: false,
                            stamp_ended: false,
                        },
                    )
                    .await?;
                } else {
                    t.status = MissionTaskStatus::Failed;
                    self.persist(
                        node_id,
                        MissionTaskTransition {
                            status: "failed".to_string(),
                            attempt: t.attempt,
                            output: None,
                            error: Some(err.to_string()),
                            stamp_started: false,
                            stamp_ended: true,
                        },
                    )
                    .await?;
                }
            }
        }
        self.broadcast(
            Some(&n),
            tasks.iter().find(|t| t.node_id == node_id).unwrap(),
        );
        Ok(())
    }

    async fn apply_respond(
        &mut self,
        tasks: &mut [MissionTaskState],
        node_id: &str,
        decision: &str,
        feedback: Option<&str>,
    ) -> Result<()> {
        let decision = match decision {
            "approve" => MissionDecision::Approve,
            "feedback" => MissionDecision::Feedback,
            "exclude" => MissionDecision::Exclude,
            _ => return Ok(()),
        };
        let Some(t) = tasks.iter_mut().find(|t| t.node_id == node_id) else {
            return Ok(());
        };
        let n = match self.definition.nodes.iter().find(|n| n.id == node_id) {
            Some(n) => n.clone(),
            None => return Ok(()),
        };
        match decision {
            MissionDecision::Approve => {
                if t.status != MissionTaskStatus::WaitingHuman {
                    return Ok(());
                }
                t.status = MissionTaskStatus::Succeeded;
                t.output = Some(json!({ "approved": true }));
                self.persist(
                    node_id,
                    MissionTaskTransition {
                        status: "succeeded".to_string(),
                        attempt: t.attempt,
                        output: feedback.map(String::from),
                        error: None,
                        stamp_started: false,
                        stamp_ended: true,
                    },
                )
                .await?;
            }
            MissionDecision::Feedback => {
                if t.status != MissionTaskStatus::WaitingHuman {
                    return Ok(());
                }
                // Re-ask: the feedback rides on the task (mission.get
                // shows it) and the fresh inbox item re-parks the node.
                t.attempt += 1;
                t.status = MissionTaskStatus::Queued;
                self.persist(
                    node_id,
                    MissionTaskTransition {
                        status: "queued".to_string(),
                        attempt: t.attempt,
                        output: None,
                        error: feedback.map(String::from),
                        stamp_started: false,
                        stamp_ended: false,
                    },
                )
                .await?;
            }
            MissionDecision::Exclude => {
                if !matches!(
                    t.status,
                    MissionTaskStatus::WaitingHuman
                        | MissionTaskStatus::Failed
                        | MissionTaskStatus::Queued
                ) {
                    return Ok(());
                }
                // excludeBranch: the node and everything downstream.
                let mut excluded: HashSet<String> = HashSet::new();
                excluded.insert(node_id.to_string());
                loop {
                    let mut grew = false;
                    for e in &self.definition.edges {
                        if excluded.contains(&e.from) && !excluded.contains(&e.to) {
                            excluded.insert(e.to.clone());
                            grew = true;
                        }
                    }
                    if !grew {
                        break;
                    }
                }
                for t in tasks.iter_mut() {
                    if excluded.contains(&t.node_id)
                        && !matches!(
                            t.status,
                            MissionTaskStatus::Excluded | MissionTaskStatus::Succeeded
                        )
                    {
                        t.status = MissionTaskStatus::Excluded;
                        self.tokens
                            .entry(t.node_id.clone())
                            .and_modify(|_| {})
                            .or_insert(0);
                        self.persist(
                            &t.node_id,
                            MissionTaskTransition {
                                status: "excluded".to_string(),
                                attempt: t.attempt,
                                output: None,
                                error: None,
                                stamp_started: false,
                                stamp_ended: true,
                            },
                        )
                        .await?;
                    }
                }
            }
        }
        self.broadcast(
            Some(&n),
            tasks.iter().find(|t| t.node_id == node_id).unwrap(),
        );
        Ok(())
    }

    async fn cancel_all(&mut self, tasks: &mut [MissionTaskState]) -> Result<()> {
        // Invalidate every in-flight execution token — late results
        // from executors that observe the cancel are dropped.
        self.token_seq += 1;
        let poison = self.token_seq;
        for token in self.tokens.values_mut() {
            *token = poison;
        }
        for t in tasks.iter_mut() {
            if !matches!(
                t.status,
                MissionTaskStatus::Succeeded
                    | MissionTaskStatus::Failed
                    | MissionTaskStatus::Cancelled
                    | MissionTaskStatus::Excluded
            ) {
                t.status = MissionTaskStatus::Cancelled;
                self.persist(
                    &t.node_id,
                    MissionTaskTransition {
                        status: "cancelled".to_string(),
                        attempt: t.attempt,
                        output: None,
                        error: None,
                        stamp_started: false,
                        stamp_ended: true,
                    },
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn finish_run(&self) -> Result<()> {
        // An explicit user cancel is a run fact, not a derived status —
        // recorded so the derivation reports `cancelled` honestly.
        if self.cancel.is_cancelled() {
            self.store.mark_mission_run_cancelled(&self.run_id).await?;
        } else {
            self.store.mark_mission_ended(&self.run_id).await?;
        }
        Ok(())
    }

    /// Persist one transition: the in-memory attempt plus the settled
    /// fields ride one struct into the store.
    async fn persist(&self, node_id: &str, t: MissionTaskTransition) -> Result<()> {
        self.store
            .set_mission_task_status(&self.run_id, node_id, t)
            .await
    }

    fn broadcast(&self, node: Option<&MissionNode>, t: &MissionTaskState) {
        let _ = self.events.send(json!({
            "event": "mission.progress",
            "runId": self.run_id,
            "node": node.map(|n| serde_json::to_value(n).unwrap_or(Value::Null)).unwrap_or(Value::Null),
            "status": t.status.as_str(),
            "attempt": t.attempt,
        }));
    }
}

/// One persisted task row: (node_id, status, attempt, output, error).
pub type MissionTaskRow = (String, String, u32, Option<String>, Option<String>);

/// Derived run status - always computed from task states, never stored.
pub fn derive_run_status(
    tasks: &[MissionTaskRow],
    interrupted: bool,
    ended: bool,
    cancelled: bool,
) -> &'static str {
    if interrupted {
        return "interrupted";
    }
    if cancelled {
        return "cancelled";
    }
    let terminal = |s: &str| matches!(s, "succeeded" | "failed" | "cancelled" | "excluded");
    if tasks.iter().all(|t| terminal(&t.1)) {
        if !ended {
            return "running";
        }
        if tasks.iter().any(|t| t.1 == "failed" || t.1 == "excluded") {
            return "failed";
        }
        return "succeeded";
    }
    "running"
}
