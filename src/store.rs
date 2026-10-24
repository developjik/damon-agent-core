use rusqlite::OptionalExtension;
use std::path::Path;

use anyhow::Context;
use serde_json::Value;
use tokio_rusqlite::Connection;

/// SQLite-backed session/message store. Append-only message log with
/// FTS search, usage rows, and compaction-aware reads.
#[derive(Clone)]
pub struct Store {
    conn: Connection,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StoredMessage {
    pub id: i64,
    pub session_id: String,
    pub role: String,
    /// Unix-ms stamp of when the row was persisted. Rows written
    /// before the column existed carry 0 = unknown; time-filtered
    /// search treats those as the session's `created_at` — the
    /// coarsest bound the old schema could express.
    pub ts: i64,
    /// OpenAI-shaped message JSON: content, tool_calls, tool_call_id, name.
    pub data: Value,
}

/// One `session.list` row: identity plus the metadata the list
/// filters on and `session.export` re-uses as its header. `backend`
/// and `title` are empty strings when unset, matching the tuple
/// shape the wire rows exposed before this struct existed.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionSummary {
    pub id: String,
    pub created_at: String,
    /// Agent id backing the session ("" when unset).
    pub backend: String,
    pub title: String,
    /// Working directory the session's tools run in.
    pub cwd: String,
    /// Parsed from the tags column; an unreadable cell reads as no
    /// tags rather than failing the whole listing.
    pub tags: Vec<String>,
    /// The session's stored default model (None = backend default).
    pub model: Option<String>,
    /// Pinned sessions float to the front of listings and are exempt
    /// from the retention sweep.
    pub pinned: bool,
    /// Archived sessions are hidden from default listings and exempt
    /// from the retention sweep — kept for reference, not workflow.
    pub archived: bool,
}

/// Narrowing for `list_sessions_paged`. All fields optional; set
/// fields AND together. `backend` matches the agent id exactly,
/// `cwd` the stored working directory exactly, `tag` membership in
/// the session's tag list. Archived sessions are excluded unless
/// `include_archived` is set.
#[derive(Debug, Default)]
pub struct SessionFilter {
    pub backend: Option<String>,
    pub cwd: Option<String>,
    pub tag: Option<String>,
    pub project_id: Option<String>,
    pub include_archived: bool,
}

/// One projects row. `defaults` is the raw JSON string
/// ({backend?, model?, mode?, mcpServers?}) — parsed at the RPC layer.
/// `kind` distinguishes plain projects from worktree workspaces
/// registered by `worktree.create`; a worktree row carries its git
/// facts in `meta` ({branch, prNumber?, ...}) and points at the repo's
/// own project via `parent_id`.
#[derive(Debug, Clone)]
pub struct ProjectRow {
    pub id: String,
    pub name: String,
    pub root: String,
    pub defaults: String,
    pub kind: String,
    pub parent_id: Option<String>,
    pub meta: String,
}

/// Row mapper shared by every projects SELECT (column order:
/// id, name, root, defaults, kind, parent_id, meta).
fn project_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ProjectRow> {
    Ok(ProjectRow {
        id: r.get(0)?,
        name: r.get(1)?,
        root: r.get(2)?,
        defaults: r.get(3)?,
        kind: r.get(4)?,
        parent_id: r.get(5)?,
        meta: r.get(6)?,
    })
}

/// Narrowing for full-text search. `since_ms`/`until_ms` are unix
/// milliseconds, inclusive — the RPC layer parses wire timestamps
/// (RFC3339 or date-only) into them; the store stays a pure-ms API.
#[derive(Debug, Default)]
pub struct SearchFilter {
    pub session_id: Option<String>,
    pub backend: Option<String>,
    pub cwd: Option<String>,
    pub project_id: Option<String>,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
}

/// One (local day, backend, model) group of `usage_aggregate`.
#[derive(Debug, Clone)]
pub struct UsageDayRow {
    /// Local day, `YYYY-MM-DD` (UTC stamps shifted by the caller's
    /// offset).
    pub date: String,
    /// Owning session's agent id ("" when unset).
    pub backend: String,
    pub model: String,
    /// Usage rows in the group (one per reported turn).
    pub turns: u64,
    /// Sum of the per-turn context snapshots.
    pub context_used: u64,
    pub context_size: u64,
    /// Sum of the per-turn cost deltas.
    pub cost_usd: f64,
}

/// `usage_aggregate` result: grouped rows plus the window total.
#[derive(Debug, Clone)]
pub struct UsageAggregate {
    /// Day/backend/model groups, ordered by date then backend then
    /// model.
    pub rows: Vec<UsageDayRow>,
    pub turns: u64,
    pub context_used: u64,
    pub context_size: u64,
    pub cost_usd: f64,
}

/// One granted root: an extra directory a user widened sessions to
/// (upstream desktop-cc-gui's grant_root). Stored canonically
/// (resolved, absolute) so jail comparisons are exact string compares.
#[derive(Debug, Clone)]
pub struct GrantedRoot {
    /// Project scope; None = global (every session).
    pub project_id: Option<String>,
    /// Canonical directory.
    pub root: String,
    pub created_at: String,
}

/// One durable plan-review revision (the approval fact-source for
/// `kind=plan` asks). Identity = session + planId + revision; a
/// re-asked plan chains onto the same planId with revision+1.
#[derive(Debug, Clone)]
pub struct PlanRow {
    pub plan_id: String,
    pub session_id: String,
    /// The live permission ask this revision answers.
    pub request_id: String,
    pub revision: i64,
    pub title: String,
    /// The verbatim plan text the user reviews.
    pub content: String,
    /// Non-cryptographic content fingerprint — detects text drift
    /// between the record and a late submit.
    pub content_hash: String,
    /// false = no reviewable plan text: every approval refuses.
    pub complete: bool,
    /// awaiting_review | submitting | approved | changes_requested |
    /// deferred | cancelled | expired | superseded.
    pub status: String,
    /// The recorded decision intent {decision, feedback?} — persisted
    /// before the native relay, kept when the relay fails.
    pub decision: Option<Value>,
    pub decision_intent_at: Option<i64>,
    /// When the native ack settled the decision; None = unconfirmed.
    pub applied_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    /// Revision that replaced this one (open rows only).
    pub superseded_by: Option<i64>,
}

/// `submit_plan_decision` outcome: the CAS claim-or-conflict contract.
#[derive(Debug)]
pub enum PlanSubmit {
    /// This caller owns the submission: the row is `submitting` and
    /// the decision intent is durably recorded. Carries what the
    /// native relay needs (the live ask's session and request ids)
    /// and the pre-claim status for a revert.
    Claimed {
        session_id: String,
        request_id: String,
        previous: String,
    },
    /// Lost the race or answered a closed/stale row — the current row
    /// is returned so the caller can present what actually happened.
    Conflict(Box<PlanRow>),
    /// No row for this planId + expectedRevision.
    NotFound,
    /// Approving a plan whose content is not reviewable.
    Incomplete,
    /// `request_changes` without non-empty feedback.
    FeedbackRequired,
}

/// One paired web device: a remote surface with its own revokable
/// credential. `pending` rows mint nothing — `approved_at` is what
/// lets the device token through ws_ticket, and only a trusted surface
/// sets it.
#[derive(Debug, Clone)]
pub struct DeviceRow {
    pub id: String,
    pub name: String,
    /// pending | approved
    pub status: String,
    pub created_at: String,
    pub approved_at: Option<String>,
    /// Throttled: updated at most once a minute by ticket mints.
    pub last_seen: Option<String>,
}

/// A sanitized search, split across two engines by term length.
#[derive(Debug, PartialEq)]
struct FtsQuery {
    /// FTS5 MATCH expression — every term in it is >= 3 chars (the
    /// trigram minimum). Empty when every term went to the LIKE path.
    match_expr: String,
    /// 1–2 char terms, applied as exact `%term%` substring conjuncts
    /// over the indexed content. The trigram tokenizer only indexes
    /// 3-char windows, so MATCH would silently drop these (an empty
    /// result for e.g. `커밋`).
    like_terms: Vec<String>,
    /// Same, for terms under NOT: exclusion conjuncts
    /// (`content NOT LIKE '%term%'`).
    like_not_terms: Vec<String>,
}

/// The searchable text of one stored message, for the FTS index:
/// `content` text (plain string, text parts of an array, or nested ACP
/// `content` blocks) plus tool I/O — OpenAI-style `tool_calls` names
/// and arguments on assistant messages, any `toolCall` title/rawInput,
/// and JSON-stringified payloads for tool results with no extractable
/// text. Image data is deliberately skipped — indexing base64 would
/// bloat the index without ever matching a query. Returns None when
/// nothing searchable remains.
fn fts_text(role: &str, data: &Value) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    match &data["content"] {
        Value::String(s) => parts.push(s.clone()),
        Value::Array(items) => {
            for p in items {
                if p["type"] == "text"
                    && let Some(t) = p["text"].as_str()
                {
                    parts.push(t.to_string());
                } else if p["type"] == "content"
                    && let Some(t) = p["content"]["text"].as_str()
                {
                    // ACP tool_call_update content blocks wrap the text
                    // one level deeper: {type:"content", content:{text}}.
                    parts.push(t.to_string());
                }
            }
        }
        _ => {}
    }
    if role == "assistant" {
        for tc in data["tool_calls"].as_array().into_iter().flatten() {
            let f = &tc["function"];
            if let Some(name) = f["name"].as_str() {
                parts.push(name.to_string());
            }
            if let Some(args) = f["arguments"].as_str() {
                parts.push(args.to_string());
            }
        }
        if let Some(title) = data["toolCall"]["title"].as_str() {
            parts.push(title.to_string());
        }
        let raw = &data["toolCall"]["rawInput"];
        if let Some(s) = raw.as_str() {
            parts.push(s.to_string());
        } else if !raw.is_null() {
            parts.push(raw.to_string());
        }
    }
    // A tool result with no extractable text (object/number payload)
    // still gets its compact JSON indexed — the output is what users
    // search for.
    if role == "tool"
        && !matches!(&data["content"], Value::String(_) | Value::Array(_))
        && !data["content"].is_null()
    {
        parts.push(data["content"].to_string());
    }
    let text = parts.join("\n");
    (!text.trim().is_empty()).then_some(text)
}

/// Index every message that `messages_fts` is missing, using the
/// current `fts_text` extractor. Shared by the v3 coverage migration
/// and the v4 trigram rebuild (whose fresh table is missing
/// everything).
fn fts_reindex_missing(c: &rusqlite::Connection) -> rusqlite::Result<()> {
    let missing: Vec<(i64, String, String, String)> = {
        let mut stmt = c.prepare(
            "SELECT id, session_id, role, data FROM messages
             WHERE id NOT IN (SELECT message_id FROM messages_fts)",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
        rows.collect::<Result<_, _>>()?
    };
    for (id, sid, role, data) in missing {
        let Ok(v) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        if let Some(text) = fts_text(&role, &v) {
            c.execute(
                "INSERT INTO messages_fts (content, session_id, message_id)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![text, sid, id],
            )?;
        }
    }
    Ok(())
}

/// Manual snippet for the pure-LIKE path — `snippet()` is an FTS5
/// auxiliary function that requires a MATCH, which a pure-LIKE query
/// has none of. Same output shape as the SQL one: the first matched
/// term wrapped in char(1)/char(2) markers, `…` where text was cut,
/// ±48 chars of context (the SQL version's 32-token window). Matching
/// mirrors SQLite LIKE: ASCII-case-insensitive, exact otherwise. With
/// no positive term (exclusion-only), the content head stands in.
fn like_snippet(content: &str, terms: &[String]) -> String {
    const WINDOW: usize = 48;
    let bytes = content.as_bytes();
    let mut hit: Option<(usize, usize)> = None; // (byte start, term len)
    for t in terms {
        if let Some(pos) = bytes
            .windows(t.len())
            .position(|w| w.eq_ignore_ascii_case(t.as_bytes()))
            .filter(|&p| content.is_char_boundary(p))
            && hit.is_none_or(|(s, _)| pos < s)
        {
            hit = Some((pos, t.len()));
        }
    }
    let Some((start, tlen)) = hit else {
        // No highlightable occurrence — emit the head so the caller
        // still sees what matched.
        let mut end = content.len().min(2 * WINDOW);
        while end < content.len() && !content.is_char_boundary(end) {
            end += 1;
        }
        return format!("{}…", &content[..end]);
    };
    let term_end = start + tlen;
    let mut b = start.saturating_sub(WINDOW);
    while b > 0 && !content.is_char_boundary(b) {
        b -= 1;
    }
    let mut e = (term_end + WINDOW).min(content.len());
    while e < content.len() && !content.is_char_boundary(e) {
        e += 1;
    }
    let mut out = String::new();
    if b > 0 {
        out.push('…');
    }
    out.push_str(&content[b..start]);
    out.push('\u{1}');
    out.push_str(&content[start..term_end]);
    out.push('\u{2}');
    out.push_str(&content[term_end..e]);
    if e < content.len() {
        out.push('…');
    }
    out
}

/// Wall-clock unix ms for plan-review stamps.
fn plan_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Non-cryptographic content fingerprint for plan text — detects drift
/// between revisions and between record and submit (SipHash via the
/// standard library; no new dependency, per upstream).
fn plan_content_hash(content: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    content.len().hash(&mut hasher);
    content.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Force `mode` on `path`. Session transcripts and tool I/O must not be
/// group/world-accessible regardless of the process umask (SQLite opens
/// 0644 by default) — same threat model as the config file's 0600
/// warning. Runs on every open: a fresh path gets the tight mode, a
/// looser pre-existing one is tightened with a warning; failures only
/// warn so the store still opens.
#[cfg(unix)]
pub(crate) fn tighten_permissions(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    use tracing::warn;
    let was_looser = std::fs::metadata(path)
        .map(|m| m.permissions().mode() & 0o777 & !mode != 0)
        .unwrap_or(false);
    match std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)) {
        Ok(()) if was_looser => {
            warn!(
                path = %path.display(),
                "store path was group/world-accessible; tightened permissions"
            );
        }
        Err(e) => {
            warn!(error = %e, path = %path.display(), "cannot tighten store permissions");
        }
        Ok(()) => {}
    }
}

/// Current unix time in milliseconds — the `messages.ts` stamp. A
/// clock reading before the epoch yields 0 (unknown), same as
/// pre-column rows: it cannot happen on a sane host and would cost
/// nothing but search-bounds precision.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Per-cwd ceiling on `prompt_history` rows — the trim target in
/// `record_prompt`, and the ceiling on `prompt.recent`'s limit.
pub(crate) const PROMPT_HISTORY_CAP: i64 = 200;

/// One persisted mission task transition (status plus optional payload
/// and timestamp stamps).
pub struct MissionTaskTransition {
    pub status: String,
    /// Current attempt number (written on every transition).
    pub attempt: u32,
    pub output: Option<String>,
    pub error: Option<String>,
    pub stamp_started: bool,
    pub stamp_ended: bool,
}

impl Store {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
            // Transcripts are sensitive: force a private data dir even
            // when the umask is permissive (create_dir_all applies it).
            #[cfg(unix)]
            tighten_permissions(parent, 0o700);
        }
        let conn = Connection::open(path)
            .await
            .with_context(|| format!("cannot open store {}", path.display()))?;
        // Same for the database file itself; must run after open so the
        // file exists even on first launch.
        #[cfg(unix)]
        tighten_permissions(path, 0o600);
        conn.call(|c| {
            c.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA busy_timeout = 5000;
                 PRAGMA foreign_keys = ON;
                CREATE TABLE IF NOT EXISTS sessions (
                     id TEXT PRIMARY KEY,
                     created_at TEXT NOT NULL DEFAULT (datetime('now')),
                     last_active_at TEXT,
                     cwd TEXT NOT NULL DEFAULT '',
                     model TEXT,
                     agent TEXT,
                     agent_session TEXT,
                     compacted_through INTEGER NOT NULL DEFAULT 0,
                     summary TEXT,
                     title TEXT,
                     tags TEXT NOT NULL DEFAULT '[]',
                     pinned INTEGER NOT NULL DEFAULT 0,
                     archived INTEGER NOT NULL DEFAULT 0,
                     project_id TEXT
                 );
                 CREATE TABLE IF NOT EXISTS messages (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     session_id TEXT NOT NULL REFERENCES sessions(id),
                     role TEXT NOT NULL,
                     data TEXT NOT NULL,
                     ts INTEGER NOT NULL DEFAULT 0
                 );
                 CREATE INDEX IF NOT EXISTS idx_messages_session
                     ON messages(session_id, id);
                 -- trigram: the default unicode61 tokenizer folds a run
                 -- of CJK characters into ONE token, so a substring
                 -- query like 커밋 could never match 커밋 메시지.
                 -- Trigram (SQLite >= 3.34; the bundled build ships
                 -- 3.4x) indexes every 3-char window instead, making
                 -- MATCH a substring search. Pre-existing tables are
                 -- re-tokenized by the v4 migration below.
                 CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                     content,
                     session_id UNINDEXED,
                     message_id UNINDEXED,
                     tokenize = 'trigram'
                 );
                CREATE TABLE IF NOT EXISTS usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    model TEXT NOT NULL DEFAULT '',
                    context_used INTEGER NOT NULL DEFAULT 0,
                    context_size INTEGER NOT NULL DEFAULT 0,
                    cost_usd REAL NOT NULL DEFAULT 0,
                    cumulative_cost REAL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS channel_state (
                    conv_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (conv_id, key)
                );
                CREATE TABLE IF NOT EXISTS prompt_history (
                    cwd TEXT NOT NULL,
                    text TEXT NOT NULL,
                    use_count INTEGER NOT NULL DEFAULT 1,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (cwd, text)
                );
                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    root TEXT NOT NULL,
                    -- JSON: {backend?, model?, mode?, mcpServers?}
                    defaults TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    -- 'project' | 'worktree' (worktree.create rows);
                    -- meta mirrors the git facts: {branch, baseRef?,
                    -- prNumber?, prTitle?, prUrl?, repo}
                    kind TEXT NOT NULL DEFAULT 'project',
                    parent_id TEXT,
                    meta TEXT NOT NULL DEFAULT '{}'
                );
                 CREATE INDEX IF NOT EXISTS idx_usage_session
                     ON usage(session_id, id);
                 -- agent/agent_session serves the import-dedup lookup
                 -- and the list/search backend filter; usage.created_at
                 -- serves the daily rollup's day-range scan.
                 CREATE INDEX IF NOT EXISTS idx_sessions_agent
                     ON sessions(agent, agent_session);
                 CREATE INDEX IF NOT EXISTS idx_usage_created
                     ON usage(created_at);
                 -- Granted roots (see the v5 migration): extra
                 -- directories the user widened sessions to. NULL
                 -- project_id = global (every session); a project row
                 -- scopes the grant to that project's sessions. The
                 -- FK cascades — grants die with their project.
                 CREATE TABLE IF NOT EXISTS granted_roots (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
                     root TEXT NOT NULL,
                     created_at TEXT NOT NULL DEFAULT (datetime('now')),
                     UNIQUE(project_id, root)
                 );
                 -- Durable plan-review rows (see the v6 migration): the
                 -- approval fact-source for kind=plan permission asks.
                 CREATE TABLE IF NOT EXISTS plan_reviews (
                     plan_id TEXT NOT NULL,
                     session_id TEXT NOT NULL,
                     request_id TEXT NOT NULL,
                     revision INTEGER NOT NULL,
                     title TEXT NOT NULL DEFAULT '',
                     content TEXT NOT NULL DEFAULT '',
                     content_hash TEXT NOT NULL DEFAULT '',
                     complete INTEGER NOT NULL DEFAULT 0,
                     status TEXT NOT NULL DEFAULT 'awaiting_review',
                     decision TEXT,
                     decision_intent_at INTEGER,
                     applied_at INTEGER,
                     created_at INTEGER NOT NULL,
                     updated_at INTEGER NOT NULL,
                     superseded_by INTEGER,
                     PRIMARY KEY (plan_id, revision)
                 );
                 CREATE INDEX IF NOT EXISTS idx_plan_reviews_session
                     ON plan_reviews(session_id, plan_id, revision);
                 CREATE INDEX IF NOT EXISTS idx_plan_reviews_request
                     ON plan_reviews(request_id);",
            )?;
            // Migrate pre-compaction databases. Check column existence
            // first — swallowing every ALTER error would hide real
            // failures (locked/corrupt db) behind the duplicate-column case.
            let cols: std::collections::HashSet<String> = c
                .prepare("PRAGMA table_info(sessions)")?
                .query_map([], |r| r.get::<_, String>(1))?
                .collect::<Result<_, _>>()?;
            for (col, ddl) in [
                (
                    "compacted_through",
                    "ALTER TABLE sessions ADD COLUMN compacted_through INTEGER NOT NULL DEFAULT 0",
                ),
                ("summary", "ALTER TABLE sessions ADD COLUMN summary TEXT"),
                ("model", "ALTER TABLE sessions ADD COLUMN model TEXT"),
                (
                    "last_active_at",
                    "ALTER TABLE sessions ADD COLUMN last_active_at TEXT",
                ),
                ("title", "ALTER TABLE sessions ADD COLUMN title TEXT"),
                ("agent", "ALTER TABLE sessions ADD COLUMN agent TEXT"),
                (
                    "agent_session",
                    "ALTER TABLE sessions ADD COLUMN agent_session TEXT",
                ),
                (
                    "tags",
                    "ALTER TABLE sessions ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'",
                ),
                (
                    "pinned",
                    "ALTER TABLE sessions ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "archived",
                    "ALTER TABLE sessions ADD COLUMN archived INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "project_id",
                    "ALTER TABLE sessions ADD COLUMN project_id TEXT",
                ),
            ] {
                if !cols.contains(col)
                    && let Err(e) = c.execute_batch(ddl)
                {
                    // Two damond processes opening the same DB can both
                    // pass the existence check — the loser's ALTER fails
                    // with duplicate-column. Re-check rather than
                    // swallowing every error (locked/corrupt must fail).
                    let now: std::collections::HashSet<String> = c
                        .prepare("PRAGMA table_info(sessions)")?
                        .query_map([], |r| r.get::<_, String>(1))?
                        .collect::<Result<_, _>>()?;
                    if !now.contains(col) {
                        return Err(e.into());
                    }
                }
            }
            // Same column-existence migration for the usage table's
            // context/cost columns added after the ACP pivot.
            let ucols: std::collections::HashSet<String> = c
                .prepare("PRAGMA table_info(usage)")?
                .query_map([], |r| r.get::<_, String>(1))?
                .collect::<Result<_, _>>()?;
            for (col, ddl) in [
                (
                    "context_used",
                    "ALTER TABLE usage ADD COLUMN context_used INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "context_size",
                    "ALTER TABLE usage ADD COLUMN context_size INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "cumulative_cost",
                    "ALTER TABLE usage ADD COLUMN cumulative_cost REAL",
                ),
            ] {
                if !ucols.contains(col)
                    && let Err(e) = c.execute_batch(ddl)
                {
                    let now: std::collections::HashSet<String> = c
                        .prepare("PRAGMA table_info(usage)")?
                        .query_map([], |r| r.get::<_, String>(1))?
                        .collect::<Result<_, _>>()?;
                    if !now.contains(col) {
                        return Err(e.into());
                    }
                }
            }
            // Message timestamps (P2): same column-existence pattern.
            // Pre-column rows keep ts = 0 = unknown — there is no
            // honest stamp to backfill, and time-filtered search
            // clamps them to the session's created_at (the bound the
            // pre-ts schema could express) instead of dropping them.
            let mcols: std::collections::HashSet<String> = c
                .prepare("PRAGMA table_info(messages)")?
                .query_map([], |r| r.get::<_, String>(1))?
                .collect::<Result<_, _>>()?;
            for (col, ddl) in [(
                "ts",
                "ALTER TABLE messages ADD COLUMN ts INTEGER NOT NULL DEFAULT 0",
            )] {
                if !mcols.contains(col)
                    && let Err(e) = c.execute_batch(ddl)
                {
                    // Same concurrent-damond duplicate-column re-check.
                    let now: std::collections::HashSet<String> = c
                        .prepare("PRAGMA table_info(messages)")?
                        .query_map([], |r| r.get::<_, String>(1))?
                        .collect::<Result<_, _>>()?;
                    if !now.contains(col) {
                        return Err(e.into());
                    }
                }
            }
            // Worktree workspaces (same column-existence pattern): a
            // project row can now be kind='worktree' with a parent
            // project and git metadata. Existing rows default to plain
            // projects, which is what they always were.
            let pcols: std::collections::HashSet<String> = c
                .prepare("PRAGMA table_info(projects)")?
                .query_map([], |r| r.get::<_, String>(1))?
                .collect::<Result<_, _>>()?;
            for (col, ddl) in [
                (
                    "kind",
                    "ALTER TABLE projects ADD COLUMN kind TEXT NOT NULL DEFAULT 'project'",
                ),
                (
                    "parent_id",
                    "ALTER TABLE projects ADD COLUMN parent_id TEXT",
                ),
                (
                    "meta",
                    "ALTER TABLE projects ADD COLUMN meta TEXT NOT NULL DEFAULT '{}'",
                ),
            ] {
                if !pcols.contains(col)
                    && let Err(e) = c.execute_batch(ddl)
                {
                    // Same concurrent-damond duplicate-column re-check.
                    let now: std::collections::HashSet<String> = c
                        .prepare("PRAGMA table_info(projects)")?
                        .query_map([], |r| r.get::<_, String>(1))?
                        .collect::<Result<_, _>>()?;
                    if !now.contains(col) {
                        return Err(e.into());
                    }
                }
            }
            // Backfill the FTS index once for messages written before it
            // existed; user_version gates it so startup stays O(1).
            let v: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
            if v == 0 {
                c.execute_batch(
                    "INSERT INTO messages_fts (content, session_id, message_id)
                     SELECT json_extract(m.data, '$.content'), m.session_id, m.id
                     FROM messages m
                     WHERE json_type(m.data, '$.content') = 'text'
                       AND m.id NOT IN (SELECT message_id FROM messages_fts);
                     PRAGMA user_version = 1;",
                )?;
            }
            // Usage integrity (user_version 2): ACP `usage_update` cost
            // is CUMULATIVE per agent session, but rows stored it raw,
            // so SUM(cost_usd) over-counted every intermediate value.
            // Old rows are rewritten once into per-turn deltas with the
            // raw cumulative kept in `cumulative_cost`; new rows are
            // written as deltas directly (see `record_usage`). The
            // `cumulative_cost IS NULL` marker makes the rewrite
            // idempotent — a re-run (crash mid-migration, two damonds)
            // finds nothing left to convert. The never-written
            // input_tokens/output_tokens columns go away too: agents
            // send no token counts, and bundled SQLite (3.4x) supports
            // DROP COLUMN (3.35+).
            if v < 2 {
                for (col, ddl) in [
                    ("input_tokens", "ALTER TABLE usage DROP COLUMN input_tokens"),
                    (
                        "output_tokens",
                        "ALTER TABLE usage DROP COLUMN output_tokens",
                    ),
                ] {
                    let cols: std::collections::HashSet<String> = c
                        .prepare("PRAGMA table_info(usage)")?
                        .query_map([], |r| r.get::<_, String>(1))?
                        .collect::<Result<_, _>>()?;
                    if cols.contains(col)
                        && let Err(e) = c.execute_batch(ddl)
                    {
                        // Same concurrent-damond re-check as the column
                        // adds above: the loser's DROP fails with
                        // no-such-column once the winner dropped it;
                        // locked/corrupt db must still fail.
                        let now: std::collections::HashSet<String> = c
                            .prepare("PRAGMA table_info(usage)")?
                            .query_map([], |r| r.get::<_, String>(1))?
                            .collect::<Result<_, _>>()?;
                        if now.contains(col) {
                            return Err(e.into());
                        }
                    }
                }
                c.execute_batch(
                    "UPDATE usage SET
                         cumulative_cost = c.cum,
                         cost_usd = CASE
                             WHEN c.prev IS NULL OR c.cum < c.prev THEN c.cum
                             ELSE c.cum - c.prev
                         END
                     FROM (
                         SELECT id, cost_usd AS cum,
                                LAG(cost_usd) OVER (
                                    PARTITION BY session_id, model ORDER BY id) AS prev
                         FROM usage WHERE cumulative_cost IS NULL
                     ) AS c
                     WHERE usage.id = c.id AND usage.cumulative_cost IS NULL;
                     PRAGMA user_version = 2;",
                )?;
            }
            // FTS coverage (user_version 3): the v1 backfill indexed
            // only rows whose `content` was a JSON string — array-form
            // content (text parts, nested ACP blocks, tool payloads)
            // never entered the index even though `fts_text` handles
            // them. Re-run the current extractor over every message
            // missing from the index. Idempotent by construction (the
            // NOT IN filter skips indexed rows), so a crash mid-run
            // just re-runs on next open.
            if v < 3 {
                fts_reindex_missing(c)?;
                c.execute_batch("PRAGMA user_version = 3;")?;
            }
            // CJK substring search (user_version 4): the default
            // unicode61 tokenizer folds a whole run of CJK characters
            // into ONE token, so a query like "커밋" never matched
            // "커밋 메시지를 남겼다". The trigram tokenizer (SQLite
            // >= 3.34; the bundled build ships 3.4x, so no dependency
            // churn) indexes every 3-character window instead, making
            // MATCH a substring search. The tokenizer is baked into
            // the shadow index, and this is a plain-content FTS table
            // (it stores its own copy of the text — `rebuild` re-reads
            // that copy, it cannot re-tokenize it), so the table is
            // dropped, recreated with trigram, and every message
            // re-indexed through the current `fts_text` extractor.
            // Crash-safe: DROP IF EXISTS + CREATE re-run cleanly, the
            // reindex is idempotent, and user_version flips only at
            // the end.
            if v < 4 {
                c.execute_batch(
                    "DROP TABLE IF EXISTS messages_fts;
                     CREATE VIRTUAL TABLE messages_fts USING fts5(
                         content,
                         session_id UNINDEXED,
                         message_id UNINDEXED,
                         tokenize = 'trigram'
                     );",
                )?;
                fts_reindex_missing(c)?;
                c.execute_batch("PRAGMA user_version = 4;")?;
            }
            // Granted roots (user_version 5): persisted extra
            // directories a user granted to sessions (upstream
            // desktop-cc-gui's grant_root). Scope: per-project, with
            // NULL meaning global — every session. Projects carry the
            // workspace root (upstream's "workspace" scope), but bot
            // surfaces (chat channels) have no project context, so a
            // global grant is the loop-closer there; the jail join
            // stays one predicate: project_id IS NULL OR matches the
            // session's project. FK cascades on project delete.
            if v < 5 {
                c.execute_batch(
                    "CREATE TABLE IF NOT EXISTS granted_roots (
                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                         project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
                         root TEXT NOT NULL,
                         created_at TEXT NOT NULL DEFAULT (datetime('now')),
                         UNIQUE(project_id, root)
                     );
                     PRAGMA user_version = 5;",
                )?;
            }
            // Plan reviews (user_version 6): the durable approval
            // artifact for kind=plan asks (upstream desktop-cc-gui's
            // plan_review). Identity = session + planId + revision;
            // a re-ask while the previous revision is still open
            // chains as revision+1 and supersedes it. Status machine:
            // awaiting_review → submitting → approved |
            // changes_requested | deferred (non-terminal); terminal
            // cancelled | expired | superseded. The decision intent is
            // persisted BEFORE the native relay write, so a lost pipe
            // or crash leaves the record; `submitting` reverts on a
            // failed relay and settles on an ack.
            if v < 6 {
                c.execute_batch(
                    "CREATE TABLE IF NOT EXISTS plan_reviews (
                         plan_id TEXT NOT NULL,
                         session_id TEXT NOT NULL,
                         request_id TEXT NOT NULL,
                         revision INTEGER NOT NULL,
                         title TEXT NOT NULL DEFAULT '',
                         content TEXT NOT NULL DEFAULT '',
                         content_hash TEXT NOT NULL DEFAULT '',
                         complete INTEGER NOT NULL DEFAULT 0,
                         status TEXT NOT NULL DEFAULT 'awaiting_review',
                         decision TEXT,
                         decision_intent_at INTEGER,
                         applied_at INTEGER,
                         created_at INTEGER NOT NULL,
                         updated_at INTEGER NOT NULL,
                         superseded_by INTEGER,
                         PRIMARY KEY (plan_id, revision)
                     );
                     CREATE INDEX IF NOT EXISTS idx_plan_reviews_session
                         ON plan_reviews(session_id, plan_id, revision);
                     CREATE INDEX IF NOT EXISTS idx_plan_reviews_request
                         ON plan_reviews(request_id);
                     PRAGMA user_version = 6;",
                )?;
            }

            // Web device pairing (user_version 7): per-device
            // credentials for remote surfaces. A daemon-minted one-time
            // pairing key (single row — exactly one active key, like
            // upstream) is consumed+rotated in one locked step by the
            // pair request, which creates the pending device row;
            // `approved_at` (set only by a trusted surface) is what
            // lets its token through ws_ticket. Tokens are stored
            // SHA-256 hashed — the daemon never persists the usable
            // secret, so a store leak cannot mint tickets.
            if v < 7 {
                c.execute_batch(
                    "CREATE TABLE IF NOT EXISTS web_pairing (
                         id INTEGER PRIMARY KEY CHECK (id = 1),
                         key TEXT NOT NULL,
                         created_at TEXT NOT NULL DEFAULT (datetime('now'))
                     );
                     CREATE TABLE IF NOT EXISTS web_devices (
                         id TEXT PRIMARY KEY,
                         name TEXT NOT NULL DEFAULT '',
                         token_hash TEXT NOT NULL UNIQUE,
                         status TEXT NOT NULL DEFAULT 'pending',
                         created_at TEXT NOT NULL DEFAULT (datetime('now')),
                         approved_at TEXT,
                         last_seen TEXT
                     );
                     PRAGMA user_version = 7;",
                )?;
            }

            // Native-history scanner state (user_version 8): the stat
            // signature (size + mtime) per discovered native file keyed
            // to the damon session it produced — unchanged signature
            // skips the re-parse entirely. Tombstones record explicit
            // deletes: a deleted session's native handle is never
            // re-imported by a later sweep, and a resume that binds the
            // same handle (the user's explicit new intent) clears it.
            if v < 8 {
                c.execute_batch(
                    "CREATE TABLE IF NOT EXISTS scanned_native (
                         backend TEXT NOT NULL,
                         native_handle TEXT NOT NULL,
                         size INTEGER,
                         mtime_ms INTEGER,
                         session_id TEXT NOT NULL,
                         scanned_at TEXT NOT NULL DEFAULT (datetime('now')),
                         PRIMARY KEY (backend, native_handle)
                     );
                     CREATE TABLE IF NOT EXISTS session_tombstones (
                         backend TEXT NOT NULL,
                         native_handle TEXT NOT NULL,
                         created_at TEXT NOT NULL DEFAULT (datetime('now')),
                         PRIMARY KEY (backend, native_handle)
                     );
                     PRAGMA user_version = 8;",
                )?;
            }

            // Mission DAG runner (user_version 9): versioned flow
            // definitions, runs with deep-cloned snapshots, per-node
            // task instances, and the human inbox. Run status is always
            // derived from task states — never a stale stored field.
            if v < 9 {
                c.execute_batch(
                    "CREATE TABLE IF NOT EXISTS mission_flows (
                         flow_id TEXT NOT NULL,
                         version INTEGER NOT NULL,
                         name TEXT NOT NULL DEFAULT '',
                         definition TEXT NOT NULL,
                         created_at TEXT NOT NULL DEFAULT (datetime('now')),
                         PRIMARY KEY (flow_id, version)
                     );
                     CREATE TABLE IF NOT EXISTS mission_runs (
                         run_id TEXT PRIMARY KEY,
                         flow_id TEXT NOT NULL,
                         version INTEGER NOT NULL,
                         snapshot TEXT NOT NULL,
                         interrupted INTEGER NOT NULL DEFAULT 0,
                         created_at TEXT NOT NULL DEFAULT (datetime('now')),
                         ended_at TEXT,
                         cancelled INTEGER NOT NULL DEFAULT 0
                     );
                     CREATE TABLE IF NOT EXISTS mission_tasks (
                         task_id TEXT PRIMARY KEY,
                         run_id TEXT NOT NULL,
                         node_id TEXT NOT NULL,
                         status TEXT NOT NULL DEFAULT 'queued',
                         attempt INTEGER NOT NULL DEFAULT 1,
                         output TEXT,
                         error TEXT,
                         started_at TEXT,
                         ended_at TEXT
                     );
                     CREATE INDEX IF NOT EXISTS idx_mission_tasks_run
                         ON mission_tasks(run_id, node_id);
                     CREATE TABLE IF NOT EXISTS mission_inbox (
                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                         task_id TEXT NOT NULL,
                         run_id TEXT NOT NULL,
                         node_id TEXT NOT NULL,
                         status TEXT NOT NULL DEFAULT 'pending',
                         decision TEXT,
                         feedback TEXT,
                         created_at TEXT NOT NULL DEFAULT (datetime('now')),
                         resolved_at TEXT
                     );
                     PRAGMA user_version = 9;",
                )?;
            }
            Ok::<(), rusqlite::Error>(()).map_err(tokio_rusqlite::Error::from)
        })
        .await?;
        Ok(Self { conn })
    }

    /// In-memory store for tests.
    pub async fn in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory().await?;
        conn.call(|c| {
            c.execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE sessions (
                     id TEXT PRIMARY KEY,
                     created_at TEXT NOT NULL DEFAULT (datetime('now')),
                     last_active_at TEXT,
                     cwd TEXT NOT NULL DEFAULT '',
                     model TEXT,
                     agent TEXT,
                     agent_session TEXT,
                     compacted_through INTEGER NOT NULL DEFAULT 0,
                     summary TEXT,
                     title TEXT,
                     tags TEXT NOT NULL DEFAULT '[]',
                     pinned INTEGER NOT NULL DEFAULT 0,
                     archived INTEGER NOT NULL DEFAULT 0,
                     project_id TEXT
                 );
                 CREATE TABLE messages (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     session_id TEXT NOT NULL REFERENCES sessions(id),
                     role TEXT NOT NULL,
                     data TEXT NOT NULL,
                     ts INTEGER NOT NULL DEFAULT 0
                 );
                 CREATE VIRTUAL TABLE messages_fts USING fts5(
                     content,
                     session_id UNINDEXED,
                     message_id UNINDEXED,
                     tokenize = 'trigram'
                );
                CREATE TABLE IF NOT EXISTS usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    model TEXT NOT NULL DEFAULT '',
                    context_used INTEGER NOT NULL DEFAULT 0,
                    context_size INTEGER NOT NULL DEFAULT 0,
                    cost_usd REAL NOT NULL DEFAULT 0,
                    cumulative_cost REAL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE INDEX IF NOT EXISTS idx_usage_session
                    ON usage(session_id, id);
                CREATE TABLE channel_state (
                    conv_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (conv_id, key)
                );
                CREATE TABLE prompt_history (
                    cwd TEXT NOT NULL,
                    text TEXT NOT NULL,
                    use_count INTEGER NOT NULL DEFAULT 1,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (cwd, text)
                );
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    root TEXT NOT NULL,
                    defaults TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    kind TEXT NOT NULL DEFAULT 'project',
                    parent_id TEXT,
                    meta TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_sessions_agent
                    ON sessions(agent, agent_session);
                CREATE INDEX IF NOT EXISTS idx_usage_created
                    ON usage(created_at);
                CREATE TABLE granted_roots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
                    root TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(project_id, root)
                );
                CREATE TABLE plan_reviews (
                    plan_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    content TEXT NOT NULL DEFAULT '',
                    content_hash TEXT NOT NULL DEFAULT '',
                    complete INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'awaiting_review',
                    decision TEXT,
                    decision_intent_at INTEGER,
                    applied_at INTEGER,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    superseded_by INTEGER,
                    PRIMARY KEY (plan_id, revision)
                );
                CREATE INDEX idx_plan_reviews_session
                    ON plan_reviews(session_id, plan_id, revision);
                CREATE INDEX idx_plan_reviews_request
                    ON plan_reviews(request_id);
                CREATE TABLE web_pairing (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    key TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE TABLE scanned_native (
                    backend TEXT NOT NULL,
                    native_handle TEXT NOT NULL,
                    size INTEGER,
                    mtime_ms INTEGER,
                    session_id TEXT NOT NULL,
                    scanned_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (backend, native_handle)
                );
                CREATE TABLE session_tombstones (
                    backend TEXT NOT NULL,
                    native_handle TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (backend, native_handle)
                );
                CREATE TABLE web_devices (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    token_hash TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    approved_at TEXT,
                    last_seen TEXT
                );
                CREATE TABLE mission_flows (
                    flow_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    name TEXT NOT NULL DEFAULT '',
                    definition TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (flow_id, version)
                );
                CREATE TABLE mission_runs (
                    run_id TEXT PRIMARY KEY,
                    flow_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    snapshot TEXT NOT NULL,
                    interrupted INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    ended_at TEXT,
                    cancelled INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE mission_tasks (
                    task_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempt INTEGER NOT NULL DEFAULT 1,
                    output TEXT,
                    error TEXT,
                    started_at TEXT,
                    ended_at TEXT
                );
                CREATE TABLE mission_inbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    decision TEXT,
                    feedback TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    resolved_at TEXT
                );",
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await?;
        Ok(Self { conn })
    }

    /// `agent` is the Damon agent id backing this session (e.g. "claude").
    pub async fn create_session(
        &self,
        id: &str,
        cwd: &str,
        agent: Option<&str>,
    ) -> anyhow::Result<()> {
        let id = id.to_string();
        let cwd = cwd.to_string();
        let agent = agent.map(String::from);
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT INTO sessions (id, cwd, agent, last_active_at)
                     VALUES (?1, ?2, ?3, datetime('now'))",
                    rusqlite::params![id, cwd, agent],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// Persist the ACP session routing: (agent id, agent-side sessionId).
    /// Survives daemon restarts so session/resume can reattach.
    pub async fn set_agent_session(
        &self,
        id: &str,
        agent: &str,
        agent_session: &str,
    ) -> anyhow::Result<()> {
        let sid = id.to_string();
        let agent = agent.to_string();
        let agent_session = agent_session.to_string();
        // Binding a native handle is the user's explicit new intent —
        // a scanner tombstone for it dissolves.
        let _ = self.clear_tombstone(&agent, &agent_session).await;
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE sessions SET agent = ?2, agent_session = ?3 WHERE id = ?1",
                    rusqlite::params![sid, agent, agent_session],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// The persisted (agent id, agent-side sessionId) for a session.
    pub async fn agent_session(&self, id: &str) -> anyhow::Result<Option<(String, String)>> {
        let sid = id.to_string();
        Ok(self
            .conn
            .call(move |c| {
                c.query_row(
                    "SELECT COALESCE(agent, ''), COALESCE(agent_session, '')
                     FROM sessions WHERE id = ?1",
                    [sid],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()
            })
            .await?
            .filter(|(a, s)| !a.is_empty() && !s.is_empty()))
    }

    /// Find the Damon session id already bound to a native backend
    /// session — import dedup so resuming the same handle twice does
    /// not mint duplicate rows.
    pub async fn session_by_agent_session(
        &self,
        agent: &str,
        agent_session: &str,
    ) -> anyhow::Result<Option<String>> {
        let agent = agent.to_string();
        let agent_session = agent_session.to_string();
        Ok(self
            .conn
            .call(move |c| {
                c.query_row(
                    "SELECT id FROM sessions WHERE agent = ?1 AND agent_session = ?2",
                    rusqlite::params![agent, agent_session],
                    |r| r.get(0),
                )
                .optional()
            })
            .await?)
    }

    /// The session's working directory — the cwd the agent's tools run
    /// in.
    pub async fn session_cwd(&self, id: &str) -> anyhow::Result<Option<String>> {
        let id = id.to_string();
        Ok(self
            .conn
            .call(move |c| {
                c.query_row("SELECT cwd FROM sessions WHERE id = ?1", [id], |r| {
                    r.get::<_, String>(0)
                })
                .optional()
            })
            .await?)
    }

    pub async fn append(&self, session_id: &str, role: &str, data: &Value) -> anyhow::Result<i64> {
        let sid = session_id.to_string();
        let role = role.to_string();
        let data = data.to_string();
        // Stamped at call time — the persistence moment, not the
        // DB-write moment (identical in practice; keeping it outside
        // the closure leaves the SQL side effect-free).
        let ts = now_ms();
        self.conn
            .call(move |c| {
                let tx = c.transaction()?;
                tx.execute(
                    "INSERT INTO messages (session_id, role, data, ts)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![sid, role, data, ts],
                )?;
                let id = tx.last_insert_rowid();
                // Index the message's searchable text for full-text
                // search — message text and tool I/O (see `fts_text`).
                // Applies to NEW rows only; messages written before
                // tool-text indexing keep their original index entries.
                let v: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
                if let Some(text) = fts_text(&role, &v) {
                    tx.execute(
                        "INSERT INTO messages_fts (content, session_id, message_id)
                             VALUES (?1, ?2, ?3)",
                        rusqlite::params![text, sid, id],
                    )?;
                }
                // Track activity so retention prunes by idle time, not
                // creation time — a daily-used session must never be
                // swept just because it was created long ago.
                tx.execute(
                    "UPDATE sessions SET last_active_at = datetime('now') WHERE id = ?1",
                    rusqlite::params![sid],
                )?;
                tx.commit()?;
                Ok::<i64, tokio_rusqlite::Error>(id)
            })
            .await
            .map_err(Into::into)
    }

    /// Full-text search over message content. Returns (session_id,
    /// message_id, snippet) ordered by rank, with optional narrowing
    /// (see `SearchFilter`): `session_id` restricts hits to one
    /// session, `backend`/`cwd` narrow by the owning session's agent
    /// id and working directory, and `since_ms`/`until_ms` bound the
    /// MESSAGE's timestamp (unix ms, inclusive). Rows written before
    /// the ts column existed carry ts = 0 = unknown — those compare
    /// as their session's `created_at` (converted to ms), the
    /// coarsest bound the old schema could express, so pre-migration
    /// history stays inside bounded queries instead of silently
    /// dropping out of them.
    ///
    /// Sub-trigram terms (see `fts5_query`) ride along as
    /// exact-substring conjuncts; a query built only of them has no
    /// MATCH, hence no rank — those hits come back newest-first
    /// (message id order) with a Rust-built snippet.
    pub async fn search_filtered(
        &self,
        query: &str,
        limit: usize,
        filter: SearchFilter,
    ) -> anyhow::Result<Vec<(String, i64, String)>> {
        if query.trim().is_empty() {
            return Ok(vec![]);
        }
        // FTS5 query syntax (*, NEAR, column filters) must never reach
        // the parser raw — innocent input would 500. Tokenize instead:
        // quoted spans become FTS5 phrases, bare AND/OR/NOT pass through
        // as operators, and every other term is emitted as its own quoted
        // phrase — so multi-word input gets implicit-AND semantics and
        // metacharacters stay literal.
        let Some(query) = Self::fts5_query(query) else {
            return Ok(vec![]);
        };
        // Effective message time for the bounds: the row's ts, or the
        // session's creation for ts=0 rows (see doc above). LEFT JOIN —
        // an fts row whose message is gone (no normal path does that;
        // delete removes both) still matches, reading as ts=0.
        let eff_ts = "(CASE WHEN COALESCE(messages.ts, 0) = 0
                    THEN CAST(strftime('%s', sessions.created_at) AS INTEGER) * 1000
                    ELSE messages.ts END)";
        self.conn
            .call(move |c| {
                // Hybrid queries keep the MATCH — snippet() and rank
                // stay live. A pure-LIKE query has no MATCH, and
                // snippet() is an FTS5 auxiliary function that
                // requires one: the raw content comes back (the
                // snippet is built in Rust, see `like_snippet`),
                // ordered newest-first instead of by rank.
                let fts = !query.match_expr.is_empty();
                // Exact-substring patterns for the sub-trigram terms;
                // ESCAPE keeps user %/_ literal, as on the FTS path.
                let like_pat = |t: &String| {
                    format!(
                        "%{}%",
                        t.replace('\\', "\\\\")
                            .replace('%', "\\%")
                            .replace('_', "\\_")
                    )
                };
                let like_patterns: Vec<String> = query.like_terms.iter().map(like_pat).collect();
                let not_like_patterns: Vec<String> =
                    query.like_not_terms.iter().map(like_pat).collect();
                let mut sql =
                    String::from("SELECT messages_fts.session_id, messages_fts.message_id, ");
                if fts {
                    sql.push_str("snippet(messages_fts, 0, char(1), char(2), '…', 32)");
                } else {
                    sql.push_str("messages_fts.content");
                }
                sql.push_str(
                    "
                     FROM messages_fts
                     JOIN sessions ON sessions.id = messages_fts.session_id
                     LEFT JOIN messages ON messages.id = messages_fts.message_id
                     WHERE ",
                );
                let mut binds: Vec<&dyn rusqlite::ToSql> = Vec::new();
                if fts {
                    sql.push_str("messages_fts MATCH ?1");
                    binds.push(&query.match_expr);
                } else {
                    // No MATCH — the LIKE conjuncts below carry the
                    // whole predicate; give WHERE a base to bind to.
                    sql.push_str("1=1");
                }
                for p in &like_patterns {
                    let n = binds.len() + 1;
                    sql.push_str(&format!(" AND messages_fts.content LIKE ?{n} ESCAPE '\\'"));
                    binds.push(p);
                }
                for p in &not_like_patterns {
                    let n = binds.len() + 1;
                    sql.push_str(&format!(
                        " AND messages_fts.content NOT LIKE ?{n} ESCAPE '\\'"
                    ));
                    binds.push(p);
                }
                let mut idx = binds.len() + 1;
                if filter.session_id.is_some() {
                    sql.push_str(&format!(" AND messages_fts.session_id = ?{idx}"));
                    idx += 1;
                }
                if filter.backend.is_some() {
                    sql.push_str(&format!(" AND COALESCE(sessions.agent, '') = ?{idx}"));
                    idx += 1;
                }
                if filter.cwd.is_some() {
                    sql.push_str(&format!(" AND sessions.cwd = ?{idx}"));
                    idx += 1;
                }
                if filter.project_id.is_some() {
                    sql.push_str(&format!(" AND sessions.project_id = ?{idx}"));
                    idx += 1;
                }
                if filter.since_ms.is_some() {
                    sql.push_str(&format!(" AND {eff_ts} >= ?{idx}"));
                    idx += 1;
                }
                if filter.until_ms.is_some() {
                    sql.push_str(&format!(" AND {eff_ts} <= ?{idx}"));
                    idx += 1;
                }
                if fts {
                    sql.push_str(&format!(" ORDER BY rank LIMIT ?{idx}"));
                } else {
                    sql.push_str(&format!(
                        " ORDER BY messages_fts.message_id DESC LIMIT ?{idx}"
                    ));
                }
                let mut stmt = c.prepare(&sql)?;
                if let Some(s) = &filter.session_id {
                    binds.push(s);
                }
                if let Some(b) = &filter.backend {
                    binds.push(b);
                }
                if let Some(w) = &filter.cwd {
                    binds.push(w);
                }
                if let Some(p) = &filter.project_id {
                    binds.push(p);
                }
                if let Some(s) = &filter.since_ms {
                    binds.push(s);
                }
                if let Some(u) = &filter.until_ms {
                    binds.push(u);
                }
                let lim = limit as i64;
                binds.push(&lim);
                let rows = stmt
                    .query_map(binds.as_slice(), |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<_>, tokio_rusqlite::Error>(
                    rows.into_iter()
                        .map(|(session_id, message_id, text)| {
                            let snippet = if fts {
                                text
                            } else {
                                like_snippet(&text, &query.like_terms)
                            };
                            (session_id, message_id, snippet)
                        })
                        .collect(),
                )
            })
            .await
            .map_err(Into::into)
    }

    /// Build a safe FTS5 MATCH expression from user input, splitting
    /// out sub-trigram terms for a LIKE fallback.
    ///
    /// Token rules (unchanged):
    /// - `"..."` spans (with `""` escapes) become FTS5 phrase tokens.
    /// - Bare `AND`, `OR`, `NOT` pass through as operators.
    /// - Everything else becomes a single-quoted phrase — `*`, `NEAR(...)`,
    ///   `col:value`, and stray quotes stay literal text.
    ///
    /// The token list is then sanitized so the result always parses: leading
    /// and trailing binary operators are dropped, consecutive binary
    /// operators collapse, and NOT survives only as a binary exclusion
    /// operator (`x NOT y`) — FTS5 has no unary NOT, so `x AND NOT y` is
    /// rewritten to the equivalent `x NOT y`, and a leading or operand-less
    /// NOT is dropped.
    ///
    /// The split: any term shorter than 3 characters — bare or quoted,
    /// after stripping a trailing `*` — cannot survive the trigram
    /// tokenizer (SQLite silently drops sub-3-char query tokens, so
    /// `커밋` would match nothing). Such a term moves to
    /// `like_terms`/`like_not_terms` instead, where it becomes an
    /// exact-substring conjunct over the indexed content; every other
    /// term stays on the FTS5 path, keeping snippet()/rank. Operators
    /// never bind LIKE-path terms: short terms are mandatory
    /// conjuncts, so an `OR` touching one narrows to AND (fewer hits,
    /// never wrong ones) and a NOT-operand exclusion is honored.
    /// Returns `None` when nothing searchable remains.
    fn fts5_query(input: &str) -> Option<FtsQuery> {
        enum Tok {
            Term(String),
            AndOr(&'static str),
            Not,
        }
        let mut toks: Vec<Tok> = Vec::new();
        let mut chars = input.chars().peekable();
        while let Some(&c) = chars.peek() {
            if c.is_whitespace() {
                chars.next();
                continue;
            }
            if c == '"' {
                chars.next();
                let mut phrase = String::new();
                loop {
                    match chars.next() {
                        Some('"') if chars.peek() == Some(&'"') => {
                            chars.next();
                            phrase.push('"');
                        }
                        Some('"') | None => break,
                        Some(ch) => phrase.push(ch),
                    }
                }
                if !phrase.is_empty() {
                    toks.push(Tok::Term(phrase));
                }
                continue;
            }
            let mut term = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() || ch == '"' {
                    break;
                }
                term.push(ch);
                chars.next();
            }
            match term.as_str() {
                "AND" => toks.push(Tok::AndOr("AND")),
                "OR" => toks.push(Tok::AndOr("OR")),
                "NOT" => toks.push(Tok::Not),
                _ => toks.push(Tok::Term(term)),
            }
        }

        // Sanitize: drop leading/trailing binary operators, collapse
        // consecutive ones, and keep NOT only as a binary exclusion
        // operator (a trailing NOT dies unconsumed).
        let mut q = FtsQuery {
            match_expr: String::new(),
            like_terms: Vec::new(),
            like_not_terms: Vec::new(),
        };
        let mut out: Vec<String> = Vec::new();
        let mut pending_op: Option<&'static str> = None;
        let mut pending_not = false;
        for t in toks {
            match t {
                Tok::AndOr(op) => {
                    if out.is_empty() {
                        continue; // leading operator — drop
                    }
                    pending_not = false;
                    pending_op = Some(op); // last one wins on a run
                }
                Tok::Not => {
                    // FTS5 has no unary NOT, so NOT only parses as a binary
                    // exclusion: keep it when an operand precedes it, drop
                    // it when it would start the expression.
                    if !out.is_empty() {
                        pending_not = true;
                    }
                }
                Tok::Term(term) => {
                    // A trailing `*` makes the term an FTS5 prefix
                    // query (`"tok"*` — prefix on the last token of
                    // the phrase); quote-wrapping alone used to
                    // swallow it, so `error*` searched the literal
                    // token "error" and never matched "errors". A
                    // bare `*` is nothing searchable — drop it.
                    let (stem, prefix) = match term.strip_suffix('*') {
                        Some(s) => (s.trim_end_matches('*'), true),
                        None => (term.as_str(), false),
                    };
                    // Sub-trigram term: route to the LIKE conjuncts
                    // (exclusion under NOT) instead of a MATCH that
                    // would silently drop it. Its connecting operator
                    // is dropped with it — short terms stay
                    // conjunctive. The operator push is deferred
                    // until a piece actually lands in `out`, so a
                    // diverted or empty term can never strand a
                    // dangling operator in the MATCH expression.
                    if stem.chars().count() < 3 {
                        if !stem.is_empty() {
                            if pending_not {
                                q.like_not_terms.push(stem.to_string());
                                pending_not = false;
                            } else {
                                q.like_terms.push(stem.to_string());
                            }
                            pending_op = None;
                        }
                        continue;
                    }
                    let escaped = stem.replace('"', "\"\"");
                    if pending_not {
                        pending_op = None;
                        out.push("NOT".to_string());
                        pending_not = false;
                    } else if let Some(op) = pending_op.take() {
                        out.push(op.to_string());
                    }
                    if prefix {
                        out.push(format!("\"{escaped}\"*"));
                    } else {
                        out.push(format!("\"{escaped}\""));
                    }
                }
            }
        }
        q.match_expr = out.join(" ");
        if q.match_expr.is_empty() && q.like_terms.is_empty() && q.like_not_terms.is_empty() {
            None
        } else {
            Some(q)
        }
    }

    /// Set the title only when none exists — the first user
    /// message wins, later prompts and explicit renames are untouched.
    pub async fn set_title_if_empty(&self, session_id: &str, title: &str) -> anyhow::Result<()> {
        let sid = session_id.to_string();
        let title = title.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE sessions SET title = ?2 WHERE id = ?1 AND title IS NULL",
                    rusqlite::params![sid, title],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Explicit rename — overwrites any existing title.
    pub async fn rename_session(&self, session_id: &str, title: &str) -> anyhow::Result<bool> {
        let sid = session_id.to_string();
        let title = title.to_string();
        self.conn
            .call(move |c| {
                let n = c.execute(
                    "UPDATE sessions SET title = ?2 WHERE id = ?1",
                    rusqlite::params![sid, title],
                )?;
                Ok::<bool, tokio_rusqlite::Error>(n > 0)
            })
            .await
            .map_err(Into::into)
    }

    /// Replace a session's whole tag list — rename-style semantics:
    /// the caller sends the complete new list, `[]` clears. Stored as
    /// a JSON array of strings so the tag filter can match membership
    /// with json_each. Returns false when the session doesn't exist.
    pub async fn set_tags(&self, session_id: &str, tags: &[String]) -> anyhow::Result<bool> {
        let sid = session_id.to_string();
        let tags = serde_json::to_string(tags).context("serialize tags")?;
        self.conn
            .call(move |c| {
                let n = c.execute(
                    "UPDATE sessions SET tags = ?2 WHERE id = ?1",
                    rusqlite::params![sid, tags],
                )?;
                Ok::<bool, tokio_rusqlite::Error>(n > 0)
            })
            .await
            .map_err(Into::into)
    }
    /// Record one turn's context snapshot. Called once per completed
    /// prompt turn with the agent's last `usage_update`: `context_used`
    /// is the window fill level, `context_size` its capacity, `cost_usd`
    /// the CUMULATIVE cost the agent reported. The row stores the
    /// per-turn delta in `cost_usd` (this turn's cumulative minus the
    /// previous row's, so SUM(cost_usd) is the real session total) and
    /// the raw cumulative in `cumulative_cost` as the next turn's
    /// baseline. A cumulative reset (fresh agent session, restarted
    /// counter) has no sane subtraction — the clamp keeps the reported
    /// value instead of going negative. A turn with no usage update
    /// (cancelled before the first response) writes nothing.
    pub async fn record_usage(
        &self,
        session_id: &str,
        model: &str,
        context_used: u64,
        context_size: u64,
        cost_usd: f64,
    ) -> anyhow::Result<()> {
        if context_used == 0 && cost_usd == 0.0 {
            return Ok(());
        }
        let sid = session_id.to_string();
        let model = model.to_string();
        self.conn
            .call(move |c| {
                // Read the baseline and insert in one transaction —
                // two back-to-back turns must not both subtract the
                // same predecessor.
                let tx = c.transaction()?;
                let last: Option<f64> = tx
                    .query_row(
                        "SELECT cumulative_cost FROM usage
                         WHERE session_id = ?1 ORDER BY id DESC LIMIT 1",
                        rusqlite::params![sid],
                        |r| r.get::<_, Option<f64>>(0),
                    )
                    .optional()?
                    .flatten();
                let delta = match last {
                    Some(prev) if cost_usd >= prev => cost_usd - prev,
                    // Reset (or NULL baseline from a pre-migration row):
                    // keep the raw value — no negative, no subtraction.
                    _ => cost_usd,
                };
                tx.execute(
                    "INSERT INTO usage
                         (session_id, model, context_used, context_size, cost_usd, cumulative_cost)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![
                        sid,
                        model,
                        context_used as i64,
                        context_size as i64,
                        delta,
                        cost_usd
                    ],
                )?;
                tx.commit()?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Per-model rollup across all sessions: (model, last context_used,
    /// context_size, total cost_usd, turns). Context is a snapshot, so
    /// the rollup reports the most recent fill level, not a sum.
    pub async fn usage_summary(
        &self,
        project: Option<String>,
    ) -> anyhow::Result<Vec<(String, u64, u64, f64, u64)>> {
        // Project filter bounds both subqueries to the project's
        // sessions; None keeps the global rollup.
        let scope = project
            .map(|p| {
                format!(
                    " AND session_id IN (SELECT id FROM sessions WHERE project_id = '{}')",
                    p.replace('\'', "''")
                )
            })
            .unwrap_or_default();
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(&format!(
                    "SELECT model, context_used, context_size, cost_usd FROM usage
                     WHERE id IN (SELECT MAX(id) FROM usage GROUP BY model){scope}"
                ))?;
                let latest: Vec<(String, i64, i64, f64)> = stmt
                    .query_map([], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })?
                    .collect::<Result<_, _>>()?;
                let mut stmt = c.prepare(&format!(
                    "SELECT model, SUM(cost_usd), COUNT(*) FROM usage
                     WHERE 1=1{scope} GROUP BY model"
                ))?;
                let totals: Vec<(String, f64, i64)> = stmt
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                    .collect::<Result<_, _>>()?;
                let mut out = Vec::new();
                for (model, used, size, _c) in latest {
                    let (cost, turns) = totals
                        .iter()
                        .find(|(m, _, _)| *m == model)
                        .map(|(_, c, t)| (*c, *t))
                        .unwrap_or((0.0, 0));
                    out.push((model, used as u64, size as u64, cost, turns as u64));
                }
                out.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));
                Ok::<Vec<(String, u64, u64, f64, u64)>, tokio_rusqlite::Error>(out)
            })
            .await
            .map_err(Into::into)
    }

    /// Per-session totals: (last context_used, context_size, total
    /// cost_usd, turns).
    pub async fn session_usage(&self, session_id: &str) -> anyhow::Result<(u64, u64, f64, u64)> {
        let sid = session_id.to_string();
        self.conn
            .call(move |c| {
                let (used, size): (i64, i64) = c
                    .query_row(
                        "SELECT context_used, context_size FROM usage
                         WHERE session_id = ?1 ORDER BY id DESC LIMIT 1",
                        rusqlite::params![sid],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .unwrap_or((0, 0));
                // "turns" = prompt turns (persisted user messages), not
                // usage rows — an agent that reports no usage_update
                // still ran the turn. MAX keeps the count correct for
                // sessions whose usage rows outnumber stored prompts
                // (e.g. rows recorded without messages in tests/tools).
                let (cost, n): (f64, i64) = c.query_row(
                    "SELECT COALESCE(SUM(cost_usd),0),
                            MAX(
                              (SELECT COUNT(*) FROM usage WHERE session_id = ?1),
                              (SELECT COUNT(*) FROM messages
                               WHERE session_id = ?1 AND role = 'user')
                            )
                     FROM usage WHERE session_id = ?1",
                    rusqlite::params![sid],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                Ok::<(u64, u64, f64, u64), tokio_rusqlite::Error>((
                    used as u64,
                    size as u64,
                    cost,
                    n as u64,
                ))
            })
            .await
            .map_err(Into::into)
    }

    /// Daily usage rollup for the last `days` calendar days (today
    /// inclusive — days=7 covers today plus the six days before it;
    /// 0 clamps to today-only): (date 'YYYY-MM-DD', turns, cost_usd,
    /// latest context_used) grouped by day across ALL sessions, from
    /// the usage table's UTC `created_at`. Context is a snapshot, so
    /// a day reports its last fill level, not a sum. Days with no
    /// rows are absent — the client renders the gaps.
    pub async fn daily_usage(&self, days: u32) -> anyhow::Result<Vec<(String, u64, f64, u64)>> {
        let cutoff = format!("-{} days", days.saturating_sub(1));
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    // The context_used subquery correlates on the day
                    // (constant inside each group), picking that day's
                    // last row by id — the same latest-snapshot rule
                    // usage_summary applies per model.
                    "SELECT substr(u.created_at, 1, 10), COUNT(*), SUM(u.cost_usd),
                            (SELECT u2.context_used FROM usage u2
                             WHERE substr(u2.created_at, 1, 10)
                                   = substr(u.created_at, 1, 10)
                             ORDER BY u2.id DESC LIMIT 1)
                     FROM usage u
                     WHERE u.created_at >= date('now', ?1)
                     GROUP BY substr(u.created_at, 1, 10)
                     ORDER BY substr(u.created_at, 1, 10)",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![cutoff], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, f64>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<_>, tokio_rusqlite::Error>(
                    rows.into_iter()
                        .map(|(d, t, c, u)| (d, t as u64, c, u as u64))
                        .collect(),
                )
            })
            .await
            .map_err(Into::into)
    }

    /// Usage rollup grouped per (LOCAL day, backend, model) over the
    /// last `days` local days (today inclusive — 7 covers today plus
    /// the six days before it; 0 clamps to today-only, same rule as
    /// `daily_usage`), plus the window total. `tz_offset_minutes` is
    /// the caller's UTC offset: the UTC `created_at` stamps are
    /// shifted by it so buckets match the user's calendar (0 = UTC
    /// buckets, the `daily_usage` behavior). `backend` is the owning
    /// session's agent id ("" when unset). Turns count usage rows;
    /// context columns sum the per-turn snapshots (each row is a fill
    /// level, so sums grow with turn count — `daily_usage` reports
    /// the day's latest snapshot instead); cost sums the per-turn
    /// deltas. Days with no rows are absent — the client renders the
    /// gaps.
    pub async fn usage_aggregate(
        &self,
        days: u32,
        tz_offset_minutes: i32,
    ) -> anyhow::Result<UsageAggregate> {
        // SQLite date modifiers: "+540 minutes" / "-420 minutes" shift
        // a UTC stamp into the caller's wall clock; the cutoff date is
        // computed in the same shifted clock so the window is `days`
        // LOCAL days, not UTC ones.
        let tz = format!("{tz_offset_minutes:+} minutes");
        let cutoff = format!("-{} days", days.saturating_sub(1));
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT substr(datetime(u.created_at, ?1), 1, 10),
                            COALESCE(s.agent, ''),
                            u.model,
                            COUNT(*), SUM(u.context_used), SUM(u.context_size), SUM(u.cost_usd)
                     FROM usage u
                     LEFT JOIN sessions s ON s.id = u.session_id
                     WHERE date(u.created_at, ?1) >= date('now', ?1, ?2)
                     GROUP BY substr(datetime(u.created_at, ?1), 1, 10),
                              COALESCE(s.agent, ''), u.model
                     ORDER BY 1, 2, 3",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![tz, cutoff], |r| {
                        Ok(UsageDayRow {
                            date: r.get(0)?,
                            backend: r.get(1)?,
                            model: r.get(2)?,
                            turns: r.get::<_, i64>(3)? as u64,
                            context_used: r.get::<_, i64>(4)? as u64,
                            context_size: r.get::<_, i64>(5)? as u64,
                            cost_usd: r.get(6)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                let mut agg = UsageAggregate {
                    turns: 0,
                    context_used: 0,
                    context_size: 0,
                    cost_usd: 0.0,
                    rows,
                };
                for r in &agg.rows {
                    agg.turns += r.turns;
                    agg.context_used += r.context_used;
                    agg.context_size += r.context_size;
                    agg.cost_usd += r.cost_usd;
                }
                Ok::<_, tokio_rusqlite::Error>(agg)
            })
            .await
            .map_err(Into::into)
    }

    /// Delete every usage row (context/cost history) — the store side
    /// of `usage.clear`. Sessions, messages, and FTS are untouched.
    pub async fn usage_clear(&self) -> anyhow::Result<()> {
        self.conn
            .call(move |c| {
                c.execute("DELETE FROM usage", [])?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Delete a session and all of its messages.
    pub async fn delete_session(&self, id: &str) -> anyhow::Result<()> {
        let id = id.to_string();
        self.conn
            .call(move |c| {
                let tx = c.transaction()?;
                tx.execute(
                    "DELETE FROM messages_fts WHERE session_id = ?1",
                    rusqlite::params![id],
                )?;
                tx.execute(
                    "DELETE FROM messages WHERE session_id = ?1",
                    rusqlite::params![id],
                )?;
                // Usage rows belong to the session's lifecycle too —
                // leaving them behind orphaned totals into every
                // future usage_summary.
                tx.execute(
                    "DELETE FROM usage WHERE session_id = ?1",
                    rusqlite::params![id],
                )?;
                // Plan reviews are session lifecycle state as well —
                // a deleted session leaves no ghost approvals behind.
                tx.execute(
                    "DELETE FROM plan_reviews WHERE session_id = ?1",
                    rusqlite::params![id],
                )?;
                // Tombstone the native handle so the history sweep
                // never re-imports what the user just deleted. The
                // agent id + native handle are the sweep's identity.
                {
                    let handle: Option<(Option<String>, Option<String>)> = tx
                        .query_row(
                            "SELECT COALESCE(agent, ''), COALESCE(agent_session, '')
                             FROM sessions WHERE id = ?1",
                            rusqlite::params![id],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )
                        .optional()?;
                    if let Some((Some(agent), Some(native))) = handle
                        && !agent.is_empty()
                        && !native.is_empty()
                    {
                        tx.execute(
                            "INSERT OR IGNORE INTO session_tombstones
                                 (backend, native_handle) VALUES (?1, ?2)",
                            rusqlite::params![agent, native],
                        )?;
                        tx.execute(
                            "DELETE FROM scanned_native
                             WHERE backend = ?1 AND native_handle = ?2",
                            rusqlite::params![agent, native],
                        )?;
                    }
                }
                tx.execute("DELETE FROM sessions WHERE id = ?1", rusqlite::params![id])?;
                tx.commit()?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }
    /// Delete sessions (and their messages) created more than `days`
    /// ago, skipping `exclude` ids (live prompt turns). Returns the ids
    /// of the deleted sessions so callers can drop session-scoped state
    /// (e.g. MCP approvals). The FK has no ON DELETE CASCADE, so messages
    /// and FTS rows are deleted explicitly inside one transaction.
    pub async fn cleanup_older_than(
        &self,
        days: u32,
        exclude: &std::collections::HashSet<String>,
    ) -> anyhow::Result<Vec<String>> {
        let exclude: Vec<String> = exclude.iter().cloned().collect();
        self.conn
            .call(move |c| {
                let tx = c.transaction()?;
                let cutoff = format!("-{days} days");
                // Sessions IDLE long enough AND not currently running a
                // turn. last_active_at (touched on every append) is the
                // idle clock; created_at is the fallback for rows that
                // predate the column.
                let mut stmt = tx.prepare(
                    "SELECT id FROM sessions
                     WHERE COALESCE(last_active_at, created_at) < datetime('now', ?1)
                       AND id NOT IN (SELECT value FROM json_each(?2))
                       AND COALESCE(pinned, 0) = 0
                       AND COALESCE(archived, 0) = 0",
                )?;
                let doomed: Vec<String> = stmt
                    .query_map(
                        rusqlite::params![
                            cutoff,
                            serde_json::to_string(&exclude).unwrap_or_default()
                        ],
                        |r| r.get(0),
                    )?
                    .collect::<Result<_, _>>()?;
                drop(stmt);
                let ids_json = serde_json::to_string(&doomed).unwrap_or_default();
                tx.execute(
                    "DELETE FROM messages_fts WHERE session_id IN (
                         SELECT value FROM json_each(?1))",
                    rusqlite::params![ids_json],
                )?;
                tx.execute(
                    "DELETE FROM messages WHERE session_id IN (
                         SELECT value FROM json_each(?1))",
                    rusqlite::params![ids_json],
                )?;
                // Same orphan rule as delete_session: swept sessions
                // must not leave usage rows behind.
                tx.execute(
                    "DELETE FROM usage WHERE session_id IN (
                         SELECT value FROM json_each(?1))",
                    rusqlite::params![ids_json],
                )?;
                tx.execute(
                    "DELETE FROM sessions WHERE id IN (
                         SELECT value FROM json_each(?1))",
                    rusqlite::params![ids_json],
                )?;

                tx.commit()?;
                Ok::<Vec<String>, tokio_rusqlite::Error>(doomed)
            })
            .await
            .map_err(Into::into)
    }

    /// Pin/unpin a session — pinned rows float to the front of
    /// listings and the retention sweep skips them. Returns whether a
    /// row changed.
    pub async fn set_pinned(&self, id: &str, pinned: bool) -> anyhow::Result<bool> {
        let sid = id.to_string();
        Ok(self
            .conn
            .call(move |c| {
                Ok::<bool, tokio_rusqlite::Error>(
                    c.execute(
                        "UPDATE sessions SET pinned = ?2 WHERE id = ?1",
                        rusqlite::params![sid, pinned as i64],
                    )? != 0,
                )
            })
            .await?)
    }

    /// One persisted channel-conversation state cell (the bridge's
    /// `!cwd`/`!agent` prefs and chat→session mapping live here, so a
    /// bridge restart resumes where the chat left off).
    pub async fn get_channel_state(
        &self,
        conv_id: &str,
        key: &str,
    ) -> anyhow::Result<Option<String>> {
        let (conv, key) = (conv_id.to_string(), key.to_string());
        Ok(self
            .conn
            .call(move |c| {
                c.query_row(
                    "SELECT value FROM channel_state WHERE conv_id = ?1 AND key = ?2",
                    rusqlite::params![conv, key],
                    |r| r.get(0),
                )
                .optional()
                .map_err(tokio_rusqlite::Error::from)
            })
            .await?)
    }

    pub async fn set_channel_state(
        &self,
        conv_id: &str,
        key: &str,
        value: &str,
    ) -> anyhow::Result<()> {
        let (conv, key, value) = (conv_id.to_string(), key.to_string(), value.to_string());
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT INTO channel_state (conv_id, key, value) VALUES (?1, ?2, ?3)
                     ON CONFLICT(conv_id, key) DO UPDATE SET value = excluded.value",
                    rusqlite::params![conv, key, value],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// Record one sent prompt for the composer's ghost completion and
    /// ↑/↓ recall. The upsert bumps the use count and stamps recency
    /// (a resend moves the row to newest); the per-cwd trim keeps only
    /// the newest [`PROMPT_HISTORY_CAP`] rows so the table cannot grow
    /// without bound.
    pub async fn record_prompt(&self, cwd: &str, text: &str) -> anyhow::Result<()> {
        let (cwd, text) = (cwd.to_string(), text.to_string());
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT INTO prompt_history (cwd, text, use_count, updated_at)
                     VALUES (?1, ?2, 1, ?3)
                     ON CONFLICT(cwd, text) DO UPDATE
                     SET use_count = use_count + 1, updated_at = excluded.updated_at",
                    rusqlite::params![cwd, text, now_ms()],
                )?;
                c.execute(
                    "DELETE FROM prompt_history WHERE cwd = ?1 AND (cwd, text) NOT IN (
                         SELECT cwd, text FROM prompt_history WHERE cwd = ?1
                         ORDER BY updated_at DESC, use_count DESC LIMIT ?2)",
                    rusqlite::params![cwd, PROMPT_HISTORY_CAP],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// Newest-first prompt history for one cwd, with per-prompt use
    /// counts (the caller ranks ghost completions by count, not us).
    pub async fn recent_prompts(
        &self,
        cwd: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<(String, i64)>> {
        let cwd = cwd.to_string();
        Ok(self
            .conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT text, use_count FROM prompt_history WHERE cwd = ?1
                     ORDER BY updated_at DESC, use_count DESC LIMIT ?2",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![cwd, limit], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
                    })?
                    .collect::<Result<_, _>>()?;
                Ok::<Vec<(String, i64)>, tokio_rusqlite::Error>(rows)
            })
            .await?)
    }

    /// Create a project. `defaults_json` is stored verbatim (the RPC
    /// layer validates shape); the id is caller-minted (uuid).
    pub async fn create_project(
        &self,
        id: &str,
        name: &str,
        root: &str,
        defaults_json: &str,
    ) -> anyhow::Result<()> {
        self.create_project_full(id, name, root, defaults_json, "project", None, "{}")
            .await
    }

    /// Create a project or worktree row. Worktree rows (kind="worktree")
    /// carry their parent project id and a raw-JSON `meta` of git facts;
    /// the RPC layer validates the shape.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_project_full(
        &self,
        id: &str,
        name: &str,
        root: &str,
        defaults_json: &str,
        kind: &str,
        parent_id: Option<&str>,
        meta_json: &str,
    ) -> anyhow::Result<()> {
        let (id, name, root, d) = (
            id.to_string(),
            name.to_string(),
            root.to_string(),
            defaults_json.to_string(),
        );
        let (kind, parent, meta) = (
            kind.to_string(),
            parent_id.map(String::from),
            meta_json.to_string(),
        );
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT INTO projects
                        (id, name, root, defaults, kind, parent_id, meta)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params![id, name, root, d, kind, parent, meta],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// One project row; None when the id is unknown.
    pub async fn get_project(&self, id: &str) -> anyhow::Result<Option<ProjectRow>> {
        let id = id.to_string();
        Ok(self
            .conn
            .call(move |c| {
                c.query_row(
                    "SELECT id, name, root, defaults, kind, parent_id, meta
                     FROM projects WHERE id = ?1",
                    [&id],
                    project_row,
                )
                .optional()
                .map_err(tokio_rusqlite::Error::from)
            })
            .await?)
    }

    /// One project row by workspace root; None when no row (project or
    /// worktree) claims that path. The worktree flow uses this to find
    /// (or create) the parent project for a repo.
    pub async fn get_project_by_root(&self, root: &str) -> anyhow::Result<Option<ProjectRow>> {
        let root = root.to_string();
        Ok(self
            .conn
            .call(move |c| {
                c.query_row(
                    "SELECT id, name, root, defaults, kind, parent_id, meta
                     FROM projects WHERE root = ?1
                     ORDER BY kind != 'project', created_at LIMIT 1",
                    [&root],
                    project_row,
                )
                .optional()
                .map_err(tokio_rusqlite::Error::from)
            })
            .await?)
    }

    /// All projects, oldest first.
    pub async fn list_projects(&self) -> anyhow::Result<Vec<ProjectRow>> {
        Ok(self
            .conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, name, root, defaults, kind, parent_id, meta
                     FROM projects ORDER BY created_at, id",
                )?;
                let rows = stmt
                    .query_map([], project_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<ProjectRow>, tokio_rusqlite::Error>(rows)
            })
            .await?)
    }

    /// Update a worktree row's parent link and git metadata (used when
    /// `worktree.create` re-registers an already-registered path, e.g.
    /// a retry after a crash between register stages). Returns whether
    /// a row changed.
    pub async fn set_project_meta(
        &self,
        id: &str,
        parent_id: Option<&str>,
        meta_json: &str,
    ) -> anyhow::Result<bool> {
        let (id, parent, meta) = (
            id.to_string(),
            parent_id.map(String::from),
            meta_json.to_string(),
        );
        Ok(self
            .conn
            .call(move |c| {
                Ok::<bool, tokio_rusqlite::Error>(
                    c.execute(
                        "UPDATE projects SET parent_id = ?2, meta = ?3 WHERE id = ?1",
                        rusqlite::params![id, parent, meta],
                    )? != 0,
                )
            })
            .await?)
    }

    /// Replace a project's defaults JSON. Returns whether a row changed.
    pub async fn set_project_defaults(
        &self,
        id: &str,
        defaults_json: &str,
    ) -> anyhow::Result<bool> {
        let (id, d) = (id.to_string(), defaults_json.to_string());
        Ok(self
            .conn
            .call(move |c| {
                Ok::<bool, tokio_rusqlite::Error>(
                    c.execute(
                        "UPDATE projects SET defaults = ?2 WHERE id = ?1",
                        rusqlite::params![id, d],
                    )? != 0,
                )
            })
            .await?)
    }

    /// Persist the session's resolved default model. Revives the API
    /// 0.4.0 deleted as dead — session.create now writes the
    /// daemon-resolved model (explicit param or project default).
    pub async fn set_session_model(&self, id: &str, model: &str) -> anyhow::Result<()> {
        let (sid, m) = (id.to_string(), model.to_string());
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE sessions SET model = ?2 WHERE id = ?1",
                    rusqlite::params![sid, m],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// Bind a session to a project (create-time; None unbinds).
    pub async fn set_session_project(&self, id: &str, project: Option<&str>) -> anyhow::Result<()> {
        let (sid, p) = (id.to_string(), project.map(String::from));
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE sessions SET project_id = ?2 WHERE id = ?1",
                    rusqlite::params![sid, p],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// The session's project scope (None = unbound) — the key that
    /// selects a session's effective granted roots.
    pub async fn session_project(&self, id: &str) -> anyhow::Result<Option<String>> {
        let sid = id.to_string();
        Ok(self
            .conn
            .call(move |c| {
                Ok::<Option<String>, tokio_rusqlite::Error>(c.query_row(
                    "SELECT project_id FROM sessions WHERE id = ?1",
                    [&sid],
                    |r| r.get(0),
                )?)
            })
            .await?)
    }

    /// Persist one granted root. Idempotent per (project, root): a
    /// duplicate is a no-op, not an error. `root` must already be the
    /// canonical form (resolved, absolute) — validation lives at the
    /// RPC door, the store just persists.
    pub async fn grant_root(&self, project_id: Option<String>, root: String) -> anyhow::Result<()> {
        self.conn
            .call(move |c| {
                let exists: bool = c
                    .query_row(
                        "SELECT 1 FROM granted_roots WHERE project_id IS ?1 AND root = ?2",
                        rusqlite::params![project_id, root],
                        |r| r.get::<_, i64>(0).map(|_| true),
                    )
                    .optional()?
                    .unwrap_or(false);
                if !exists {
                    c.execute(
                        "INSERT INTO granted_roots (project_id, root) VALUES (?1, ?2)",
                        rusqlite::params![project_id, root],
                    )?;
                }
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Grants effective for a scope: the project's plus every global
    /// one (`project_id = None` lists globals only). `root` is the
    /// canonical stored form.
    pub async fn granted_roots(
        &self,
        project_id: Option<String>,
    ) -> anyhow::Result<Vec<GrantedRoot>> {
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT COALESCE(project_id, ''), root, created_at
                     FROM granted_roots
                     WHERE project_id IS NULL OR project_id = ?1
                     ORDER BY project_id IS NOT NULL, root",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![project_id], |r| {
                        Ok(GrantedRoot {
                            project_id: {
                                let p: String = r.get(0)?;
                                (!p.is_empty()).then_some(p)
                            },
                            root: r.get(1)?,
                            created_at: r.get(2)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// Remove one grant (exact project scope + canonical root match).
    /// Returns whether a row was removed.
    pub async fn revoke_root(
        &self,
        project_id: Option<String>,
        root: String,
    ) -> anyhow::Result<bool> {
        self.conn
            .call(move |c| {
                let n = c.execute(
                    "DELETE FROM granted_roots WHERE project_id IS ?1 AND root = ?2",
                    rusqlite::params![project_id, root],
                )?;
                Ok::<bool, tokio_rusqlite::Error>(n > 0)
            })
            .await
            .map_err(Into::into)
    }

    /// Register a `kind=plan` ask as a durable review row. A re-ask
    /// while the session's latest revision is still open (not decided,
    /// not terminal) chains onto the same planId as revision+1 and
    /// supersedes the open ones; otherwise a fresh planId starts at
    /// revision 1. Returns the new revision's row.
    pub async fn record_plan(
        &self,
        session_id: &str,
        request_id: &str,
        title: &str,
        content: &str,
        complete: bool,
    ) -> anyhow::Result<PlanRow> {
        let session_id = session_id.to_string();
        let request_id = request_id.to_string();
        let title = title.to_string();
        let content = content.to_string();
        self.conn
            .call(move |c| {
                let now = plan_now_ms();
                let tx = c.transaction()?;
                // The session's newest revision decides chaining.
                let latest: Option<(String, i64, String)> = tx
                    .query_row(
                        "SELECT plan_id, revision, status FROM plan_reviews
                         WHERE session_id = ?1
                         ORDER BY created_at DESC, revision DESC LIMIT 1",
                        rusqlite::params![session_id],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .optional()?;
                let (plan_id, revision) = match latest {
                    // Still-open review: the same plan, revised.
                    Some((pid, rev, status))
                        if matches!(
                            status.as_str(),
                            "awaiting_review" | "changes_requested" | "deferred"
                        ) =>
                    {
                        let next = rev + 1;
                        tx.execute(
                            "UPDATE plan_reviews
                             SET status = 'superseded', superseded_by = ?1, updated_at = ?2
                             WHERE plan_id = ?3
                               AND status IN ('awaiting_review', 'submitting',
                                              'deferred', 'changes_requested')",
                            rusqlite::params![next, now, pid],
                        )?;
                        (pid, next)
                    }
                    // No open review — a new plan chain.
                    _ => (uuid::Uuid::new_v4().to_string(), 1),
                };
                let hash = plan_content_hash(&content);
                tx.execute(
                    "INSERT INTO plan_reviews
                         (plan_id, session_id, request_id, revision, title,
                          content, content_hash, complete, status,
                          created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'awaiting_review', ?9, ?9)",
                    rusqlite::params![
                        plan_id,
                        session_id,
                        request_id,
                        revision,
                        title,
                        content,
                        hash,
                        complete as i64,
                        now
                    ],
                )?;
                tx.commit()?;
                Ok::<PlanRow, tokio_rusqlite::Error>(PlanRow {
                    plan_id,
                    session_id,
                    request_id,
                    revision,
                    title,
                    content,
                    content_hash: hash,
                    complete,
                    status: "awaiting_review".to_string(),
                    decision: None,
                    decision_intent_at: None,
                    applied_at: None,
                    created_at: now,
                    updated_at: now,
                    superseded_by: None,
                })
            })
            .await
            .map_err(Into::into)
    }

    fn plan_row_columns() -> &'static str {
        "plan_id, session_id, request_id, revision, title, content, content_hash,
         complete, status, decision, decision_intent_at, applied_at,
         created_at, updated_at, superseded_by"
    }

    fn map_plan_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<PlanRow> {
        let decision: Option<String> = r.get(9)?;
        Ok(PlanRow {
            plan_id: r.get(0)?,
            session_id: r.get(1)?,
            request_id: r.get(2)?,
            revision: r.get(3)?,
            title: r.get(4)?,
            content: r.get(5)?,
            content_hash: r.get(6)?,
            complete: r.get::<_, i64>(7)? != 0,
            status: r.get(8)?,
            decision: decision.and_then(|raw| serde_json::from_str(&raw).ok()),
            decision_intent_at: r.get(10)?,
            applied_at: r.get(11)?,
            created_at: r.get(12)?,
            updated_at: r.get(13)?,
            superseded_by: r.get(14)?,
        })
    }

    /// One revision of a plan review.
    pub async fn plan_row(&self, plan_id: &str, revision: i64) -> anyhow::Result<Option<PlanRow>> {
        let (pid, rev) = (plan_id.to_string(), revision);
        self.conn
            .call(move |c| {
                Ok::<Option<PlanRow>, tokio_rusqlite::Error>(
                    c.query_row(
                        &format!(
                            "SELECT {} FROM plan_reviews
                             WHERE plan_id = ?1 AND revision = ?2",
                            Self::plan_row_columns()
                        ),
                        rusqlite::params![pid, rev],
                        Self::map_plan_row,
                    )
                    .optional()?,
                )
            })
            .await
            .map_err(Into::into)
    }

    /// The still-open revision answering this live ask, if any — the
    /// hook `permission.respond` uses to land a legacy decision in the
    /// durable row.
    pub async fn open_plan_for_request(&self, request_id: &str) -> anyhow::Result<Option<PlanRow>> {
        let rid = request_id.to_string();
        self.conn
            .call(move |c| {
                Ok::<Option<PlanRow>, tokio_rusqlite::Error>(
                    c.query_row(
                        &format!(
                            "SELECT {} FROM plan_reviews
                             WHERE request_id = ?1
                               AND status IN ('awaiting_review', 'deferred')
                             ORDER BY created_at DESC LIMIT 1",
                            Self::plan_row_columns()
                        ),
                        rusqlite::params![rid],
                        Self::map_plan_row,
                    )
                    .optional()?,
                )
            })
            .await
            .map_err(Into::into)
    }

    /// Every revision of every plan for a session, chain-ordered —
    /// the review history `plan.list` serves.
    pub async fn plan_reviews(&self, session_id: &str) -> anyhow::Result<Vec<PlanRow>> {
        let sid = session_id.to_string();
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(&format!(
                    "SELECT {} FROM plan_reviews
                     WHERE session_id = ?1
                     ORDER BY plan_id, revision",
                    Self::plan_row_columns()
                ))?;
                let rows = stmt
                    .query_map(rusqlite::params![sid], Self::map_plan_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// CAS decision claim: expectedRevision + status-conditional update
    /// inside one serialized connection call — "double-click, two
    /// windows, retry" all resolve to claim-or-conflict. The decision
    /// intent ({decision, feedback}) is persisted HERE, before any
    /// native relay. Approving an incomplete plan and requesting
    /// changes without feedback are refused outright.
    pub async fn submit_plan_decision(
        &self,
        plan_id: &str,
        expected_revision: i64,
        decision: &str,
        feedback: Option<&str>,
    ) -> anyhow::Result<PlanSubmit> {
        if decision == "request_changes" && feedback.map(str::trim).unwrap_or("").is_empty() {
            return Ok(PlanSubmit::FeedbackRequired);
        }
        if !matches!(decision, "approve" | "request_changes" | "defer") {
            anyhow::bail!("unknown plan decision {decision:?}");
        }
        let pid = plan_id.to_string();
        let decision = decision.to_string();
        let feedback = feedback.map(String::from);
        self.conn
            .call(move |c| {
                let current: Option<PlanRow> = c
                    .query_row(
                        &format!(
                            "SELECT {} FROM plan_reviews
                             WHERE plan_id = ?1 AND revision = ?2",
                            Self::plan_row_columns()
                        ),
                        rusqlite::params![pid, expected_revision],
                        Self::map_plan_row,
                    )
                    .optional()?;
                let Some(current) = current else {
                    return Ok::<PlanSubmit, tokio_rusqlite::Error>(PlanSubmit::NotFound);
                };
                if decision == "approve" && !current.complete {
                    return Ok(PlanSubmit::Incomplete);
                }
                // Only these two states accept a submission; everything
                // else (another reviewer won, terminal, submitting) is
                // a conflict surfaced with the current row.
                if !matches!(current.status.as_str(), "awaiting_review" | "deferred") {
                    return Ok(PlanSubmit::Conflict(Box::new(current)));
                }
                let intent = serde_json::json!({
                    "decision": decision,
                    "feedback": feedback,
                });
                let now = plan_now_ms();
                let claimed = c.execute(
                    "UPDATE plan_reviews
                     SET status = 'submitting', decision = ?1,
                         decision_intent_at = ?2, updated_at = ?2
                     WHERE plan_id = ?3 AND revision = ?4
                       AND status IN ('awaiting_review', 'deferred')",
                    rusqlite::params![intent.to_string(), now, pid, expected_revision],
                )?;
                if claimed == 0 {
                    let current: Option<PlanRow> = c
                        .query_row(
                            &format!(
                                "SELECT {} FROM plan_reviews
                                 WHERE plan_id = ?1 AND revision = ?2",
                                Self::plan_row_columns()
                            ),
                            rusqlite::params![pid, expected_revision],
                            Self::map_plan_row,
                        )
                        .optional()?;
                    return Ok(match current {
                        Some(row) => PlanSubmit::Conflict(Box::new(row)),
                        None => PlanSubmit::NotFound,
                    });
                }
                Ok(PlanSubmit::Claimed {
                    session_id: current.session_id,
                    request_id: current.request_id,
                    previous: current.status,
                })
            })
            .await
            .map_err(Into::into)
    }

    /// Native ack: `submitting` settles into the decision's status and
    /// records the confirmation time.
    pub async fn settle_plan(
        &self,
        plan_id: &str,
        revision: i64,
        decision: &str,
    ) -> anyhow::Result<()> {
        let settled = match decision {
            "approve" => "approved",
            "request_changes" => "changes_requested",
            "defer" => "deferred",
            other => anyhow::bail!("unknown plan decision {other:?}"),
        };
        let pid = plan_id.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE plan_reviews
                     SET status = ?1, applied_at = ?2, updated_at = ?2
                     WHERE plan_id = ?3 AND revision = ?4 AND status = 'submitting'",
                    rusqlite::params![settled, plan_now_ms(), pid, revision],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Failed native relay: the decision intent stays recorded
    /// (decision/decision_intent_at untouched) but the row returns to
    /// its pre-claim status — an approval never reads as delivered.
    pub async fn revert_plan(
        &self,
        plan_id: &str,
        revision: i64,
        previous: &str,
    ) -> anyhow::Result<()> {
        let pid = plan_id.to_string();
        let previous = previous.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE plan_reviews SET status = ?1, updated_at = ?2
                     WHERE plan_id = ?3 AND revision = ?4 AND status = 'submitting'",
                    rusqlite::params![previous, plan_now_ms(), pid, revision],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    // ---- Web device pairing ----------------------------------------
    //
    // The pairing key is daemon-minted and single-use: the pair request
    // consumes it AND rotates in a fresh one inside one locked step, so
    // a code is good for exactly one pairing no matter how many devices
    // race it (upstream's compare-and-rotate-under-one-lock).

    /// The active pairing key, minting one if none exists yet.
    pub async fn pairing_key_ensure(&self) -> anyhow::Result<String> {
        self.conn
            .call(move |c| {
                let existing: Option<String> = c
                    .query_row("SELECT key FROM web_pairing WHERE id = 1", [], |r| r.get(0))
                    .optional()?;
                if let Some(key) = existing {
                    return Ok::<String, tokio_rusqlite::Error>(key);
                }
                let key = crate::cli_lifecycle::generate_pairing_key();
                c.execute(
                    "INSERT INTO web_pairing (id, key) VALUES (1, ?1)
                     ON CONFLICT(id) DO UPDATE SET key = ?1, created_at = datetime('now')",
                    rusqlite::params![key],
                )?;
                Ok::<String, tokio_rusqlite::Error>(key)
            })
            .await
            .map_err(Into::into)
    }

    /// Spend the pairing key on one pairing: compare + delete + rotate
    /// under one connection call — a code opens exactly one pairing.
    /// Returns the fresh key for the next device, or None when the
    /// submitted key does not match.
    pub async fn consume_pairing_key(&self, key: &str) -> anyhow::Result<Option<String>> {
        let key = key.trim().to_string();
        self.conn
            .call(move |c| {
                let tx = c.transaction()?;
                let spent = tx.execute(
                    "DELETE FROM web_pairing WHERE id = 1 AND key = ?1",
                    rusqlite::params![key],
                )?;
                if spent == 0 {
                    return Ok::<Option<String>, tokio_rusqlite::Error>(None);
                }
                let fresh = crate::cli_lifecycle::generate_pairing_key();
                tx.execute(
                    "INSERT INTO web_pairing (id, key) VALUES (1, ?1)",
                    rusqlite::params![fresh],
                )?;
                tx.commit()?;
                Ok(Some(fresh))
            })
            .await
            .map_err(Into::into)
    }

    /// Create a pending device row. `token_hash` is the SHA-256 of the
    /// device token (the usable secret never touches the store).
    pub async fn create_device(&self, name: &str, token_hash: &str) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let name = name.trim().to_string();
        let name = if name.is_empty() {
            "unnamed device".to_string()
        } else {
            name
        };
        let (row_id, token_hash) = (id.clone(), token_hash.to_string());
        self.conn
            .call(move |c| -> tokio_rusqlite::Result<()> {
                c.execute(
                    "INSERT INTO web_devices (id, name, token_hash) VALUES (?1, ?2, ?3)",
                    rusqlite::params![row_id, name, token_hash],
                )?;
                Ok(())
            })
            .await
            .map_err(anyhow::Error::from)?;
        Ok(id)
    }

    /// Approve a pending device — `approved_at` is what lets its token
    /// through ws_ticket. Only transitions from pending.
    pub async fn approve_device(&self, id: &str) -> anyhow::Result<bool> {
        let id = id.to_string();
        self.conn
            .call(move |c| {
                let n = c.execute(
                    "UPDATE web_devices SET status = 'approved', approved_at = datetime('now')
                     WHERE id = ?1 AND status = 'pending'",
                    rusqlite::params![id],
                )?;
                Ok::<bool, tokio_rusqlite::Error>(n > 0)
            })
            .await
            .map_err(Into::into)
    }

    /// Revoke (or deny) a device: the row goes, so the token hash goes
    /// with it — the credential is dead immediately, no grace period.
    pub async fn revoke_device(&self, id: &str) -> anyhow::Result<bool> {
        let id = id.to_string();
        self.conn
            .call(move |c| {
                let n = c.execute(
                    "DELETE FROM web_devices WHERE id = ?1",
                    rusqlite::params![id],
                )?;
                Ok::<bool, tokio_rusqlite::Error>(n > 0)
            })
            .await
            .map_err(Into::into)
    }

    /// Rename a device (desktop-side label only).
    pub async fn rename_device(&self, id: &str, name: &str) -> anyhow::Result<bool> {
        let (id, name) = (id.to_string(), name.trim().to_string());
        if name.is_empty() {
            return Ok(false);
        }
        self.conn
            .call(move |c| {
                let n = c.execute(
                    "UPDATE web_devices SET name = ?2 WHERE id = ?1",
                    rusqlite::params![id, name],
                )?;
                Ok::<bool, tokio_rusqlite::Error>(n > 0)
            })
            .await
            .map_err(Into::into)
    }

    /// Every device, oldest first — the desktop's device list.
    pub async fn devices(&self) -> anyhow::Result<Vec<DeviceRow>> {
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, name, status, created_at, approved_at, last_seen
                     FROM web_devices ORDER BY created_at, id",
                )?;
                let rows = stmt
                    .query_map([], |r| {
                        Ok(DeviceRow {
                            id: r.get(0)?,
                            name: r.get(1)?,
                            status: r.get(2)?,
                            created_at: r.get(3)?,
                            approved_at: r.get(4)?,
                            last_seen: r.get(5)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// Device-token check for ws_ticket: the SHA-256 must match an
    /// APPROVED row. Pending rows mint nothing — fail closed.
    pub async fn device_token_ok(&self, token_hash: &str) -> anyhow::Result<bool> {
        let token_hash = token_hash.to_string();
        self.conn
            .call(move |c| {
                let ok: bool = c
                    .query_row(
                        "SELECT 1 FROM web_devices
                         WHERE token_hash = ?1 AND status = 'approved'",
                        rusqlite::params![token_hash],
                        |r| r.get::<_, i64>(0).map(|_| true),
                    )
                    .optional()?
                    .unwrap_or(false);
                Ok::<bool, tokio_rusqlite::Error>(ok)
            })
            .await
            .map_err(Into::into)
    }

    /// Pair-status poll: the row's status when id AND token hash match,
    /// None otherwise (the handler reads None as "revoked" — a device
    /// that proves nothing gets no state).
    pub async fn device_pair_status(
        &self,
        id: &str,
        token_hash: &str,
    ) -> anyhow::Result<Option<String>> {
        let (id, token_hash) = (id.to_string(), token_hash.to_string());
        self.conn
            .call(move |c| {
                let status: Option<String> = c
                    .query_row(
                        "SELECT status FROM web_devices WHERE id = ?1 AND token_hash = ?2",
                        rusqlite::params![id, token_hash],
                        |r| r.get(0),
                    )
                    .optional()?;
                Ok::<Option<String>, tokio_rusqlite::Error>(status)
            })
            .await
            .map_err(Into::into)
    }

    /// Throttled last-seen: at most one write per minute per device —
    /// every ticket mint would otherwise rewrite the row.
    pub async fn touch_device(&self, token_hash: &str) -> anyhow::Result<()> {
        let token_hash = token_hash.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE web_devices SET last_seen = datetime('now')
                     WHERE token_hash = ?1
                       AND (last_seen IS NULL OR last_seen < datetime('now', '-60 seconds'))",
                    rusqlite::params![token_hash],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    // ---- Native-history scanner ------------------------------------

    /// The stored stat signature for one discovered native file, if the
    /// scanner has seen it before.
    pub async fn scanned_signature(
        &self,
        backend: &str,
        native_handle: &str,
    ) -> anyhow::Result<Option<(Option<i64>, Option<i64>)>> {
        let (backend, native_handle) = (backend.to_string(), native_handle.to_string());
        self.conn
            .call(move |c| {
                let sig: Option<(Option<i64>, Option<i64>)> = c
                    .query_row(
                        "SELECT size, mtime_ms FROM scanned_native
                         WHERE backend = ?1 AND native_handle = ?2",
                        rusqlite::params![backend, native_handle],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?;
                Ok::<_, tokio_rusqlite::Error>(sig)
            })
            .await
            .map_err(Into::into)
    }

    /// Record one scanned file → session mapping. Atomically refused
    /// (returns false) when a tombstone exists for the handle: the
    /// tombstone check and the insert share one connection call, so a
    /// `session.delete` racing the sweep can never be re-upserted over.
    pub async fn record_scanned(
        &self,
        backend: &str,
        native_handle: &str,
        size: Option<u64>,
        mtime_ms: Option<i64>,
        session_id: &str,
    ) -> anyhow::Result<bool> {
        let (backend, native_handle, session_id) = (
            backend.to_string(),
            native_handle.to_string(),
            session_id.to_string(),
        );
        self.conn
            .call(move |c| {
                let n = c.execute(
                    "INSERT INTO scanned_native
                         (backend, native_handle, size, mtime_ms, session_id)
                     SELECT ?1, ?2, ?3, ?4, ?5
                     WHERE NOT EXISTS (
                         SELECT 1 FROM session_tombstones
                         WHERE backend = ?1 AND native_handle = ?2
                     )
                     ON CONFLICT(backend, native_handle) DO UPDATE SET
                         size = ?3, mtime_ms = ?4, session_id = ?5,
                         scanned_at = datetime('now')",
                    rusqlite::params![
                        backend,
                        native_handle,
                        size.map(|s| s as i64),
                        mtime_ms,
                        session_id
                    ],
                )?;
                Ok::<bool, tokio_rusqlite::Error>(n > 0)
            })
            .await
            .map_err(Into::into)
    }

    /// Every scanned (backend, native_handle → session_id) mapping —
    /// the vanish pass reconciles this against fresh discovery.
    pub async fn scanned_all(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT backend, native_handle, session_id FROM scanned_native
                     ORDER BY scanned_at",
                )?;
                let rows = stmt
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// Drop one scanned mapping (the file vanished AND its session was
    /// deleted — the tombstone makes any re-import impossible, so the
    /// stale mapping would only haunt the vanish pass).
    pub async fn drop_scanned(&self, backend: &str, native_handle: &str) -> anyhow::Result<()> {
        let (backend, native_handle) = (backend.to_string(), native_handle.to_string());
        self.conn
            .call(move |c| {
                c.execute(
                    "DELETE FROM scanned_native WHERE backend = ?1 AND native_handle = ?2",
                    rusqlite::params![backend, native_handle],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Tombstone a deleted session's native handle — the sweep consults
    /// this before creating anything.
    pub async fn add_tombstone(&self, backend: &str, native_handle: &str) -> anyhow::Result<()> {
        let (backend, native_handle) = (backend.to_string(), native_handle.to_string());
        if backend.is_empty() || native_handle.is_empty() {
            return Ok(());
        }
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO session_tombstones (backend, native_handle)
                     VALUES (?1, ?2)",
                    rusqlite::params![backend, native_handle],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Clear a tombstone — a session bound to the same native handle
    /// again is the user's explicit new intent.
    pub async fn clear_tombstone(&self, backend: &str, native_handle: &str) -> anyhow::Result<()> {
        let (backend, native_handle) = (backend.to_string(), native_handle.to_string());
        self.conn
            .call(move |c| {
                c.execute(
                    "DELETE FROM session_tombstones WHERE backend = ?1 AND native_handle = ?2",
                    rusqlite::params![backend, native_handle],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Whether a handle is tombstoned (sweep-side skip check).
    pub async fn is_tombstoned(&self, backend: &str, native_handle: &str) -> anyhow::Result<bool> {
        let (backend, native_handle) = (backend.to_string(), native_handle.to_string());
        self.conn
            .call(move |c| {
                let hit: bool = c
                    .query_row(
                        "SELECT 1 FROM session_tombstones WHERE backend = ?1 AND native_handle = ?2",
                        rusqlite::params![backend, native_handle],
                        |r| r.get::<_, i64>(0).map(|_| true),
                    )
                    .optional()?
                    .unwrap_or(false);
                Ok::<bool, tokio_rusqlite::Error>(hit)
            })
            .await
            .map_err(Into::into)
    }

    // ---- Mission DAG runner -----------------------------------------

    /// Store a flow definition snapshot: a re-create of the same
    /// flow_id inserts the NEXT version (drafts and runs are isolated —
    /// runs keep the snapshot they started with).
    pub async fn create_mission_flow(
        &self,
        flow_id: &str,
        name: &str,
        definition: &str,
    ) -> anyhow::Result<u32> {
        let (flow_id, name, definition) = (
            flow_id.to_string(),
            name.to_string(),
            definition.to_string(),
        );
        self.conn
            .call(move |c| {
                let next: u32 = c
                    .query_row(
                        "SELECT COALESCE(MAX(version), 0) + 1
                         FROM mission_flows WHERE flow_id = ?1",
                        rusqlite::params![flow_id],
                        |r| r.get(0),
                    )
                    .map_err(tokio_rusqlite::Error::from)?;
                c.execute(
                    "INSERT INTO mission_flows (flow_id, version, name, definition)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![flow_id, next, name, definition],
                )
                .map_err(tokio_rusqlite::Error::from)?;
                Ok::<u32, tokio_rusqlite::Error>(next)
            })
            .await
            .map_err(Into::into)
    }

    /// The latest snapshot of a flow: (version, name, definition JSON).
    pub async fn latest_mission_flow(
        &self,
        flow_id: &str,
    ) -> anyhow::Result<Option<(u32, String, String)>> {
        let flow_id = flow_id.to_string();
        self.conn
            .call(move |c| {
                let row = c
                    .query_row(
                        "SELECT version, name, definition FROM mission_flows
                         WHERE flow_id = ?1 ORDER BY version DESC LIMIT 1",
                        rusqlite::params![flow_id],
                        |r| {
                            Ok((
                                r.get::<_, u32>(0)?,
                                r.get::<_, String>(1)?,
                                r.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(tokio_rusqlite::Error::from)?;
                Ok::<_, tokio_rusqlite::Error>(row)
            })
            .await
            .map_err(Into::into)
    }

    /// Create a run carrying a deep-cloned snapshot of the definition.
    pub async fn create_mission_run(
        &self,
        run_id: &str,
        flow_id: &str,
        version: u32,
        snapshot: &str,
    ) -> anyhow::Result<()> {
        let (run_id, flow_id, version, snapshot) = (
            run_id.to_string(),
            flow_id.to_string(),
            version,
            snapshot.to_string(),
        );
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT INTO mission_runs (run_id, flow_id, version, snapshot)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![run_id, flow_id, version, snapshot],
                )
                .map_err(tokio_rusqlite::Error::from)?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// The run's snapshot + flags: (snapshot, flow_id, interrupted,
    /// ended_at).
    /// The run row: (snapshot, flow_id, version, interrupted, ended,
    /// cancelled).
    pub async fn mission_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Option<(String, String, u32, bool, bool, bool)>> {
        let run_id = run_id.to_string();
        self.conn
            .call(move |c| {
                let row = c
                    .query_row(
                        "SELECT snapshot, flow_id, version, interrupted,
                                (ended_at IS NOT NULL) AS ended, cancelled
                         FROM mission_runs WHERE run_id = ?1",
                        rusqlite::params![run_id],
                        |r| {
                            Ok((
                                r.get::<_, String>(0)?,
                                r.get::<_, String>(1)?,
                                r.get::<_, u32>(2)?,
                                r.get::<_, bool>(3)?,
                                r.get::<_, bool>(4)?,
                                r.get::<_, bool>(5)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(tokio_rusqlite::Error::from)?;
                Ok::<_, tokio_rusqlite::Error>(row)
            })
            .await
            .map_err(Into::into)
    }

    /// All runs, newest first: (run_id, flow_id, version, interrupted,
    /// ended, cancelled, created_at).
    pub async fn mission_runs_list(
        &self,
    ) -> anyhow::Result<Vec<(String, String, u32, bool, bool, bool, String)>> {
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT run_id, flow_id, version, interrupted,
                            (ended_at IS NOT NULL) AS ended, cancelled, created_at
                     FROM mission_runs ORDER BY created_at DESC, run_id DESC",
                )?;
                let rows = stmt
                    .query_map([], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, u32>(2)?,
                            r.get::<_, bool>(3)?,
                            r.get::<_, bool>(4)?,
                            r.get::<_, bool>(5)?,
                            r.get::<_, String>(6)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(tokio_rusqlite::Error::from)?;
                Ok::<_, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// Insert one queued task instance.
    pub async fn insert_mission_task(&self, run_id: &str, node_id: &str) -> anyhow::Result<()> {
        let (run_id, node_id) = (run_id.to_string(), node_id.to_string());
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO mission_tasks (task_id, run_id, node_id)
                     VALUES (?1, ?2, ?3)",
                    rusqlite::params![format!("{run_id}:{node_id}"), run_id, node_id],
                )
                .map_err(tokio_rusqlite::Error::from)?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Task rows for a run, node order: (node_id, status, attempt,
    /// output, error).
    pub async fn mission_tasks(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Vec<(String, String, u32, Option<String>, Option<String>)>> {
        let run_id = run_id.to_string();
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT node_id, status, attempt, output, error FROM mission_tasks
                     WHERE run_id = ?1 ORDER BY node_id",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![run_id], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, u32>(2)?,
                            r.get::<_, Option<String>>(3)?,
                            r.get::<_, Option<String>>(4)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(tokio_rusqlite::Error::from)?;
                Ok::<_, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// Persist one task transition. `output`/`error` are written when
    /// present; `started`/`ended` stamp the timestamps when the caller
    /// says the transition touches them.
    pub async fn set_mission_task_status(
        &self,
        run_id: &str,
        node_id: &str,
        transition: MissionTaskTransition,
    ) -> anyhow::Result<()> {
        let task_id = format!("{run_id}:{node_id}");
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE mission_tasks SET status = ?1,
                         attempt = ?2,
                         output = COALESCE(?3, output),
                         error = COALESCE(?4, error),
                         started_at = CASE WHEN ?5 THEN datetime('now') ELSE started_at END,
                         ended_at = CASE WHEN ?6 THEN datetime('now') ELSE ended_at END
                     WHERE task_id = ?7",
                    rusqlite::params![
                        transition.status,
                        transition.attempt,
                        transition.output,
                        transition.error,
                        transition.stamp_started as i64,
                        transition.stamp_ended as i64,
                        task_id
                    ],
                )
                .map_err(tokio_rusqlite::Error::from)?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// The human inbox: one item per waiting human task.
    pub async fn insert_mission_inbox(&self, run_id: &str, node_id: &str) -> anyhow::Result<()> {
        let (run_id, node_id) = (run_id.to_string(), node_id.to_string());
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT INTO mission_inbox (task_id, run_id, node_id)
                     VALUES (?1, ?2, ?3)",
                    rusqlite::params![format!("{run_id}:{node_id}"), run_id, node_id],
                )
                .map_err(tokio_rusqlite::Error::from)?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Resolve one inbox item — CAS on `pending`, so of two answers
    /// exactly one wins. Returns false when the item is already
    /// resolved (or unknown).
    pub async fn resolve_mission_inbox(
        &self,
        run_id: &str,
        node_id: &str,
        decision: &str,
        feedback: Option<&str>,
    ) -> anyhow::Result<bool> {
        let task_id = format!("{run_id}:{node_id}");
        let (decision, feedback) = (decision.to_string(), feedback.map(String::from));
        self.conn
            .call(move |c| {
                let n = c.execute(
                    "UPDATE mission_inbox SET status = 'resolved', decision = ?1,
                         feedback = ?2, resolved_at = datetime('now')
                     WHERE id = (
                         SELECT id FROM mission_inbox
                         WHERE task_id = ?3 AND status = 'pending'
                         ORDER BY id LIMIT 1
                     )",
                    rusqlite::params![decision, feedback, task_id],
                )?;
                Ok::<bool, tokio_rusqlite::Error>(n > 0)
            })
            .await
            .map_err(Into::into)
    }

    /// Inbox items for a run: (node_id, status, decision, feedback).
    pub async fn mission_inbox_for_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Vec<(String, String, Option<String>, Option<String>)>> {
        let run_id = run_id.to_string();
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT node_id, status, decision, feedback FROM mission_inbox
                     WHERE run_id = ?1 ORDER BY created_at",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![run_id], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, Option<String>>(2)?,
                            r.get::<_, Option<String>>(3)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(tokio_rusqlite::Error::from)?;
                Ok::<_, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// Boot recovery: every run with unfinished work that has no
    /// `ended_at` is crash-orphaned — mark `interrupted`, cancel its
    /// non-terminal tasks, and close its pending inbox items. Returns
    /// the recovered run ids.
    pub async fn recover_interrupted_mission_runs(
        &self,
        live_run_ids: Vec<String>,
    ) -> anyhow::Result<Vec<String>> {
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT run_id FROM mission_runs
                     WHERE ended_at IS NULL AND interrupted = 0
                       AND EXISTS (SELECT 1 FROM mission_tasks
                                   WHERE run_id = mission_runs.run_id
                                     AND status IN ('queued', 'running',
                                                    'waiting_human'))",
                )?;
                let runs: Vec<String> = stmt
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(tokio_rusqlite::Error::from)?
                    .into_iter()
                    .filter(|id| !live_run_ids.contains(id))
                    .collect();
                for run in &runs {
                    c.execute(
                        "UPDATE mission_runs SET interrupted = 1 WHERE run_id = ?1",
                        rusqlite::params![run],
                    )
                    .map_err(tokio_rusqlite::Error::from)?;
                    c.execute(
                        "UPDATE mission_tasks SET status = 'cancelled', ended_at = datetime('now')
                         WHERE run_id = ?1
                           AND status IN ('queued', 'running', 'waiting_human')",
                        rusqlite::params![run],
                    )
                    .map_err(tokio_rusqlite::Error::from)?;
                    c.execute(
                        "UPDATE mission_inbox SET status = 'resolved',
                             decision = 'cancelled', resolved_at = datetime('now')
                         WHERE run_id = ?1 AND status = 'pending'",
                        rusqlite::params![run],
                    )
                    .map_err(tokio_rusqlite::Error::from)?;
                }
                Ok::<_, tokio_rusqlite::Error>(runs)
            })
            .await
            .map_err(Into::into)
    }

    /// Mark a run `interrupted` (crash honesty) — used by boot recovery.
    pub async fn mark_mission_interrupted(&self, run_id: &str) -> anyhow::Result<()> {
        let run_id = run_id.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE mission_runs SET interrupted = 1 WHERE run_id = ?1",
                    rusqlite::params![run_id],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Mark a run ended (all tasks terminal).
    pub async fn mark_mission_ended(&self, run_id: &str) -> anyhow::Result<()> {
        let run_id = run_id.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE mission_runs SET ended_at = datetime('now') WHERE run_id = ?1",
                    rusqlite::params![run_id],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Mark a run explicitly cancelled (user cancel — distinct from the
    /// crash-honest `interrupted`).
    pub async fn mark_mission_run_cancelled(&self, run_id: &str) -> anyhow::Result<()> {
        let run_id = run_id.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE mission_runs SET cancelled = 1,
                         ended_at = COALESCE(ended_at, datetime('now'))
                     WHERE run_id = ?1",
                    rusqlite::params![run_id],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Turn ended: the native ask died with it, so still-open reviews
    /// expire (history must not pretend they are answerable). Decision
    /// intents already on file stay. Returns the expired count.
    pub async fn expire_open_plans(&self, session_id: &str) -> anyhow::Result<usize> {
        let sid = session_id.to_string();
        self.conn
            .call(move |c| {
                let n = c.execute(
                    "UPDATE plan_reviews SET status = 'expired', updated_at = ?1
                     WHERE session_id = ?2
                       AND status IN ('awaiting_review', 'submitting', 'deferred')",
                    rusqlite::params![plan_now_ms(), sid],
                )?;
                Ok::<usize, tokio_rusqlite::Error>(n)
            })
            .await
            .map_err(Into::into)
    }

    /// Delete a project. Refuses while sessions still reference it —
    /// their history is the project's; orphaning silently loses the
    /// grouping. The count rides the error message.
    pub async fn delete_project(&self, id: &str) -> anyhow::Result<()> {
        let id = id.to_string();
        let id_for_count = id.clone();
        let bound: i64 = self
            .conn
            .call(move |c| {
                c.query_row(
                    "SELECT COUNT(*) FROM sessions WHERE project_id = ?1",
                    [&id_for_count],
                    |r| r.get(0),
                )
                .map_err(tokio_rusqlite::Error::from)
            })
            .await?;
        anyhow::ensure!(
            bound == 0,
            "project still has {bound} session(s) — delete or reassign them first"
        );
        self.conn
            .call(move |c| {
                c.execute("DELETE FROM projects WHERE id = ?1", [&id])?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    pub async fn delete_channel_state(&self, conv_id: &str, key: &str) -> anyhow::Result<()> {
        let (conv, key) = (conv_id.to_string(), key.to_string());
        self.conn
            .call(move |c| {
                c.execute(
                    "DELETE FROM channel_state WHERE conv_id = ?1 AND key = ?2",
                    rusqlite::params![conv, key],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }
    /// Archive/unarchive a session — archived rows are hidden from
    /// default listings (pass `includeArchived` to see them) and the
    /// retention sweep skips them. Returns whether a row changed.
    pub async fn set_archived(&self, id: &str, archived: bool) -> anyhow::Result<bool> {
        let sid = id.to_string();
        Ok(self
            .conn
            .call(move |c| {
                Ok::<bool, tokio_rusqlite::Error>(
                    c.execute(
                        "UPDATE sessions SET archived = ?2 WHERE id = ?1",
                        rusqlite::params![sid, archived as i64],
                    )? != 0,
                )
            })
            .await?)
    }

    /// Mark a session active now — the retention sweep's idle clock.
    pub async fn touch(&self, id: &str) -> anyhow::Result<()> {
        let sid = id.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE sessions SET last_active_at = datetime('now') WHERE id = ?1",
                    [sid],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// All sessions as `SessionSummary` rows. Paginated; ordered by
    /// `created_at, id` (oldest first — clients slice newest-side
    /// themselves). `filter` narrows by backend (agent id), cwd, and
    /// tag membership — set fields AND together;
    /// `SessionFilter::default()` lists everything. backend/cwd match
    /// the stored values exactly; the tag filter matches membership
    /// via json_each over the JSON-array column (no index — the
    /// sessions table is small and tag queries are opt-in).
    pub async fn list_sessions_paged(
        &self,
        limit: u32,
        offset: u32,
        filter: SessionFilter,
    ) -> anyhow::Result<Vec<SessionSummary>> {
        self.conn
            .call(move |c| {
                let mut sql = String::from(
                    "SELECT id, created_at, COALESCE(agent, ''), COALESCE(title, ''),
                            cwd, COALESCE(tags, '[]'), model,
                            COALESCE(pinned, 0), COALESCE(archived, 0)
                     FROM sessions",
                );
                let mut conds: Vec<String> = Vec::new();
                let mut binds: Vec<&dyn rusqlite::ToSql> = Vec::new();
                if let Some(b) = &filter.backend {
                    binds.push(b);
                    conds.push(format!("COALESCE(agent, '') = ?{}", binds.len()));
                }
                if let Some(w) = &filter.cwd {
                    binds.push(w);
                    conds.push(format!("cwd = ?{}", binds.len()));
                }
                if let Some(p) = &filter.project_id {
                    binds.push(p);
                    conds.push(format!("project_id = ?{}", binds.len()));
                }
                if let Some(t) = &filter.tag {
                    binds.push(t);
                    conds.push(format!(
                        "EXISTS (SELECT 1 FROM json_each(COALESCE(tags, '[]'))
                          WHERE value = ?{})",
                        binds.len()
                    ));
                }
                // Archived sessions are reference material — default
                // listings hide them.
                if !filter.include_archived {
                    conds.push("COALESCE(archived, 0) = 0".to_string());
                }
                if !conds.is_empty() {
                    sql.push_str(" WHERE ");
                    sql.push_str(&conds.join(" AND "));
                }
                // Pinned sessions float to the front; within a pin
                // group the order is unchanged.
                sql.push_str(" ORDER BY COALESCE(pinned, 0) DESC, created_at, id LIMIT ? OFFSET ?");
                binds.push(&limit);
                binds.push(&offset);
                let mut stmt = c.prepare(&sql)?;
                let rows = stmt
                    .query_map(binds.as_slice(), |row| {
                        Ok(SessionSummary {
                            id: row.get(0)?,
                            created_at: row.get(1)?,
                            backend: row.get(2)?,
                            title: row.get(3)?,
                            cwd: row.get(4)?,
                            // A corrupt cell reads as untagged rather
                            // than failing the whole listing.
                            tags: serde_json::from_str(&row.get::<_, String>(5)?)
                                .unwrap_or_default(),
                            model: row.get(6)?,
                            pinned: row.get::<_, i64>(7)? != 0,
                            archived: row.get::<_, i64>(8)? != 0,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<SessionSummary>, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// One session's list-row metadata — the `session.export` header.
    /// None when the session doesn't exist.
    pub async fn session_overview(&self, id: &str) -> anyhow::Result<Option<SessionSummary>> {
        let id = id.to_string();
        Ok(self
            .conn
            .call(move |c| {
                c.query_row(
                    "SELECT id, created_at, COALESCE(agent, ''), COALESCE(title, ''),
                            cwd, COALESCE(tags, '[]'), model,
                            COALESCE(pinned, 0), COALESCE(archived, 0)
                     FROM sessions WHERE id = ?1",
                    [&id],
                    |row| {
                        Ok(SessionSummary {
                            id: row.get(0)?,
                            created_at: row.get(1)?,
                            backend: row.get(2)?,
                            title: row.get(3)?,
                            cwd: row.get(4)?,
                            tags: serde_json::from_str(&row.get::<_, String>(5)?)
                                .unwrap_or_default(),
                            model: row.get(6)?,
                            pinned: row.get::<_, i64>(7)? != 0,
                            archived: row.get::<_, i64>(8)? != 0,
                        })
                    },
                )
                .optional()
            })
            .await?)
    }

    /// Session messages with their row ids, paginated by row id.
    /// Respects the compaction cutoff — rows at or before it are hidden
    /// (the summary row is not injected).
    pub async fn messages_paged(
        &self,
        session_id: &str,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<StoredMessage>> {
        let sid = session_id.to_string();
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, session_id, role, data, ts FROM messages
                     WHERE session_id = ?1
                       AND id > COALESCE(
                           (SELECT compacted_through FROM sessions WHERE id = ?1), 0)
                     ORDER BY id LIMIT ?2 OFFSET ?3",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![sid, limit, offset], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, i64>(4)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<_>, tokio_rusqlite::Error>(rows)
            })
            .await?
            .into_iter()
            .map(|(id, session_id, role, data, ts)| {
                Ok(StoredMessage {
                    id,
                    session_id,
                    role,
                    ts,
                    data: serde_json::from_str(&data)?,
                })
            })
            .collect()
    }

    /// Write a consistent snapshot of the whole database to `to` via
    /// `VACUUM INTO`. Safe while the daemon is live — SQLite takes the
    /// snapshot under its normal locking, so WAL writers continue — and
    /// the target must NOT exist: SQLite refuses to overwrite, which is
    /// exactly the backup semantics we want. The result is a compacted,
    /// self-contained file (no -wal/-shm sidecars needed to read it).
    pub async fn backup(&self, to: &Path) -> anyhow::Result<()> {
        let dest = to.display().to_string();
        self.conn
            .call(move |c| {
                // VACUUM INTO takes its filename as a parameter-bound
                // expression; it cannot run inside a transaction, and
                // conn.call closures never open one implicitly.
                c.execute("VACUUM INTO ?1", rusqlite::params![dest])?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        // The snapshot holds the same transcripts — force the same
        // private mode as the live database file.
        #[cfg(unix)]
        tighten_permissions(to, 0o600);
        Ok(())
    }

    /// Duplicate a session: new id, copied row fields (cwd/model/agent/
    /// tags, title suffixed " (fork)"), copied messages — optionally
    /// only up to and including `upto` (an original message id) — and
    /// copied usage rows retargeted to the new id, so the fork's
    /// cost/context rollups and its next `record_usage` delta baseline
    /// stay coherent with the copied conversation. The agent's own
    /// session id is NOT copied: the fork must attach a fresh agent
    /// session (the agent cannot branch its own context). Copied
    /// messages keep their original ts and are re-indexed for FTS
    /// under their new ids, so a fork's history is searchable. One
    /// transaction — a crash mid-fork leaves no half-copied session.
    pub async fn fork_session(&self, from: &str, upto: Option<i64>) -> anyhow::Result<String> {
        let from = from.to_string();
        let new_id = uuid::Uuid::new_v4().to_string();
        let nid = new_id.clone();
        self.conn
            .call(move |c| {
                let tx = c.transaction()?;
                let row = tx
                    .query_row(
                        "SELECT cwd, model, agent, agent_session, title, COALESCE(tags, '[]')
                         FROM sessions WHERE id = ?1",
                        rusqlite::params![from],
                        |r| {
                            Ok((
                                r.get::<_, String>(0)?,
                                r.get::<_, Option<String>>(1)?,
                                r.get::<_, Option<String>>(2)?,
                                r.get::<_, Option<String>>(3)?,
                                r.get::<_, Option<String>>(4)?,
                                r.get::<_, String>(5)?,
                            ))
                        },
                    )
                    .optional()?
                    .ok_or_else(|| rusqlite::Error::QueryReturnedNoRows)?;
                let (cwd, model, agent, agent_session, title, tags) = row;
                let new_title = title.map(|t| format!("{t} (fork)"));
                // The persistence handle rides along so the fork is
                // resumable: session.resume on it reattaches the same
                // native session. Both rows share the handle only
                // until each runs its next turn — run_turn rebinds a
                // session to its own fresh native handle afterwards
                // (see set_agent_session), so the sharing window is
                // bounded. Note `upto` bounds the copied *store*
                // history only; the native transcript replays in full
                // on resume — it is the provider's, not ours to cut.
                tx.execute(
                    "INSERT INTO sessions
                         (id, created_at, last_active_at, cwd, model, agent,
                          agent_session, title, tags)
                     VALUES (?1, datetime('now'), datetime('now'), ?2, ?3, ?4,
                             ?5, ?6, ?7)",
                    rusqlite::params![nid, cwd, model, agent, agent_session, new_title, tags],
                )?;
                let mut stmt = tx.prepare(
                    "SELECT id, role, data, ts FROM messages WHERE session_id = ?1
                       AND (?2 IS NULL OR id <= ?2) ORDER BY id",
                )?;
                let msgs: Vec<(i64, String, String, i64)> = stmt
                    .query_map(rusqlite::params![from, upto], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
                    })?
                    .collect::<Result<_, _>>()?;
                drop(stmt);
                for (_old_id, role, data, ts) in msgs {
                    tx.execute(
                        "INSERT INTO messages (session_id, role, data, ts)
                         VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![nid, role, data, ts],
                    )?;
                    let mid = tx.last_insert_rowid();
                    let v: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
                    if let Some(text) = fts_text(&role, &v) {
                        tx.execute(
                            "INSERT INTO messages_fts (content, session_id, message_id)
                                 VALUES (?1, ?2, ?3)",
                            rusqlite::params![text, nid, mid],
                        )?;
                    }
                }
                // Retargeted usage copy — see doc comment. `upto`
                // bounds messages only: usage is session-level state,
                // and a fork that later runs its own turns must see
                // the full cumulative baseline.
                tx.execute(
                    "INSERT INTO usage
                         (session_id, model, context_used, context_size,
                          cost_usd, cumulative_cost, created_at)
                     SELECT ?1, model, context_used, context_size,
                            cost_usd, cumulative_cost, created_at
                     FROM usage WHERE session_id = ?2",
                    rusqlite::params![nid, from],
                )?;
                tx.commit()?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(|e| {
                // QueryReturnedNoRows surfaces as a bare rusqlite error —
                // translate it into the message callers expect.
                if e.to_string().contains("QueryReturnedNoRows") {
                    anyhow::anyhow!("session not found")
                } else {
                    e.into()
                }
            })?;
        Ok(new_id)
    }
}

#[cfg(test)]
mod tests {
    use super::{FtsQuery, Store};

    fn q(input: &str) -> FtsQuery {
        Store::fts5_query(input).expect("input should leave something searchable")
    }

    /// Sub-trigram terms move to the LIKE path; everything else stays
    /// a quoted phrase on the FTS path. Mixed queries keep both.
    #[test]
    fn fts5_query_splits_short_terms_onto_the_like_path() {
        // Pure short-token query: no MATCH at all.
        assert_eq!(
            q("커밋"),
            FtsQuery {
                match_expr: String::new(),
                like_terms: vec!["커밋".into()],
                like_not_terms: vec![]
            }
        );
        // Mixed: long term keeps FTS (with rank/snippet), short term
        // rides along as a substring conjunct.
        assert_eq!(
            q("error 커밋"),
            FtsQuery {
                match_expr: "\"error\"".into(),
                like_terms: vec!["커밋".into()],
                like_not_terms: vec![]
            }
        );
        // Two short bare tokens: both must match (AND semantics).
        assert_eq!(
            q("ab cd"),
            FtsQuery {
                match_expr: String::new(),
                like_terms: vec!["ab".into(), "cd".into()],
                like_not_terms: vec![]
            }
        );
        // A short quoted phrase is equally invisible to trigram.
        assert_eq!(
            q("\"timeout in relay\" ab"),
            FtsQuery {
                match_expr: "\"timeout in relay\"".into(),
                like_terms: vec!["ab".into()],
                like_not_terms: vec![]
            }
        );
    }

    /// NOT over a short term becomes an exclusion conjunct without
    /// stranding a dangling NOT in the MATCH expression.
    #[test]
    fn fts5_query_keeps_short_not_operands_as_exclusions() {
        assert_eq!(
            q("error NOT 커밋"),
            FtsQuery {
                match_expr: "\"error\"".into(),
                like_terms: vec![],
                like_not_terms: vec!["커밋".into()]
            }
        );
    }

    /// Long-term behavior is unchanged: quoted phrases, prefix
    /// queries, operator sanitization.
    #[test]
    fn fts5_query_keeps_long_terms_phrases_and_prefix_on_fts_path() {
        let query = q("\"timeout in relay\"");
        assert_eq!(query.match_expr, "\"timeout in relay\"");
        assert!(query.like_terms.is_empty() && query.like_not_terms.is_empty());

        assert_eq!(q("error*").match_expr, "\"error\"*");
        // Leading/trailing operators drop; consecutive collapse.
        assert_eq!(
            q("AND error OR OR timeout").match_expr,
            "\"error\" OR \"timeout\""
        );
        // `x AND NOT y` rewrites to the binary-exclusion form.
        assert_eq!(
            q("error AND NOT timeout").match_expr,
            "\"error\" NOT \"timeout\""
        );
        // A 3-char term is exactly at the trigram minimum — FTS path.
        assert_eq!(q("abc").match_expr, "\"abc\"");
        // Prefix star on a short stem: substring conjunct (superset of
        // the prefix), not a dropped token.
        assert_eq!(q("ab*").like_terms, vec!["ab"]);
    }

    /// Operator-only or wildcard-only input is nothing searchable.
    #[test]
    fn fts5_query_returns_none_for_operator_only_input() {
        for empty in ["AND OR", "NOT", "\"\"", "*"] {
            assert!(Store::fts5_query(empty).is_none(), "{empty:?}");
        }
        // A trailing wildcard-only term is noise, not a dangling
        // operator: the query degrades to its searchable part instead
        // of producing an unparsable MATCH (or a 500).
        assert_eq!(q("error AND *").match_expr, "\"error\"");
    }
}
