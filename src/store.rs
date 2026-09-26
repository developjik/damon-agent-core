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
#[derive(Debug, Clone)]
pub struct ProjectRow {
    pub id: String,
    pub name: String,
    pub root: String,
    pub defaults: String,
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
                 CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                     content,
                     session_id UNINDEXED,
                     message_id UNINDEXED
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
                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    root TEXT NOT NULL,
                    -- JSON: {backend?, model?, mode?, mcpServers?}
                    defaults TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                 CREATE INDEX IF NOT EXISTS idx_usage_session
                     ON usage(session_id, id);
                 -- agent/agent_session serves the import-dedup lookup
                 -- and the list/search backend filter; usage.created_at
                 -- serves the daily rollup's day-range scan.
                 CREATE INDEX IF NOT EXISTS idx_sessions_agent
                     ON sessions(agent, agent_session);
                 CREATE INDEX IF NOT EXISTS idx_usage_created
                     ON usage(created_at);",
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
                let missing: Vec<(i64, String, String, String)> = {
                    let mut stmt = c.prepare(
                        "SELECT id, session_id, role, data FROM messages
                          WHERE id NOT IN (SELECT message_id FROM messages_fts)",
                    )?;
                    let rows =
                        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
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
                c.execute_batch("PRAGMA user_version = 3;")?;
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
                     message_id UNINDEXED
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
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    root TEXT NOT NULL,
                    defaults TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE INDEX IF NOT EXISTS idx_sessions_agent
                    ON sessions(agent, agent_session);
                CREATE INDEX IF NOT EXISTS idx_usage_created
                    ON usage(created_at);",
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
                let mut sql = String::from(
                    "SELECT messages_fts.session_id, messages_fts.message_id,
                            snippet(messages_fts, 0, char(1), char(2), '…', 32)
                     FROM messages_fts
                     JOIN sessions ON sessions.id = messages_fts.session_id
                     LEFT JOIN messages ON messages.id = messages_fts.message_id
                     WHERE messages_fts MATCH ?1",
                );
                let mut idx = 2;
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
                sql.push_str(&format!(" ORDER BY rank LIMIT ?{idx}"));
                let mut stmt = c.prepare(&sql)?;
                let mut binds: Vec<&dyn rusqlite::ToSql> = vec![&query];
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
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<_>, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// Build a safe FTS5 MATCH expression from user input.
    ///
    /// Token rules:
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
    /// Returns `None` when nothing searchable remains.
    fn fts5_query(input: &str) -> Option<String> {
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
                    // FTS5's NOT is binary and already conjunctive — "x NOT
                    // y" is "x and-not y" — while "x AND NOT y" does not
                    // parse, so NOT replaces any pending AND/OR.
                    if pending_not {
                        pending_op = None;
                        out.push("NOT".to_string());
                        pending_not = false;
                    } else if let Some(op) = pending_op.take() {
                        out.push(op.to_string());
                    }
                    // A trailing `*` makes the term an FTS5 prefix
                    // query (`"tok"*` — prefix on the last token of
                    // the phrase); quote-wrapping alone used to
                    // swallow it, so `error*` searched the literal
                    // token "error" and never matched "errors". A
                    // bare `*` is nothing searchable — drop it.
                    if let Some(stem) = term.strip_suffix('*') {
                        let stem = stem.trim_end_matches('*');
                        if !stem.is_empty() {
                            out.push(format!("\"{}\"*", stem.replace('"', "\"\"")));
                        }
                    } else {
                        out.push(format!("\"{}\"", term.replace('"', "\"\"")));
                    }
                }
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out.join(" "))
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

    /// Create a project. `defaults_json` is stored verbatim (the RPC
    /// layer validates shape); the id is caller-minted (uuid).
    pub async fn create_project(
        &self,
        id: &str,
        name: &str,
        root: &str,
        defaults_json: &str,
    ) -> anyhow::Result<()> {
        let (id, name, root, d) = (
            id.to_string(),
            name.to_string(),
            root.to_string(),
            defaults_json.to_string(),
        );
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT INTO projects (id, name, root, defaults) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![id, name, root, d],
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
                    "SELECT id, name, root, defaults FROM projects WHERE id = ?1",
                    [&id],
                    |r| {
                        Ok(ProjectRow {
                            id: r.get(0)?,
                            name: r.get(1)?,
                            root: r.get(2)?,
                            defaults: r.get(3)?,
                        })
                    },
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
                    "SELECT id, name, root, defaults FROM projects ORDER BY created_at, id",
                )?;
                let rows = stmt
                    .query_map([], |r| {
                        Ok(ProjectRow {
                            id: r.get(0)?,
                            name: r.get(1)?,
                            root: r.get(2)?,
                            defaults: r.get(3)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<ProjectRow>, tokio_rusqlite::Error>(rows)
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
