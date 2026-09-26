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
    /// OpenAI-shaped message JSON: content, tool_calls, tool_call_id, name.
    pub data: Value,
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
                     title TEXT
                 );
                 CREATE TABLE IF NOT EXISTS messages (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     session_id TEXT NOT NULL REFERENCES sessions(id),
                     role TEXT NOT NULL,
                     data TEXT NOT NULL
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
                 CREATE INDEX IF NOT EXISTS idx_usage_session
                     ON usage(session_id, id);",
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
                     title TEXT
                 );
                 CREATE TABLE messages (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     session_id TEXT NOT NULL REFERENCES sessions(id),
                     role TEXT NOT NULL,
                     data TEXT NOT NULL
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
                    ON usage(session_id, id);",
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
        self.conn
            .call(move |c| {
                let tx = c.transaction()?;
                tx.execute(
                    "INSERT INTO messages (session_id, role, data) VALUES (?1, ?2, ?3)",
                    rusqlite::params![sid, role, data],
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
    /// message_id, snippet) ordered by rank, with optional narrowing:
    /// `session_id` restricts hits to
    /// one session; `before`/`after` bound the SESSION's `created_at`
    /// inclusively (messages carry no timestamps, so the session's
    /// creation is the coarsest honest bound). ISO-8601 strings — a `T`
    /// date/time separator is normalized to the space form
    /// `datetime('now')` writes, so plain string comparison holds;
    /// date-only bounds compare as their midnight.
    pub async fn search_filtered(
        &self,
        query: &str,
        limit: usize,
        session_id: Option<&str>,
        before: Option<String>,
        after: Option<String>,
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
        let norm = |o: Option<String>| {
            o.map(|s| s.trim().replacen('T', " ", 1))
                .filter(|s| !s.is_empty())
        };
        let (before, after) = (norm(before), norm(after));
        let sid = session_id.map(String::from);
        self.conn
            .call(move |c| {
                let mut sql = String::from(
                    "SELECT messages_fts.session_id, messages_fts.message_id,
                            snippet(messages_fts, 0, char(1), char(2), '…', 32)
                     FROM messages_fts
                     JOIN sessions ON sessions.id = messages_fts.session_id
                     WHERE messages_fts MATCH ?1",
                );
                let mut idx = 2;
                if sid.is_some() {
                    sql.push_str(&format!(" AND messages_fts.session_id = ?{idx}"));
                    idx += 1;
                }
                if before.is_some() {
                    sql.push_str(&format!(" AND sessions.created_at <= ?{idx}"));
                    idx += 1;
                }
                if after.is_some() {
                    sql.push_str(&format!(" AND sessions.created_at >= ?{idx}"));
                    idx += 1;
                }
                sql.push_str(&format!(" ORDER BY rank LIMIT ?{idx}"));
                let mut stmt = c.prepare(&sql)?;
                let mut binds: Vec<&dyn rusqlite::ToSql> = vec![&query];
                if let Some(s) = &sid {
                    binds.push(s);
                }
                if let Some(b) = &before {
                    binds.push(b);
                }
                if let Some(a) = &after {
                    binds.push(a);
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
                    out.push(format!("\"{}\"", term.replace('"', "\"\"")));
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
    pub async fn usage_summary(&self) -> anyhow::Result<Vec<(String, u64, u64, f64, u64)>> {
        self.conn
            .call(|c| {
                let mut stmt = c.prepare(
                    "SELECT model, context_used, context_size, cost_usd FROM usage
                     WHERE id IN (SELECT MAX(id) FROM usage GROUP BY model)",
                )?;
                let latest: Vec<(String, i64, i64, f64)> = stmt
                    .query_map([], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })?
                    .collect::<Result<_, _>>()?;
                let mut stmt =
                    c.prepare("SELECT model, SUM(cost_usd), COUNT(*) FROM usage GROUP BY model")?;
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
                       AND id NOT IN (SELECT value FROM json_each(?2))",
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

    /// All sessions as `(id, created_at, agent, title)` — agent/title
    /// are empty strings when unset. Paginated; ordered by `created_at, id`.
    pub async fn list_sessions_paged(
        &self,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<(String, String, String, String)>> {
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, created_at, COALESCE(agent, ''), COALESCE(title, '')
                     FROM sessions ORDER BY created_at, id LIMIT ?1 OFFSET ?2",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![limit, offset], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<(String, String, String, String)>, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
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
                    "SELECT id, session_id, role, data FROM messages
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
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<_>, tokio_rusqlite::Error>(rows)
            })
            .await?
            .into_iter()
            .map(|(id, session_id, role, data)| {
                Ok(StoredMessage {
                    id,
                    session_id,
                    role,
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
    /// title suffixed " (fork)"), and copied messages — optionally only
    /// up to and including `upto` (an original message id). The agent's
    /// own session id is NOT copied: the fork must attach a fresh agent
    /// session (the agent cannot branch its own context). Copied
    /// messages are re-indexed for FTS under their new ids, so a fork's
    /// history is searchable. One transaction — a crash mid-fork leaves
    /// no half-copied session.
    pub async fn fork_session(&self, from: &str, upto: Option<i64>) -> anyhow::Result<String> {
        let from = from.to_string();
        let new_id = uuid::Uuid::new_v4().to_string();
        let nid = new_id.clone();
        self.conn
            .call(move |c| {
                let tx = c.transaction()?;
                let row = tx
                    .query_row(
                        "SELECT cwd, model, agent, title FROM sessions WHERE id = ?1",
                        rusqlite::params![from],
                        |r| {
                            Ok((
                                r.get::<_, String>(0)?,
                                r.get::<_, Option<String>>(1)?,
                                r.get::<_, Option<String>>(2)?,
                                r.get::<_, Option<String>>(3)?,
                            ))
                        },
                    )
                    .optional()?
                    .ok_or_else(|| rusqlite::Error::QueryReturnedNoRows)?;
                let (cwd, model, agent, title) = row;
                let new_title = title.map(|t| format!("{t} (fork)"));
                tx.execute(
                    "INSERT INTO sessions (id, created_at, last_active_at, cwd, model, agent, title)
                     VALUES (?1, datetime('now'), datetime('now'), ?2, ?3, ?4, ?5)",
                    rusqlite::params![nid, cwd, model, agent, new_title],
                )?;
                let mut stmt = tx.prepare(
                    "SELECT id, role, data FROM messages WHERE session_id = ?1
                       AND (?2 IS NULL OR id <= ?2) ORDER BY id",
                )?;
                let msgs: Vec<(i64, String, String)> = stmt
                    .query_map(rusqlite::params![from, upto], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                    })?
                    .collect::<Result<_, _>>()?;
                drop(stmt);
                for (_old_id, role, data) in msgs {
                    tx.execute(
                        "INSERT INTO messages (session_id, role, data) VALUES (?1, ?2, ?3)",
                        rusqlite::params![nid, role, data],
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
