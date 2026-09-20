use rusqlite::OptionalExtension;
use std::path::Path;

use anyhow::Context;
use serde_json::Value;
use tokio_rusqlite::Connection;

/// SQLite-backed session/message store. Append-only message log;
/// forking and compaction are deliberately out of Phase 2 scope.
#[derive(Clone)]
pub struct Store {
    conn: Connection,
}

#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub id: i64,
    pub session_id: String,
    pub role: String,
    /// OpenAI-shaped message JSON: content, tool_calls, tool_call_id, name.
    pub data: Value,
}
/// Force `mode` on `path`. Session transcripts and tool I/O must not be
/// group/world-accessible regardless of the process umask (SQLite opens
/// 0644 by default) — same threat model as the config file's 0600
/// warning. Runs on every open: a fresh path gets the tight mode, a
/// looser pre-existing one is tightened with a warning; failures only
/// warn so the store still opens.
#[cfg(unix)]
fn tighten_permissions(path: &Path, mode: u32) {
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
                     input_tokens INTEGER NOT NULL DEFAULT 0,
                     output_tokens INTEGER NOT NULL DEFAULT 0,
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
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
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

    /// `model` is the session's default model override (from session/new);
    /// a per-prompt model still wins over it.
    pub async fn create_session(
        &self,
        id: &str,
        cwd: &str,
        model: Option<&str>,
    ) -> anyhow::Result<()> {
        let id = id.to_string();
        let cwd = cwd.to_string();
        let model = model.map(String::from);
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT INTO sessions (id, cwd, model, last_active_at)
                     VALUES (?1, ?2, ?3, datetime('now'))",
                    rusqlite::params![id, cwd, model],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await?;
        Ok(())
    }

    /// The session's stored default model override, if any.
    pub async fn session_model(&self, id: &str) -> anyhow::Result<Option<String>> {
        let id = id.to_string();
        Ok(self
            .conn
            .call(move |c| {
                c.query_row("SELECT model FROM sessions WHERE id = ?1", [id], |r| {
                    r.get::<_, Option<String>>(0)
                })
                .optional()
            })
            .await?
            .flatten())
    }

    /// The session's working directory — the cwd builtin tools run in.
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
                // Index text content for full-text search.
                if let Ok(v) = serde_json::from_str::<Value>(&data)
                    && let Some(text) = v["content"].as_str()
                {
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
    /// message_id, snippet) ordered by rank.
    pub async fn search(
        &self,
        query: &str,
        limit: usize,
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
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT session_id, message_id,
                            snippet(messages_fts, 0, '[', ']', '…', 32)
                     FROM messages_fts
                     WHERE messages_fts MATCH ?1
                     ORDER BY rank
                     LIMIT ?2",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![query, limit as i64], |row| {
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

    /// Messages for a session in insertion order, as OpenAI-shaped JSON.
    /// Respects compaction: messages at or before `compacted_through` are
    /// replaced by the stored summary (as a leading user message).
    pub async fn messages(&self, session_id: &str) -> anyhow::Result<Vec<Value>> {
        let sid = session_id.to_string();
        let (_cutoff, summary, rows) = self
            .conn
            .call(move |c| {
                // Read cutoff + rows in one transaction — a concurrent
                // set_compaction (another process) between the two reads
                // would pair a stale summary with the wrong row window.
                let tx = c.transaction()?;
                let (cutoff, summary): (i64, Option<String>) = match tx.query_row(
                    "SELECT compacted_through, summary FROM sessions WHERE id = ?1",
                    rusqlite::params![sid],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ) {
                    Ok(v) => v,
                    // Missing session → no cutoff; other errors must surface.
                    Err(rusqlite::Error::QueryReturnedNoRows) => (0, None),
                    Err(e) => return Err(e.into()),
                };
                let mut stmt = tx.prepare(
                    "SELECT data FROM messages WHERE session_id = ?1 AND id > ?2 ORDER BY id",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![sid, cutoff], |row| {
                        row.get::<_, String>(0)
                    })?
                    .collect::<Result<Vec<String>, _>>()?;
                Ok::<(i64, Option<String>, Vec<String>), tokio_rusqlite::Error>((
                    cutoff, summary, rows,
                ))
            })
            .await?;
        let mut out: Vec<Value> = Vec::new();
        if let Some(s) = summary {
            out.push(serde_json::json!({
                "role": "user",
                "content": format!("[Earlier conversation summary]\n{s}"),
            }));
        }
        for s in rows {
            out.push(serde_json::from_str(&s)?);
        }
        Ok(out)
    }

    /// All messages with their row ids, ignoring compaction. Used by the
    /// compactor to summarize the dropped range.
    pub async fn messages_full(&self, session_id: &str) -> anyhow::Result<Vec<StoredMessage>> {
        let sid = session_id.to_string();
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, session_id, role, data FROM messages
                     WHERE session_id = ?1 ORDER BY id",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![sid], |row| {
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

    /// Record a compaction: messages up to `through_id` are replaced by
    /// `summary` on the next `messages()` read.
    pub async fn set_compaction(
        &self,
        session_id: &str,
        through_id: i64,
        summary: &str,
    ) -> anyhow::Result<()> {
        let sid = session_id.to_string();
        let summary = summary.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "UPDATE sessions SET compacted_through = ?2, summary = ?3
                     WHERE id = ?1",
                    rusqlite::params![sid, through_id, summary],
                )
                .map_err(tokio_rusqlite::Error::from)
            })
            .await?;
        Ok(())
    }

    /// Current compaction state: (compacted_through message id, summary).
    pub async fn compaction(&self, session_id: &str) -> anyhow::Result<(i64, Option<String>)> {
        let sid = session_id.to_string();
        self.conn
            .call(move |c| {
                let r = c.query_row(
                    "SELECT compacted_through, summary FROM sessions WHERE id = ?1",
                    rusqlite::params![sid],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                );
                match r {
                    Ok(v) => Ok::<(i64, Option<String>), tokio_rusqlite::Error>(v),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok((0, None)),
                    Err(e) => Err(e.into()),
                }
            })
            .await
            .map_err(Into::into)
    }

    /// All sessions as `(id, created_at, model, title)` — model/title are
    /// empty strings when unset.
    pub async fn list_sessions(&self) -> anyhow::Result<Vec<(String, String, String, String)>> {
        self.conn
            .call(|c| {
                let mut stmt = c.prepare(
                    "SELECT id, created_at, COALESCE(model, ''), COALESCE(title, '')
                     FROM sessions ORDER BY created_at",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<(String, String, String, String)>, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// Set the session title only when none exists — the first user
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

    /// Record one turn's token usage. Called once per completed prompt
    /// turn — a zero-usage turn (cancelled before first response) writes
    /// nothing so the table only holds real consumption.
    pub async fn record_usage(
        &self,
        session_id: &str,
        model: &str,
        input: u64,
        output: u64,
    ) -> anyhow::Result<()> {
        if input == 0 && output == 0 {
            return Ok(());
        }
        let sid = session_id.to_string();
        let model = model.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT INTO usage (session_id, model, input_tokens, output_tokens)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![sid, model, input as i64, output as i64],
                )?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }

    /// Per-model totals across all sessions: (model, input, output, turns).
    pub async fn usage_summary(&self) -> anyhow::Result<Vec<(String, u64, u64, u64)>> {
        self.conn
            .call(|c| {
                let mut stmt = c.prepare(
                    "SELECT model, SUM(input_tokens), SUM(output_tokens), COUNT(*)
                     FROM usage GROUP BY model ORDER BY SUM(input_tokens) DESC",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)? as u64,
                            row.get::<_, i64>(2)? as u64,
                            row.get::<_, i64>(3)? as u64,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<(String, u64, u64, u64)>, tokio_rusqlite::Error>(rows)
            })
            .await
            .map_err(Into::into)
    }

    /// Per-session totals: (input, output, turns).
    pub async fn session_usage(&self, session_id: &str) -> anyhow::Result<(u64, u64, u64)> {
        let sid = session_id.to_string();
        self.conn
            .call(move |c| {
                let (i, o, n): (i64, i64, i64) = c.query_row(
                    "SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0), COUNT(*)
                     FROM usage WHERE session_id = ?1",
                    rusqlite::params![sid],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )?;
                Ok::<(u64, u64, u64), tokio_rusqlite::Error>((i as u64, o as u64, n as u64))
            })
            .await
            .map_err(Into::into)
    }

    /// Update the session's stored default model (None clears it).
    pub async fn set_session_model(
        &self,
        session_id: &str,
        model: Option<&str>,
    ) -> anyhow::Result<bool> {
        let sid = session_id.to_string();
        let model = model.map(String::from);
        self.conn
            .call(move |c| {
                let n = c.execute(
                    "UPDATE sessions SET model = ?2 WHERE id = ?1",
                    rusqlite::params![sid, model],
                )?;
                Ok::<bool, tokio_rusqlite::Error>(n > 0)
            })
            .await
            .map_err(Into::into)
    }

    /// Whether a session row exists.
    pub async fn session_exists(&self, id: &str) -> anyhow::Result<bool> {
        let id = id.to_string();
        self.conn
            .call(move |c| {
                let n: i64 = c.query_row(
                    "SELECT COUNT(*) FROM sessions WHERE id = ?1",
                    rusqlite::params![id],
                    |row| row.get(0),
                )?;
                Ok::<bool, tokio_rusqlite::Error>(n > 0)
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

    /// Like `list_sessions`, but paginated — same 4-tuple shape.
    pub async fn list_sessions_paged(
        &self,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<(String, String, String, String)>> {
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, created_at, COALESCE(model, ''), COALESCE(title, '')
                     FROM sessions ORDER BY created_at LIMIT ?1 OFFSET ?2",
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

    /// Like `messages_full`, but paginated by row id.
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
                     WHERE session_id = ?1 ORDER BY id LIMIT ?2 OFFSET ?3",
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
}
