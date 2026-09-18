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

impl Store {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .await
            .with_context(|| format!("cannot open store {}", path.display()))?;
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
                     summary TEXT
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
                 );",
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
                     summary TEXT
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
                 );",
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
        // FTS5 query syntax ("", *, NEAR, AND) would 500 on innocent
        // input — wrap the whole query as one quoted phrase instead.
        let query = format!("\"{}\"", query.replace('"', "\"\""));
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

    /// All sessions as `(id, created_at, model)` — model is the stored
    /// session default override, empty string when unset.
    pub async fn list_sessions(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        self.conn
            .call(|c| {
                let mut stmt = c.prepare(
                    "SELECT id, created_at, COALESCE(model, '') FROM sessions ORDER BY created_at",
                )?;
                let rows = stmt
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<(String, String, String)>, tokio_rusqlite::Error>(rows)
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

    /// Like `list_sessions`, but paginated.
    pub async fn list_sessions_paged(
        &self,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<(String, String, String)>> {
        self.conn
            .call(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, created_at, COALESCE(model, '') FROM sessions
                     ORDER BY created_at LIMIT ?1 OFFSET ?2",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![limit, offset], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<(String, String, String)>, tokio_rusqlite::Error>(rows)
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
