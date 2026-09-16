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
                 CREATE TABLE IF NOT EXISTS sessions (
                     id TEXT PRIMARY KEY,
                     created_at TEXT NOT NULL DEFAULT (datetime('now')),
                     cwd TEXT NOT NULL DEFAULT '',
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
            // Migrate pre-compaction databases.
            let _ = c.execute_batch(
                "ALTER TABLE sessions ADD COLUMN compacted_through INTEGER NOT NULL DEFAULT 0;
                 ALTER TABLE sessions ADD COLUMN summary TEXT;",
            );
            // Backfill the FTS index for messages written before it existed.
            c.execute_batch(
                "INSERT INTO messages_fts (content, session_id, message_id)
                 SELECT json_extract(m.data, '$.content'), m.session_id, m.id
                 FROM messages m
                 WHERE json_type(m.data, '$.content') = 'text'
                   AND m.id NOT IN (SELECT message_id FROM messages_fts);",
            )?;
            Ok::<(), rusqlite::Error>(())
            .map_err(tokio_rusqlite::Error::from)
        })
        .await?;
        Ok(Self { conn })
    }

    /// In-memory store for tests.
    pub async fn in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory().await?;
        conn.call(|c| {
            c.execute_batch(
                "CREATE TABLE sessions (
                     id TEXT PRIMARY KEY,
                     created_at TEXT NOT NULL DEFAULT (datetime('now')),
                     cwd TEXT NOT NULL DEFAULT '',
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

    pub async fn create_session(&self, id: &str, cwd: &str) -> anyhow::Result<()> {
        let id = id.to_string();
        let cwd = cwd.to_string();
        self.conn
            .call(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO sessions (id, cwd) VALUES (?1, ?2)",
                    rusqlite::params![id, cwd],
                )
                .map_err(tokio_rusqlite::Error::from)
            })
            .await?;
        Ok(())
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
                if let Ok(v) = serde_json::from_str::<Value>(&data) {
                    if let Some(text) = v["content"].as_str() {
                        tx.execute(
                            "INSERT INTO messages_fts (content, session_id, message_id)
                             VALUES (?1, ?2, ?3)",
                            rusqlite::params![text, sid, id],
                        )?;
                    }
                }
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
        let query = query.to_string();
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
                let (cutoff, summary): (i64, Option<String>) = c
                    .query_row(
                        "SELECT compacted_through, summary FROM sessions WHERE id = ?1",
                        rusqlite::params![sid],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .unwrap_or((0, None));
                let mut stmt = c.prepare(
                    "SELECT data FROM messages WHERE session_id = ?1 AND id > ?2 ORDER BY id",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![sid, cutoff], |row| {
                        row.get::<_, String>(0)
                    })?
                    .collect::<Result<Vec<String>, _>>()?;
                Ok::<(i64, Option<String>, Vec<String>), tokio_rusqlite::Error>(
                    (cutoff, summary, rows),
                )
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
    pub async fn messages_full(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<StoredMessage>> {
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

    pub async fn list_sessions(&self) -> anyhow::Result<Vec<(String, String)>> {
        self.conn
            .call(|c| {
                let mut stmt =
                    c.prepare("SELECT id, created_at FROM sessions ORDER BY created_at")?;
                let rows = stmt
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<Vec<(String, String)>, tokio_rusqlite::Error>(rows)
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
                c.execute(
                    "DELETE FROM messages WHERE session_id = ?1",
                    rusqlite::params![id],
                )?;
                c.execute("DELETE FROM sessions WHERE id = ?1", rusqlite::params![id])?;
                Ok::<(), tokio_rusqlite::Error>(())
            })
            .await
            .map_err(Into::into)
    }
}
