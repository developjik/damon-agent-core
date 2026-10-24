//! Bot personas: file-based identity for sessions (adapted from
//! upstream desktop-cc-gui's `bots/<id>/{bot.json, SOUL.md, AGENTS.md}`).
//!
//! A bot is a directory under `<data_dir>/bots/<id>/`:
//!
//! - `bot.json` — `{"name": …, "description"?: …}`; the id IS the
//!   directory name.
//! - `SOUL.md` — the personality; rides in front of every turn.
//! - `AGENTS.md` — behavioral instructions; follow the soul block.
//!
//! Files are the interface on purpose: users edit a persona with any
//! editor and the NEXT session picks it up. A running session keeps
//! the block it was created with — identity must not change
//! mid-conversation. Unlike the memory ledgers, persona files carry
//! no write gates: they are user-authored prompt text on local disk,
//! the same trust level as an `AGENTS.md` the agent itself already
//! reads — there is nothing here for a model to smuggle past a gate,
//! because only the user writes these files.

use anyhow::Context;
use std::path::{Path, PathBuf};

/// Bot ids are directory names AND RPC params — lowercase, path-safe,
/// no separators, no traversal, no leading `-` (flag confusion).
pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !id.starts_with('-')
}

/// The bots root: `<data_dir>/bots` (config `data_dir`, else the
/// default `~/.damon`).
pub fn bots_root(data_dir: Option<&Path>) -> PathBuf {
    let base = data_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(crate::config::default_data_dir);
    base.join("bots")
}

/// One bot persona as loaded from disk. `soul`/`agents` are None when
/// the file is missing or effectively empty.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BotDef {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub soul: Option<String>,
    pub agents: Option<String>,
}

impl BotDef {
    /// The turn-time persona block, or None for a metadata-only bot
    /// (nothing to inject — not even the wrapper). The soul block
    /// names the bot; the instructions block follows it. Unlike the
    /// memory block, this IS instructions by design — it is the
    /// user's own authored identity, not model-sourced data.
    pub fn persona_block(&self) -> Option<String> {
        let mut parts: Vec<String> = Vec::new();
        if let Some(soul) = &self.soul {
            // The name rides in an attribute — strip what could break
            // out of it; display names are free text.
            let safe = self.name.replace(['"', '\n', '\r'], " ");
            parts.push(format!("<persona bot=\"{safe}\">\n{soul}\n</persona>"));
        }
        if let Some(agents) = &self.agents {
            parts.push(format!(
                "<persona-instructions>\n{agents}\n</persona-instructions>"
            ));
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("\n\n"))
        }
    }
}

/// A resolved persona bound to a session at create time.
#[derive(Debug, Clone)]
pub struct Persona {
    /// The bot id (status/list surfaces).
    pub bot: String,
    /// The composed block; None for a metadata-only bot.
    pub block: Option<String>,
}

/// Load one bot. None when the id is invalid, the directory is
/// missing, or `bot.json` fails to parse — `bot.list` skips the same
/// way, so a half-written draft folder never bricks the RPC surface.
pub fn load(root: &Path, id: &str) -> Option<BotDef> {
    if !valid_id(id) {
        return None;
    }
    let dir = root.join(id);
    let meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("bot.json")).ok()?).ok()?;
    let name = meta["name"]
        .as_str()
        .filter(|n| !n.trim().is_empty())
        .map(String::from)?;
    Some(BotDef {
        id: id.to_string(),
        name,
        description: meta["description"].as_str().map(String::from),
        soul: read_markdown(&dir.join("SOUL.md")),
        agents: read_markdown(&dir.join("AGENTS.md")),
    })
}

/// Resolve a persona for session binding. Returns None for unknown
/// ids (and, by construction of [`load`], invalid ones).
pub fn resolve(root: &Path, id: &str) -> Option<Persona> {
    let def = load(root, id)?;
    Some(Persona {
        bot: def.id.clone(),
        block: def.persona_block(),
    })
}

/// All bots, sorted by id. Unreadable entries are skipped — see
/// [`load`].
pub fn list(root: &Path) -> Vec<BotDef> {
    let mut out: Vec<BotDef> = std::fs::read_dir(root)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter_map(|e| e.file_name().to_str().and_then(|id| load(root, id)))
        .collect();
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out
}

/// Read a persona markdown file; whitespace-only content counts as
/// absent (an emptied SOUL.md means "no soul", not an empty block).
fn read_markdown(path: &Path) -> Option<String> {
    let s = std::fs::read_to_string(path).ok()?;
    if s.trim().is_empty() { None } else { Some(s) }
}

/// Write semantics for optional string fields: absent keeps the
/// current value, null (or, for files, "") clears it, a string sets
/// it. The three states map straight onto the `bot.save` params.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Field {
    Keep,
    Clear,
    Set(String),
}

/// A `bot.save` request after validation.
#[derive(Debug, Clone)]
pub struct BotSave {
    pub id: String,
    pub name: String,
    pub description: Field,
    pub soul: Field,
    pub agents: Field,
}

/// Create or update a bot. `bot.json` is rewritten merged (untouched
/// fields keep their current values); `SOUL.md`/`AGENTS.md` are only
/// written when their field is Set, and removed when Set(""). Returns
/// the bot as it now exists on disk.
pub fn save(root: &Path, req: &BotSave) -> anyhow::Result<BotDef> {
    if !valid_id(&req.id) {
        anyhow::bail!(
            "invalid bot id `{}` — lowercase letters, digits, and `-`, up to 64 chars",
            req.id
        );
    }
    if req.name.trim().is_empty() {
        anyhow::bail!("name required");
    }
    let dir = root.join(&req.id);
    std::fs::create_dir_all(&dir)?;
    let current = load(root, &req.id);
    let name = req.name.trim().to_string();
    let description = match &req.description {
        Field::Keep => current.as_ref().and_then(|c| c.description.clone()),
        Field::Clear => None,
        Field::Set(d) => Some(d.trim().to_string()).filter(|d| !d.is_empty()),
    };
    write_file(
        &dir.join("SOUL.md"),
        matches!(req.soul, Field::Keep),
        match &req.soul {
            Field::Set(s) => Some(s.as_str()),
            _ => None,
        },
    )?;
    write_file(
        &dir.join("AGENTS.md"),
        matches!(req.agents, Field::Keep),
        match &req.agents {
            Field::Set(s) => Some(s.as_str()),
            _ => None,
        },
    )?;
    let meta = serde_json::json!({
        "name": name,
        "description": description,
    });
    std::fs::write(dir.join("bot.json"), serde_json::to_string_pretty(&meta)?)?;
    load(root, &req.id).ok_or_else(|| anyhow::anyhow!("bot {} did not round-trip", req.id))
}

/// Write/remove one markdown file. `keep` leaves the file alone;
/// `Some(text)` writes (empty text removes — an emptied file means
/// "no block", same as absent).
fn write_file(path: &Path, keep: bool, text: Option<&str>) -> anyhow::Result<()> {
    if keep {
        return Ok(());
    }
    match text.filter(|t| !t.trim().is_empty()) {
        Some(t) => std::fs::write(path, t)?,
        None => drop(std::fs::remove_file(path)), // absent is fine
    }
    Ok(())
}

/// Delete a bot. Returns false when it did not exist.
pub fn delete(root: &Path, id: &str) -> anyhow::Result<bool> {
    if !valid_id(id) {
        anyhow::bail!("invalid bot id `{id}`");
    }
    let dir = root.join(id);
    if !dir.exists() {
        return Ok(false);
    }
    std::fs::remove_dir_all(&dir).with_context(|| format!("cannot remove {}", dir.display()))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_root(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "damon-bots-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn id_rules() {
        assert!(valid_id("telegram-friend"));
        assert!(valid_id("b0t"));
        assert!(!valid_id(""));
        assert!(!valid_id("UPPER"));
        assert!(!valid_id("a/b"));
        assert!(!valid_id(".."));
        assert!(!valid_id("-flag"));
        assert!(!valid_id(&"x".repeat(65)));
    }

    #[test]
    fn save_load_delete_roundtrip() {
        let root = tmp_root("roundtrip");
        let saved = save(
            &root,
            &BotSave {
                id: "friend".into(),
                name: "친구".into(),
                description: Field::Set("반말 친구".into()),
                soul: Field::Set("항상 편하게 반말한다.".into()),
                agents: Field::Set("답은 짧게.".into()),
            },
        )
        .unwrap();
        assert_eq!(saved.name, "친구");
        assert_eq!(saved.soul.as_deref(), Some("항상 편하게 반말한다."));

        let loaded = load(&root, "friend").unwrap();
        assert_eq!(loaded, saved);

        assert!(delete(&root, "friend").unwrap());
        assert!(!delete(&root, "friend").unwrap());
        assert!(load(&root, "friend").is_none());
    }

    #[test]
    fn keep_clear_set_semantics() {
        let root = tmp_root("fields");
        save(
            &root,
            &BotSave {
                id: "b".into(),
                name: "B".into(),
                description: Field::Set("d".into()),
                soul: Field::Set("soul v1".into()),
                agents: Field::Set("agents v1".into()),
            },
        )
        .unwrap();
        // Keep everything except the name.
        let after = save(
            &root,
            &BotSave {
                id: "b".into(),
                name: "B2".into(),
                description: Field::Keep,
                soul: Field::Keep,
                agents: Field::Clear,
            },
        )
        .unwrap();
        assert_eq!(after.name, "B2");
        assert_eq!(after.description.as_deref(), Some("d"));
        assert_eq!(after.soul.as_deref(), Some("soul v1"));
        assert_eq!(after.agents, None);
        // Clearing again is a no-op; setting "" also clears.
        let after = save(
            &root,
            &BotSave {
                id: "b".into(),
                name: "B".into(),
                description: Field::Clear,
                soul: Field::Set("".into()),
                agents: Field::Keep,
            },
        )
        .unwrap();
        assert_eq!(after.description, None);
        assert_eq!(after.soul, None);
    }

    #[test]
    fn list_skips_broken_entries_and_sorts() {
        let root = tmp_root("list");
        for id in ["zeta", "alpha"] {
            save(
                &root,
                &BotSave {
                    id: id.into(),
                    name: id.into(),
                    description: Field::Keep,
                    soul: Field::Keep,
                    agents: Field::Keep,
                },
            )
            .unwrap();
        }
        // A draft folder without bot.json, and one with broken JSON.
        std::fs::create_dir_all(root.join("draft")).unwrap();
        std::fs::create_dir_all(root.join("broken")).unwrap();
        std::fs::write(root.join("broken/bot.json"), "{nope").unwrap();
        let ids: Vec<String> = list(&root).into_iter().map(|b| b.id).collect();
        assert_eq!(ids, vec!["alpha", "zeta"]);
    }

    #[test]
    fn persona_block_shapes() {
        let root = tmp_root("block");
        let full = save(
            &root,
            &BotSave {
                id: "b".into(),
                name: "Team \"A\"\nline".into(),
                description: Field::Keep,
                soul: Field::Set("soul text".into()),
                agents: Field::Set("agents text".into()),
            },
        )
        .unwrap();
        let block = full.persona_block().unwrap();
        assert!(block.starts_with("<persona bot=\"Team  A"));
        assert!(block.contains("soul text"));
        assert!(block.contains("<persona-instructions>\nagents text"));
        // Metadata-only bot: nothing to inject.
        let bare = save(
            &root,
            &BotSave {
                id: "bare".into(),
                name: "Bare".into(),
                description: Field::Keep,
                soul: Field::Keep,
                agents: Field::Keep,
            },
        )
        .unwrap();
        assert_eq!(bare.persona_block(), None);
        // Whitespace-only soul counts as absent.
        let blank = save(
            &root,
            &BotSave {
                id: "blank".into(),
                name: "Blank".into(),
                description: Field::Keep,
                soul: Field::Set("   \n\t".into()),
                agents: Field::Keep,
            },
        )
        .unwrap();
        assert_eq!(blank.persona_block(), None);
    }

    #[test]
    fn resolve_binds_block_and_id() {
        let root = tmp_root("resolve");
        save(
            &root,
            &BotSave {
                id: "bot-1".into(),
                name: "One".into(),
                description: Field::Keep,
                soul: Field::Set("s".into()),
                agents: Field::Keep,
            },
        )
        .unwrap();
        let p = resolve(&root, "bot-1").unwrap();
        assert_eq!(p.bot, "bot-1");
        assert!(p.block.unwrap().contains("s"));
        assert!(resolve(&root, "nope").is_none());
        // Invalid ids can never resolve — no traversal through bots/.
        assert!(resolve(&root, "../secrets").is_none());
    }
}
