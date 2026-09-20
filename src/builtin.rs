//! Builtin tools — `fs.read` / `fs.write` / `fs.edit` / `fs.list` /
//! `fs.search` / `shell.exec` — so a fresh install is useful without any
//! MCP servers configured.
//!
//! # Threat model
//!
//! These tools run with the daemon's own privileges, exactly like MCP
//! servers spawned from the config. The PRIMARY gate is the permission
//! prompt: every call round-trips through the client unless
//! `auto_approve` is set. `allowed_paths` is a SECONDARY sandbox for the
//! `fs.*` tools only — each target is canonicalized and must live under
//! an allowed root, so a misbehaving model (or a permissive
//! `auto_approve` config) can't wander the filesystem. It is NOT a
//! boundary against `shell.exec`: a shell escapes a path list trivially,
//! so set `shell = false` when confinement actually matters.

use std::path::{Component, Path, PathBuf};

use anyhow::Context;
use serde_json::{Value, json};
use tracing::warn;

use crate::config::{BuiltinConfig, glob_match};

/// Byte cap on fs.read content and each shell.exec output stream.
/// Head+tail are kept so both early errors and final output survive.
const OUTPUT_CAP: usize = 64 * 1024;
/// fs.list entry cap.
const LIST_CAP: usize = 1000;
/// Entries a glob-pattern fs.list walk visits before giving up — far
/// above LIST_CAP so sparse matches deep in a tree are still found.
const LIST_WALK_CAP: usize = 50_000;
/// fs.search match cap.
const SEARCH_CAP: usize = 200;
/// Files larger than this are skipped by fs.search — they're usually
/// generated or minified and drown real matches.
const SEARCH_FILE_CAP: u64 = 4 * 1024 * 1024;
/// Bound on files fs.search opens per call.
const SEARCH_FILES_CAP: usize = 10_000;
/// Lines longer than this are truncated before regex matching — the
/// backtracking engine's recursion depth is bounded and giant minified
/// lines would exceed it.
const SEARCH_LINE_CAP: usize = 16 * 1024;
/// Per-match text cap in fs.search results.
const SEARCH_TEXT_CAP: usize = 256;
/// Default shell.exec timeout.
const DEFAULT_SHELL_TIMEOUT: u64 = 120;
/// Hard ceiling on shell.exec timeout — a model-supplied timeout must
/// not be able to stall a turn for hours.
const MAX_SHELL_TIMEOUT: u64 = 3600;

/// Filesystem tool names — always present when the toolset is enabled.
const FS_TOOLS: [&str; 5] = ["fs.read", "fs.write", "fs.edit", "fs.list", "fs.search"];
const SHELL_TOOL: &str = "shell.exec";

/// The daemon's builtin toolset, constructed from `[builtin_tools]`.
pub struct BuiltinTools {
    enabled: bool,
    auto_approve: bool,
    shell: bool,
    /// Raw configured roots; canonicalized per call in `resolve_path`
    /// (a root may not exist at startup but be created later).
    allowed_roots: Vec<PathBuf>,
    shell_timeout: u64,
    /// "Always allow" grants: (session_id, tool). Lives on the tools
    /// object like McpRegistry's grants — a config reload rebuilds the
    /// object and clears them, matching MCP behavior.
    session_grants: parking_lot::Mutex<std::collections::HashSet<(String, String)>>,
}

impl BuiltinTools {
    pub fn from_config(cfg: &BuiltinConfig) -> Self {
        Self {
            enabled: cfg.enabled.unwrap_or(true),
            auto_approve: cfg.auto_approve.unwrap_or(false),
            shell: cfg.shell.unwrap_or(true),
            allowed_roots: cfg.allowed_paths.iter().map(PathBuf::from).collect(),
            shell_timeout: cfg.shell_timeout_secs.unwrap_or(DEFAULT_SHELL_TIMEOUT),
            session_grants: parking_lot::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// OpenAI `tools` array for chat completions, merged into the turn's
    /// tool list alongside MCP tools.
    pub fn openai_tools(&self) -> Vec<Value> {
        if !self.enabled {
            return Vec::new();
        }
        let mut tools = vec![
            decl(
                "fs.read",
                "Read a file as text (UTF-8, lossy). Returns a window of \
                 lines plus a `truncated` flag; output is capped at 64KiB.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "File path — absolute, or relative to the session working directory"},
                        "offset": {"type": "integer", "description": "1-based line number to start at (default 1)"},
                        "limit": {"type": "integer", "description": "Maximum number of lines to return (default: all, up to the output cap)"},
                    },
                    "required": ["path"],
                }),
            ),
            decl(
                "fs.write",
                "Write a file, creating parent directories as needed. \
                 Overwrites existing content.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "File path — absolute, or relative to the session working directory"},
                        "content": {"type": "string", "description": "Full file content to write"},
                    },
                    "required": ["path", "content"],
                }),
            ),
            decl(
                "fs.edit",
                "Replace an exact string in a file. `old_string` must occur \
                 exactly once — the call fails on zero or multiple matches, \
                 so include enough context to make it unique.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "File path — absolute, or relative to the session working directory"},
                        "old_string": {"type": "string", "description": "Exact text to replace; must match exactly once"},
                        "new_string": {"type": "string", "description": "Replacement text"},
                    },
                    "required": ["path", "old_string", "new_string"],
                }),
            ),
            decl(
                "fs.list",
                "List a directory's immediate children (directories are \
                 suffixed with '/'). With `pattern`, walks recursively and \
                 returns entries whose path relative to `path` matches the \
                 glob (`*` any infix, `?` one char).",
                json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Directory path — absolute, or relative to the session working directory"},
                        "pattern": {"type": "string", "description": "Optional glob over relative paths, e.g. \"*.rs\"; makes the listing recursive"},
                    },
                    "required": ["path"],
                }),
            ),
            decl(
                "fs.search",
                "Search file contents recursively under `path` with a regex. \
                 Returns matching rows as {file, line, text}. Binary and \
                 very large files are skipped.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "File or directory to search — absolute, or relative to the session working directory"},
                        "pattern": {"type": "string", "description": "Regex pattern matched against each line"},
                    },
                    "required": ["path", "pattern"],
                }),
            ),
        ];
        if self.shell {
            tools.push(decl(
                "shell.exec",
                "Run a shell command (sh -c / cmd /C) in the session working \
                 directory. Returns stdout, stderr, and the exit code; each \
                 stream is capped at 64KiB (head+tail kept).",
                json!({
                    "type": "object",
                    "properties": {
                        "command": {"type": "string", "description": "Command line passed to the shell"},
                        "timeout_secs": {"type": "integer", "description": "Kill the command after this many seconds (default from config, max 3600)"},
                    },
                    "required": ["command"],
                }),
            ));
        }
        tools
    }

    pub fn has_tool(&self, name: &str) -> bool {
        self.enabled && (FS_TOOLS.contains(&name) || (self.shell && name == SHELL_TOOL))
    }
    /// "Always allow" grant check — mirrors McpRegistry::session_approved.
    pub fn session_approved(&self, session_id: &str, name: &str) -> bool {
        self.session_grants
            .lock()
            .contains(&(session_id.to_string(), name.to_string()))
    }

    /// Record an "always allow" grant for this session + tool.
    pub fn approve_for_session(&self, session_id: &str, name: &str) {
        self.session_grants
            .lock()
            .insert((session_id.to_string(), name.to_string()));
    }

    /// Whether calls to this tool skip the permission prompt. Applies to
    /// ALL builtin tools uniformly — there is no per-tool granularity.
    pub fn auto_approve(&self, name: &str) -> bool {
        self.auto_approve && self.has_tool(name)
    }

    /// Execute a builtin tool. `cwd` is the session's working directory
    /// and may be empty (relative paths then resolve against the process
    /// cwd). The return value is serialized into the tool message, so it
    /// is always a structured object, never pre-stringified text.
    pub async fn call(&self, name: &str, args: Value, cwd: &str) -> anyhow::Result<Value> {
        if !self.has_tool(name) {
            anyhow::bail!("unknown tool {name}");
        }
        match name {
            "fs.read" => self.fs_read(&args, cwd).await,
            "fs.write" => self.fs_write(&args, cwd).await,
            "fs.edit" => self.fs_edit(&args, cwd).await,
            "fs.list" => self.fs_list(&args, cwd).await,
            "fs.search" => self.fs_search(&args, cwd).await,
            SHELL_TOOL => self.shell_exec(&args, cwd).await,
            _ => unreachable!("has_tool gate passed for {name}"),
        }
    }

    /// Resolve `path` against `cwd` (or the process cwd when `cwd` is
    /// empty) and enforce the `allowed_paths` sandbox when configured.
    /// Returns the path to actually operate on — NOT canonicalized in the
    /// unrestricted case, so error messages show what the model asked for.
    fn resolve_path(&self, path: &str, cwd: &str) -> anyhow::Result<PathBuf> {
        let raw = Path::new(path);
        let joined = if raw.is_absolute() {
            raw.to_path_buf()
        } else {
            let base = if cwd.is_empty() {
                std::env::current_dir().context("no session cwd and process cwd unavailable")?
            } else {
                PathBuf::from(cwd)
            };
            base.join(raw)
        };
        if self.allowed_roots.is_empty() {
            return Ok(joined);
        }
        let resolved = canonicalize_lenient(&joined);
        let allowed = self
            .allowed_roots
            .iter()
            .any(|root| resolved.starts_with(canonicalize_lenient(root)));
        if !allowed {
            anyhow::bail!("path {path:?} is outside the configured allowed_paths");
        }
        Ok(resolved)
    }

    /// fs.read — line-windowed read. `offset` is a 1-based line number,
    /// `limit` a max line count; output stops at OUTPUT_CAP either way.
    async fn fs_read(&self, args: &Value, cwd: &str) -> anyhow::Result<Value> {
        let path = arg_str(args, "path")?;
        let offset = args["offset"].as_u64().unwrap_or(1).max(1) as usize;
        let limit = args["limit"].as_u64().map(|n| n as usize);
        let resolved = self.resolve_path(path, cwd)?;
        let bytes = tokio::fs::read(&resolved)
            .await
            .with_context(|| format!("fs.read: cannot read {}", resolved.display()))?;
        // Lossy: binary or invalid UTF-8 still yields a view instead of
        // an error — a read tool should never refuse to show content.
        let text = String::from_utf8_lossy(&bytes);
        let mut out = String::new();
        let mut truncated = false;
        let mut written = 0usize;
        for (idx, line) in text.lines().enumerate() {
            if idx + 1 < offset {
                continue;
            }
            if let Some(l) = limit
                && written >= l
            {
                truncated = true;
                break;
            }
            let budget = OUTPUT_CAP.saturating_sub(out.len());
            if line.len() + 1 > budget {
                // A single line can exceed the cap (minified files) —
                // emit a truncated prefix rather than nothing.
                out.push_str(&line[..boundary_down(line, budget)]);
                truncated = true;
                break;
            }
            out.push_str(line);
            out.push('\n');
            written += 1;
        }
        Ok(json!({ "content": out, "truncated": truncated }))
    }

    async fn fs_write(&self, args: &Value, cwd: &str) -> anyhow::Result<Value> {
        let path = arg_str(args, "path")?;
        let content = arg_str(args, "content")?;
        let resolved = self.resolve_path(path, cwd)?;
        if let Some(parent) = resolved.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("fs.write: cannot create {}", parent.display()))?;
        }
        tokio::fs::write(&resolved, content)
            .await
            .with_context(|| format!("fs.write: cannot write {}", resolved.display()))?;
        Ok(json!({ "bytes": content.len() }))
    }

    /// fs.edit — exact-match replace, unique-match enforced. Uniqueness
    /// is the safety story: a model that under-specifies context gets an
    /// error instead of a silent wrong-site edit.
    async fn fs_edit(&self, args: &Value, cwd: &str) -> anyhow::Result<Value> {
        let path = arg_str(args, "path")?;
        let old = arg_str(args, "old_string")?;
        let new = arg_str(args, "new_string")?;
        if old.is_empty() {
            anyhow::bail!("fs.edit: old_string must not be empty");
        }
        let resolved = self.resolve_path(path, cwd)?;
        // read_to_string (not lossy): rewriting a non-UTF-8 file through
        // a lossy decode would corrupt it.
        let text = tokio::fs::read_to_string(&resolved)
            .await
            .with_context(|| format!("fs.edit: cannot read {}", resolved.display()))?;
        let count = text.matches(old).count();
        match count {
            0 => anyhow::bail!("fs.edit: old_string not found in {path:?}"),
            1 => {}
            n => anyhow::bail!(
                "fs.edit: old_string matches {n} times in {path:?}; it must be unique"
            ),
        }
        let updated = text.replacen(old, new, 1);
        tokio::fs::write(&resolved, &updated)
            .await
            .with_context(|| format!("fs.edit: cannot write {}", resolved.display()))?;
        Ok(json!({ "replacements": 1, "bytes": updated.len() }))
    }

    /// fs.list — immediate children without a pattern; recursive glob
    /// walk with one. Directories are reported with a trailing '/'.
    async fn fs_list(&self, args: &Value, cwd: &str) -> anyhow::Result<Value> {
        let path = arg_str(args, "path")?;
        let pattern = args["pattern"].as_str();
        let resolved = self.resolve_path(path, cwd)?;
        let mut entries = Vec::new();
        let mut truncated = false;
        match pattern {
            None => {
                let mut rd = tokio::fs::read_dir(&resolved)
                    .await
                    .with_context(|| format!("fs.list: cannot list {}", resolved.display()))?;
                while let Some(entry) = rd.next_entry().await? {
                    let mut name = entry.file_name().to_string_lossy().into_owned();
                    if entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
                        name.push('/');
                    }
                    entries.push(name);
                    if entries.len() >= LIST_CAP {
                        truncated = true;
                        break;
                    }
                }
            }
            Some(pat) => {
                let (walked, walk_truncated) = walk(&resolved, LIST_WALK_CAP).await?;
                truncated = walk_truncated;
                for (p, is_dir) in walked {
                    let rel = p.strip_prefix(&resolved).unwrap_or(&p).to_string_lossy();
                    if glob_match(pat, &rel) {
                        let mut s = rel.into_owned();
                        if is_dir {
                            s.push('/');
                        }
                        entries.push(s);
                        if entries.len() >= LIST_CAP {
                            truncated = true;
                            break;
                        }
                    }
                }
            }
        }
        entries.sort();
        Ok(json!({ "entries": entries, "truncated": truncated }))
    }

    /// fs.search — regex over file contents, recursive under `path`.
    /// Returns {file, line, text} rows; binary and oversized files are
    /// skipped rather than erroring the whole search.
    async fn fs_search(&self, args: &Value, cwd: &str) -> anyhow::Result<Value> {
        let path = arg_str(args, "path")?;
        let pattern = arg_str(args, "pattern")?;
        let re = regex::Regex::new(pattern)
            .map_err(|e| anyhow::anyhow!("fs.search: invalid pattern {pattern:?}: {e}"))?;
        let resolved = self.resolve_path(path, cwd)?;
        let files = if tokio::fs::metadata(&resolved)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            vec![resolved.clone()]
        } else {
            walk(&resolved, SEARCH_FILES_CAP)
                .await?
                .0
                .into_iter()
                .filter(|(_, is_dir)| !is_dir)
                .map(|(p, _)| p)
                .collect()
        };
        let mut matches = Vec::new();
        let mut truncated = false;
        'files: for file in files {
            let Ok(meta) = tokio::fs::metadata(&file).await else {
                continue;
            };
            if meta.len() > SEARCH_FILE_CAP {
                continue;
            }
            let Ok(bytes) = tokio::fs::read(&file).await else {
                continue;
            };
            // NUL in the first block → binary; skip.
            if bytes[..bytes.len().min(8192)].contains(&0) {
                continue;
            }
            let text = String::from_utf8_lossy(&bytes);
            let rel = file
                .strip_prefix(&resolved)
                .unwrap_or(&file)
                .to_string_lossy()
                .into_owned();
            for (idx, line) in text.lines().enumerate() {
                let probe = &line[..boundary_down(line, line.len().min(SEARCH_LINE_CAP))];
                if re.is_match(probe) {
                    matches.push(json!({
                        "file": rel,
                        "line": idx + 1,
                        "text": &probe[..boundary_down(probe, probe.len().min(SEARCH_TEXT_CAP))],
                    }));
                    if matches.len() >= SEARCH_CAP {
                        truncated = true;
                        break 'files;
                    }
                }
            }
        }
        Ok(json!({ "matches": matches, "truncated": truncated }))
    }

    /// shell.exec — `sh -c` (unix) / `cmd /C` (windows), stdout+stderr
    /// captured and capped, timeout enforced with kill_on_drop.
    async fn shell_exec(&self, args: &Value, cwd: &str) -> anyhow::Result<Value> {
        let command = arg_str(args, "command")?;
        let timeout_secs = args["timeout_secs"]
            .as_u64()
            .unwrap_or(self.shell_timeout)
            .min(MAX_SHELL_TIMEOUT);
        let mut cmd = if cfg!(windows) {
            let mut c = tokio::process::Command::new("cmd");
            c.args(["/C", command]);
            c
        } else {
            let mut c = tokio::process::Command::new("sh");
            c.args(["-c", command]);
            c
        };
        if !cwd.is_empty() {
            cmd.current_dir(cwd);
        }
        // kill_on_drop: when the timeout fires, dropping the output()
        // future kills the child instead of orphaning it. Caveat: the
        // kill is SIGKILL on the direct child only — a `sh -c` that
        // spawned its own children can leave grandchildren behind.
        cmd.kill_on_drop(true);
        let out =
            match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), cmd.output())
                .await
            {
                Ok(Ok(out)) => out,
                Ok(Err(e)) => {
                    return Err(e).context(format!("shell.exec: failed to spawn {command:?}"));
                }
                Err(_) => {
                    return Ok(json!({
                        "stdout": "",
                        "stderr": format!("command timed out after {timeout_secs}s and was killed"),
                        "exit_code": Value::Null,
                        "timed_out": true,
                    }));
                }
            };
        Ok(json!({
            "stdout": cap_output(&out.stdout),
            "stderr": cap_output(&out.stderr),
            "exit_code": out.status.code(),
            "timed_out": false,
        }))
    }
}

/// One OpenAI function-tool declaration.
fn decl(name: &str, description: &str, parameters: Value) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters,
        }
    })
}

fn arg_str<'a>(args: &'a Value, key: &str) -> anyhow::Result<&'a str> {
    args[key]
        .as_str()
        .with_context(|| format!("missing or invalid '{key}' argument"))
}

/// Recursive directory walk. Returns (path, is_dir) pairs plus a
/// truncation flag, capped at `cap` entries. Unreadable subdirectories
/// are skipped with a warning — one bad dir shouldn't kill a listing;
/// an unreadable ROOT is an error.
async fn walk(root: &Path, cap: usize) -> anyhow::Result<(Vec<(PathBuf, bool)>, bool)> {
    let mut out = Vec::new();
    let mut truncated = false;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(e) => {
                if dir == root {
                    return Err(e).with_context(|| format!("cannot list {}", root.display()));
                }
                warn!(path = %dir.display(), error = %e, "skipping unreadable directory");
                continue;
            }
        };
        // Sort each level so traversal order (and therefore which entries
        // survive the cap) is deterministic.
        let mut children = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let is_dir = entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false);
            children.push((entry.path(), is_dir));
        }
        children.sort();
        for (p, is_dir) in children {
            if out.len() >= cap {
                truncated = true;
                return Ok((out, truncated));
            }
            if is_dir {
                stack.push(p.clone());
            }
            out.push((p, is_dir));
        }
    }
    Ok((out, truncated))
}

/// Lexically resolve `.` and `..` without touching the filesystem.
/// `..` at an absolute root stays put (can't escape above `/`); for a
/// relative path a leading `..` is preserved.
fn normalize_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() && !out.has_root() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Canonicalize as much of `path` as exists, then append the normalized
/// remainder. fs.write/fs.edit targets may not exist yet — plain
/// `canonicalize()` would fail and wrongly reject in-sandbox writes.
/// Normalizing first collapses `..` so `allowed/sub/../../etc` can't
/// escape through the non-existent tail.
fn canonicalize_lenient(path: &Path) -> PathBuf {
    let normalized = normalize_path(path);
    let mut ancestor = normalized.clone();
    let mut tail = Vec::new();
    while !ancestor.exists() {
        match ancestor.file_name() {
            Some(name) => {
                tail.push(name.to_os_string());
                ancestor.pop();
            }
            // Nothing exists all the way up — return as-is.
            None => return normalized,
        }
    }
    let mut out = ancestor.canonicalize().unwrap_or(ancestor);
    for comp in tail.iter().rev() {
        out.push(comp);
    }
    out
}

/// Largest index <= i that lies on a char boundary.
fn boundary_down(s: &str, mut i: usize) -> usize {
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// Smallest index >= i that lies on a char boundary.
fn boundary_up(s: &str, mut i: usize) -> usize {
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
}

/// Lossy-decode and cap at OUTPUT_CAP keeping head+tail — the beginning
/// usually carries the error that matters, the end the final result.
fn cap_output(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= OUTPUT_CAP {
        return text.into_owned();
    }
    let half = OUTPUT_CAP / 2;
    let head_end = boundary_down(&text, half);
    let tail_start = boundary_up(&text, text.len() - half);
    format!(
        "{}\n[… {} bytes elided …]\n{}",
        &text[..head_end],
        tail_start - head_end,
        &text[tail_start..],
    )
}

/// Minimal backtracking regex for fs.search.
///
/// The crate deliberately has no `regex` dependency and adding one for a
/// single tool isn't justified, so this engine covers the common subset:
/// literals, `.`, `* + ? {m} {m,} {m,n}` (greedy and lazy `?` suffix),
/// `[...]` classes with ranges/negation, `\d \w \s` and their negations,
/// escapes, `^`/`$` anchors, `(...)` groups (including `(?:...)`), and
/// `|` alternation. Unsupported constructs — backreferences, look-around,
/// `\b`, named classes — are parse ERRORS, never silently misinterpreted.
///
/// Matching is recursive backtracking over chars; two bounds keep a
/// pathological pattern from hurting the daemon: DEPTH_CAP limits
/// recursion (a repetition longer than that simply fails to match) and
/// STEP_BUDGET caps total work per line.
mod regex {
    /// Max pattern length — also bounds parse recursion on nested groups.
    const PATTERN_CAP: usize = 1024;
    /// Max matcher recursion depth (one frame per repetition iteration).
    const DEPTH_CAP: usize = 4096;
    /// Total match steps per line before the match is abandoned.
    const STEP_BUDGET: usize = 1_000_000;

    pub struct Regex {
        ast: Ast,
    }

    /// Alternation of sequences.
    struct Ast {
        alts: Vec<Vec<Item>>,
    }

    struct Item {
        atom: Atom,
        min: u32,
        max: u32,
        greedy: bool,
    }

    enum Atom {
        Char(char),
        /// `.` — any char except newline (lines are matched individually
        /// anyway, but keep standard semantics).
        Any,
        Class {
            negated: bool,
            ranges: Vec<(char, char)>,
        },
        Start,
        End,
        Group(Ast),
    }

    /// Mutable matcher state shared across the recursion.
    struct State {
        budget: usize,
        depth: usize,
    }

    /// Continuation: given the matcher state and a position, does the REST
    /// of the pattern match? `State` is passed in (not captured) so the
    /// continuation can coexist with the caller's own `&mut State` borrow.
    /// Backtracking = trying a shorter/longer match and re-invoking it.
    type Cont<'a> = dyn FnMut(&mut State, usize) -> bool + 'a;

    impl Regex {
        pub fn new(pattern: &str) -> Result<Self, String> {
            let chars: Vec<char> = pattern.chars().collect();
            if chars.len() > PATTERN_CAP {
                return Err(format!("pattern too long (max {PATTERN_CAP} chars)"));
            }
            let mut p = Parser { c: &chars, i: 0 };
            let ast = p.parse_alt()?;
            if p.i != chars.len() {
                return Err(format!("unexpected '{}'", chars[p.i]));
            }
            Ok(Self { ast })
        }

        /// Unanchored search: try every start position. The step budget
        /// is shared across positions so total work per line is bounded.
        pub fn is_match(&self, text: &str) -> bool {
            let s: Vec<char> = text.chars().collect();
            let mut st = State {
                budget: STEP_BUDGET,
                depth: 0,
            };
            (0..=s.len()).any(|start| {
                match_ast(
                    &self.ast,
                    &s,
                    start,
                    &mut st,
                    &mut |_: &mut State, _: usize| true,
                )
            })
        }
    }

    fn match_ast(ast: &Ast, s: &[char], pos: usize, st: &mut State, cont: &mut Cont) -> bool {
        ast.alts.iter().any(|seq| match_seq(seq, s, pos, st, cont))
    }

    fn match_seq(seq: &[Item], s: &[char], pos: usize, st: &mut State, cont: &mut Cont) -> bool {
        match seq.split_first() {
            None => cont(st, pos),
            Some((item, rest)) => {
                match_rep(item, 0, s, pos, st, &mut |st2: &mut State, p: usize| {
                    match_seq(rest, s, p, st2, cont)
                })
            }
        }
    }

    /// Match `item` repeated [min, max] times, then the continuation.
    /// Greedy tries the longest run first; lazy the shortest. Both fall
    /// back through every intermediate count on continuation failure.
    fn match_rep(
        item: &Item,
        count: u32,
        s: &[char],
        pos: usize,
        st: &mut State,
        cont: &mut Cont,
    ) -> bool {
        if st.budget == 0 || st.depth >= DEPTH_CAP {
            return false;
        }
        st.budget -= 1;
        if !item.greedy && count >= item.min && cont(st, pos) {
            return true;
        }
        if count < item.max {
            st.depth += 1;
            let more = match_atom(&item.atom, s, pos, st, &mut |st2: &mut State, p2: usize| {
                if p2 == pos {
                    // Zero-width iteration: count it toward min but don't
                    // recurse — `(a*)*` on an empty match would spin forever.
                    count + 1 >= item.min && cont(st2, p2)
                } else {
                    match_rep(item, count + 1, s, p2, st2, cont)
                }
            });
            st.depth -= 1;
            if more {
                return true;
            }
        }
        item.greedy && count >= item.min && cont(st, pos)
    }

    fn match_atom(atom: &Atom, s: &[char], pos: usize, st: &mut State, cont: &mut Cont) -> bool {
        if st.budget == 0 {
            return false;
        }
        st.budget -= 1;
        match atom {
            Atom::Char(c) => pos < s.len() && s[pos] == *c && cont(st, pos + 1),
            Atom::Any => pos < s.len() && s[pos] != '\n' && cont(st, pos + 1),
            Atom::Class { negated, ranges } => {
                pos < s.len()
                    && ranges.iter().any(|(lo, hi)| *lo <= s[pos] && s[pos] <= *hi) != *negated
                    && cont(st, pos + 1)
            }
            Atom::Start => pos == 0 && cont(st, pos),
            Atom::End => pos == s.len() && cont(st, pos),
            Atom::Group(ast) => match_ast(ast, s, pos, st, cont),
        }
    }

    struct Parser<'a> {
        c: &'a [char],
        i: usize,
    }

    impl Parser<'_> {
        fn peek(&self) -> Option<char> {
            self.c.get(self.i).copied()
        }

        fn bump(&mut self) -> Option<char> {
            let c = self.peek();
            if c.is_some() {
                self.i += 1;
            }
            c
        }

        fn eat(&mut self, c: char) -> bool {
            if self.peek() == Some(c) {
                self.i += 1;
                true
            } else {
                false
            }
        }

        fn parse_alt(&mut self) -> Result<Ast, String> {
            let mut alts = vec![self.parse_seq()?];
            while self.eat('|') {
                alts.push(self.parse_seq()?);
            }
            Ok(Ast { alts })
        }

        fn parse_seq(&mut self) -> Result<Vec<Item>, String> {
            let mut items = Vec::new();
            while let Some(c) = self.peek() {
                if c == '|' || c == ')' {
                    break;
                }
                let atom = self.parse_atom()?;
                items.push(self.parse_quant(atom)?);
            }
            Ok(items)
        }

        fn parse_quant(&mut self, atom: Atom) -> Result<Item, String> {
            let (min, max, quantified) = match self.peek() {
                Some('*') => {
                    self.i += 1;
                    (0, u32::MAX, true)
                }
                Some('+') => {
                    self.i += 1;
                    (1, u32::MAX, true)
                }
                Some('?') => {
                    self.i += 1;
                    (0, 1, true)
                }
                Some('{') => match self.braces() {
                    Some((min, max, len)) => {
                        if max < min {
                            return Err("quantifier {m,n} with n < m".into());
                        }
                        self.i += len;
                        (min, max, true)
                    }
                    // A malformed `{` is a literal — same as real regex.
                    None => (1, 1, false),
                },
                _ => (1, 1, false),
            };
            // `?` directly after a quantifier is the lazy marker.
            let greedy = !(quantified && self.eat('?'));
            Ok(Item {
                atom,
                min,
                max,
                greedy,
            })
        }

        fn parse_atom(&mut self) -> Result<Atom, String> {
            match self.bump() {
                None => Err("unexpected end of pattern".into()),
                Some('(') => {
                    if self.peek() == Some('?') {
                        self.i += 1;
                        // We don't capture, so (?:...) is just a group.
                        // (?= (?! (?<name> etc. are unsupported.
                        if !self.eat(':') {
                            return Err("unsupported group '(?' — only (?:...) is supported".into());
                        }
                    }
                    let ast = self.parse_alt()?;
                    if !self.eat(')') {
                        return Err("unclosed '('".into());
                    }
                    Ok(Atom::Group(ast))
                }
                Some('[') => self.parse_class(),
                Some('.') => Ok(Atom::Any),
                Some('^') => Ok(Atom::Start),
                Some('$') => Ok(Atom::End),
                Some('\\') => self.parse_escape(),
                Some(c @ ('*' | '+' | '?')) => Err(format!("'{c}' has nothing to quantify")),
                Some(c) => Ok(Atom::Char(c)),
            }
        }

        fn parse_escape(&mut self) -> Result<Atom, String> {
            match self.bump() {
                None => Err("trailing '\\'".into()),
                Some(e) => {
                    if let Some(ranges) = class_ranges(e) {
                        return Ok(Atom::Class {
                            negated: e.is_ascii_uppercase(),
                            ranges,
                        });
                    }
                    Ok(Atom::Char(escaped_char(e)?))
                }
            }
        }

        fn parse_class(&mut self) -> Result<Atom, String> {
            let negated = self.eat('^');
            let mut ranges = Vec::new();
            let mut first = true;
            loop {
                let c = match self.bump() {
                    None => return Err("unclosed '['".into()),
                    // `]` as the first char is a literal (POSIX behavior).
                    Some(']') if !first => break,
                    Some(c) => c,
                };
                first = false;
                let lo = if c == '\\' {
                    match self.bump() {
                        None => return Err("trailing '\\' in class".into()),
                        Some(e) => {
                            if let Some(mut r) = class_ranges(e) {
                                // Negated shorthand inside a class would
                                // need complement ranges — reject instead.
                                if e.is_ascii_uppercase() {
                                    return Err(format!("'\\{e}' is not supported inside [...]"));
                                }
                                ranges.append(&mut r);
                                continue;
                            }
                            escaped_char(e)?
                        }
                    }
                } else {
                    c
                };
                // `a-z` is a range, but `-]` makes the '-' a literal.
                if self.peek() == Some('-') && self.c.get(self.i + 1).is_some_and(|&n| n != ']') {
                    self.i += 1; // consume '-'
                    let hi = match self.bump() {
                        Some('\\') => match self.bump() {
                            Some(e) if class_ranges(e).is_none() => escaped_char(e)?,
                            _ => return Err("bad range end in class".into()),
                        },
                        Some(h) => h,
                        None => return Err("unclosed '['".into()),
                    };
                    if hi < lo {
                        return Err(format!("reversed range '{lo}-{hi}'"));
                    }
                    ranges.push((lo, hi));
                } else {
                    ranges.push((lo, lo));
                }
            }
            Ok(Atom::Class { negated, ranges })
        }

        /// Parse `{m}` `{m,}` `{m,n}` at self.i (pointing at '{').
        /// Returns (min, max, chars consumed), or None when the braces
        /// aren't a quantifier — a malformed `{` is a literal in real
        /// regex too.
        fn braces(&self) -> Option<(u32, u32, usize)> {
            let mut j = self.i + 1;
            let s1 = j;
            while j < self.c.len() && self.c[j].is_ascii_digit() {
                j += 1;
            }
            if j == s1 {
                return None;
            }
            let min: u32 = self.c[s1..j].iter().collect::<String>().parse().ok()?;
            let max = if self.c.get(j) == Some(&',') {
                j += 1;
                let s2 = j;
                while j < self.c.len() && self.c[j].is_ascii_digit() {
                    j += 1;
                }
                if j == s2 {
                    u32::MAX
                } else {
                    self.c[s2..j].iter().collect::<String>().parse().ok()?
                }
            } else {
                min
            };
            if self.c.get(j) == Some(&'}') {
                Some((min, max, j + 1 - self.i))
            } else {
                None
            }
        }
    }

    /// `\d` `\w` `\s` (and uppercase negations) → range sets.
    /// None for any other escape.
    fn class_ranges(e: char) -> Option<Vec<(char, char)>> {
        let ranges = match e.to_ascii_lowercase() {
            'd' => vec![('0', '9')],
            'w' => vec![('a', 'z'), ('A', 'Z'), ('0', '9'), ('_', '_')],
            's' => vec![
                (' ', ' '),
                ('\t', '\t'),
                ('\n', '\n'),
                ('\r', '\r'),
                ('\x0b', '\x0b'),
                ('\x0c', '\x0c'),
            ],
            _ => return None,
        };
        Some(ranges)
    }

    /// Single-char escapes. Unrecognized ASCII letters are likely real
    /// regex syntax (`\b` `\A` `\p{...}`) — error rather than silently
    /// matching a letter. Non-letters escape to themselves (`\.` `\\`).
    fn escaped_char(e: char) -> Result<char, String> {
        match e {
            'n' => Ok('\n'),
            't' => Ok('\t'),
            'r' => Ok('\r'),
            'f' => Ok('\x0c'),
            'v' => Ok('\x0b'),
            '0' => Ok('\0'),
            c if c.is_ascii_alphabetic() => Err(format!("unsupported escape '\\{c}'")),
            c => Ok(c),
        }
    }
}
