//! Daemon discovery file: `~/.damon/daemon.json`.
//!
//! Local clients (CLI, TUI, IDE plugins) read this file to find the
//! running daemon's port without parsing the daemon's config — the
//! config path itself is published so clients can also show *which*
//! config the daemon booted from.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// On-disk shape, in the order clients see the keys. Deliberately
/// carries NO secrets — port/pid/version/tls/configPath only. The auth
/// token must never touch disk in plaintext; putting it here would hand
/// it to every local reader of the file (and to backups/root sweeps
/// that legitimately traverse `~/.damon` for the store).
#[derive(Serialize, Deserialize)]
struct Discovery {
    port: u16,
    pid: u32,
    version: String,
    tls: bool,
    #[serde(rename = "configPath")]
    config_path: String,
}

/// Force `mode` on `path`. Mirrors store.rs's helper: the discovery
/// file is 0600 and `~/.damon` 0700 regardless of umask; a looser
/// pre-existing path is tightened with a warning, and failures only
/// warn so discovery never blocks boot.
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
                "discovery path was group/world-accessible; tightened permissions"
            );
        }
        Err(e) => {
            warn!(error = %e, path = %path.display(), "cannot tighten discovery permissions");
        }
        Ok(()) => {}
    }
}

/// Write `~/.damon/daemon.json` describing this daemon.
///
/// Overwrites any stale file left by a SIGKILLed predecessor — the pid
/// inside is what lets readers (and [`Guard`]) tell stale from live.
/// A missing home directory is not an error: discovery is additive, so
/// we warn and return a no-op [`Guard`].
pub fn write(bind: SocketAddr, tls: bool, config_path: &Path) -> anyhow::Result<Guard> {
    let home = directories::BaseDirs::new().map(|d| d.home_dir().to_path_buf());
    let Some(home) = home else {
        tracing::warn!("no home directory; skipping daemon.json discovery write");
        return Ok(Guard::noop());
    };
    write_under(&home, bind, tls, config_path)
}

/// `write` with an explicit base dir — the seam tests use to point at a
/// temp dir instead of the real `$HOME`.
fn write_under(
    base_dir: &Path,
    bind: SocketAddr,
    tls: bool,
    config_path: &Path,
) -> anyhow::Result<Guard> {
    let dir = base_dir.join(".damon");
    std::fs::create_dir_all(&dir).with_context(|| format!("cannot create {}", dir.display()))?;
    // Force a private dir even under a permissive umask
    // (create_dir_all applies the umask).
    #[cfg(unix)]
    tighten_permissions(&dir, 0o700);

    let path = dir.join("daemon.json");
    let payload = serde_json::to_string(&Discovery {
        port: bind.port(),
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        tls,
        config_path: config_path.display().to_string(),
    })?;
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            // Applies at creation only; the explicit chmod below also
            // tightens a pre-existing looser file we just truncated.
            .mode(0o600)
            .open(&path)
            .with_context(|| format!("cannot create {}", path.display()))?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("cannot chmod {}", path.display()))?;
        file.write_all(payload.as_bytes())
            .with_context(|| format!("cannot write {}", path.display()))?;
    }
    #[cfg(not(unix))]
    std::fs::write(&path, &payload).with_context(|| format!("cannot write {}", path.display()))?;
    Ok(Guard { path: Some(path) })
}

/// Removes the discovery file on drop. Bindings must stay alive for the
/// daemon's whole lifetime (`let _guard = …`, NOT `let _ = …`, which
/// drops immediately).
pub struct Guard {
    /// `None` = nothing to remove (no home dir / write failed at the
    /// call site): drop is a no-op.
    path: Option<PathBuf>,
}

impl Guard {
    /// A [`Guard`] that removes nothing — the fallback for call sites
    /// that treat discovery as best-effort and keep booting on failure.
    pub fn noop() -> Self {
        Guard { path: None }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let Some(path) = self.path.as_ref() else {
            return;
        };
        // Re-parse and compare pids: a successor daemon may have
        // overwritten the file, and removing it would hide *their*
        // daemon from clients.
        let ours = std::fs::read_to_string(path)
            .ok()
            .and_then(|body| serde_json::from_str::<Discovery>(&body).ok())
            .is_some_and(|d| d.pid == std::process::id());
        if !ours {
            return;
        }
        // Warn-only: drop runs on process-exit paths where a panic here
        // would abort mid-shutdown.
        if let Err(e) = std::fs::remove_file(path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(error = %e, path = %path.display(), "cannot remove daemon.json");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::net::{IpAddr, Ipv4Addr};

    // No tempfile dependency; uuid (v4) is already a main dependency, so
    // each test gets a unique temp dir and parallel runs never collide.
    fn temp_base() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "damon-discovery-test-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn cleanup(base: &Path) {
        // Best-effort; the OS temp cleaner is the backstop.
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn write_creates_file_with_all_fields() {
        let base = temp_base();
        let cfg_path = base.join("config.toml");
        let guard = write_under(&base, addr(7777), true, &cfg_path).unwrap();

        let file = base.join(".damon/daemon.json");
        let v: Value = serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
        assert_eq!(v["port"], 7777);
        assert_eq!(v["pid"], std::process::id());
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(v["tls"], true);
        assert_eq!(v["configPath"], cfg_path.display().to_string());
        // Exactly the documented fields — a secret sneaking in here is
        // the one regression this file must never ship.
        assert_eq!(v.as_object().unwrap().len(), 5);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let file_mode = std::fs::metadata(&file).unwrap().permissions().mode();
            assert_eq!(file_mode & 0o777, 0o600, "daemon.json must be 0600 at rest");
            let dir_mode = std::fs::metadata(base.join(".damon"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(dir_mode & 0o777, 0o700, "~/.damon must be 0700");
        }
        drop(guard);
        cleanup(&base);
    }

    #[test]
    fn drop_removes_file_we_own() {
        let base = temp_base();
        let guard = write_under(&base, addr(1), false, Path::new("/x.toml")).unwrap();
        let file = base.join(".damon/daemon.json");
        assert!(file.exists());
        drop(guard);
        assert!(!file.exists(), "Guard::drop must remove our daemon.json");
        cleanup(&base);
    }

    #[test]
    fn drop_spares_file_owned_by_other_pid() {
        let base = temp_base();
        let guard = write_under(&base, addr(1), false, Path::new("/x.toml")).unwrap();
        let file = base.join(".damon/daemon.json");
        // Simulate a successor daemon having overwritten the file.
        let mut v: Value = serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
        v["pid"] = Value::from(std::process::id() + 1);
        std::fs::write(&file, serde_json::to_string(&v).unwrap()).unwrap();

        drop(guard);
        assert!(file.exists(), "must not remove a file another daemon owns");
        cleanup(&base);
    }

    #[test]
    fn write_overwrites_stale_file() {
        let base = temp_base();
        let dir = base.join(".damon");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("daemon.json");
        // Leftover from a SIGKILLed predecessor.
        std::fs::write(
            &file,
            r#"{"port":1,"pid":999999,"version":"0.0.0","tls":false,"configPath":"/old.toml"}"#,
        )
        .unwrap();

        let _guard = write_under(&base, addr(9999), false, Path::new("/x.toml")).unwrap();
        let v: Value = serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
        assert_eq!(v["port"], 9999, "stale file must be replaced");
        assert_eq!(v["pid"], std::process::id());
        cleanup(&base);
    }
}
