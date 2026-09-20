//! OS service registration: launchd (macOS), systemd user unit (Linux),
//! Task Scheduler (Windows). Generators are pure functions for testability.

use std::path::PathBuf;

/// launchd plist for a user agent.
pub fn launchd_plist(exe: &str, config: &str, log: &str) -> String {
    // Paths go into XML verbatim — escape or a `&`/`<` in a path
    // produces a malformed plist that launchd silently rejects.
    let (exe, config, log) = (xml_escape(exe), xml_escape(config), xml_escape(log));
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>dev.damon.damond</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>--config</string>
        <string>{config}</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>StandardOutPath</key><string>{log}</string>
    <key>StandardErrorPath</key><string>{log}</string>
</dict>
</plist>
"#
    )
}

/// Minimal XML escaping for plist string values.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Escape `"`, `\`, and `%` for a double-quoted systemd ExecStart
/// argument — `%` starts a specifier, so a literal percent is `%%`.
fn systemd_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%")
}

/// systemd user unit.
pub fn systemd_unit(exe: &str, config: &str) -> String {
    // systemd splits ExecStart on whitespace — quote paths so a space
    // in the install dir doesn't produce a broken unit.
    let (exe, config) = (systemd_escape(exe), systemd_escape(config));
    format!(
        "[Unit]\n\
         Description=Damon agent core daemon\n\
         After=network.target\n\
         \n\
         [Service]\n\
         ExecStart=\"{exe}\" --config \"{config}\"\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    )
}

/// Escape a path for the /TR command line: `"` becomes `\"`, and
/// backslashes immediately preceding a generated quote are doubled, so
/// an embedded quote or a trailing `\` can't break out of the task's
/// quoted command line.
fn schtasks_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    let mut slashes = 0usize;
    for c in s.chars() {
        if c == '\\' {
            slashes += 1;
        } else if c == '"' {
            for _ in 0..slashes * 2 {
                out.push('\\');
            }
            out.push_str("\\\"");
            slashes = 0;
        } else {
            for _ in 0..slashes {
                out.push('\\');
            }
            out.push(c);
            slashes = 0;
        }
    }
    // The path ends right before our generated closing quote.
    for _ in 0..slashes * 2 {
        out.push('\\');
    }
    out
}

/// schtasks command for Windows logon start.
pub fn schtasks_command(exe: &str, config: &str) -> String {
    let (exe, config) = (schtasks_escape(exe), schtasks_escape(config));
    format!(
        "schtasks /Create /TN damond /SC ONLOGON /TR \"\\\"{exe}\\\" --config \\\"{config}\\\"\" /F"
    )
}

pub fn launchd_plist_path() -> PathBuf {
    directories::BaseDirs::new()
        .map(|d| {
            d.home_dir()
                .join("Library/LaunchAgents/dev.damon.damond.plist")
        })
        .unwrap_or_else(|| PathBuf::from("dev.damon.damond.plist"))
}

pub fn systemd_unit_path() -> PathBuf {
    directories::BaseDirs::new()
        .map(|d| d.config_dir().join("systemd/user/damond.service"))
        .unwrap_or_else(|| PathBuf::from("damond.service"))
}

/// Install the service for the current platform. Returns what was done.
pub fn install(exe: &str, config: &str) -> anyhow::Result<String> {
    #[cfg(target_os = "macos")]
    {
        // $HOME unset would produce "/Library/Logs/damond.log" — a
        // root-owned path the user agent can't write.
        let home = directories::BaseDirs::new()
            .map(|d| d.home_dir().to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let log = format!("{}/Library/Logs/damond.log", home.display());
        let path = launchd_plist_path();
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(&path, launchd_plist(exe, config, &log))?;
        let status = std::process::Command::new("launchctl")
            .args(["load", "-w"])
            .arg(&path)
            .status()?;
        anyhow::ensure!(status.success(), "launchctl load failed");
        Ok(format!("installed launchd agent: {}", path.display()))
    }
    #[cfg(target_os = "linux")]
    {
        let path = systemd_unit_path();
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(&path, systemd_unit(exe, config))?;
        // Reload first — an existing unit file would otherwise leave
        // systemd running the stale definition until the next reload.
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
        let status = std::process::Command::new("systemctl")
            .args(["--user", "enable", "--now", "damond"])
            .status()?;
        anyhow::ensure!(status.success(), "systemctl enable failed");
        Ok(format!("installed systemd user unit: {}", path.display()))
    }
    #[cfg(target_os = "windows")]
    {
        let cmd = schtasks_command(exe, config);
        let status = std::process::Command::new("cmd")
            .args(["/C", &cmd])
            .status()?;
        anyhow::ensure!(status.success(), "schtasks failed");
        Ok("registered Task Scheduler task: damond".to_string())
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    anyhow::bail!("service install not supported on this platform")
}

/// Print the service definition without installing (for review/pipes).
pub fn print_definition(exe: &str, config: &str) -> String {
    #[cfg(target_os = "macos")]
    {
        // Mirror install(): absolute home from BaseDirs — launchd does
        // not expand a literal `~` in StandardOutPath.
        let home = directories::BaseDirs::new()
            .map(|d| d.home_dir().to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let log = format!("{}/Library/Logs/damond.log", home.display());
        return launchd_plist(exe, config, &log);
    }
    #[cfg(target_os = "linux")]
    {
        return systemd_unit(exe, config);
    }
    #[cfg(target_os = "windows")]
    {
        return schtasks_command(exe, config);
    }
    #[allow(unreachable_code)]
    "unsupported platform".to_string()
}

/// Remove the service registration for the current platform.
/// Best-effort on the stop/unload step — a service that isn't running
/// must not block removal of its definition file.
pub fn uninstall() -> anyhow::Result<String> {
    #[cfg(target_os = "macos")]
    {
        let path = launchd_plist_path();
        if path.exists() {
            // Unload first (ignore failure — may not be loaded), then
            // remove the plist so it can't come back on next login.
            let _ = std::process::Command::new("launchctl")
                .args(["unload", "-w"])
                .arg(&path)
                .status();
            std::fs::remove_file(&path)?;
            Ok(format!("removed launchd agent: {}", path.display()))
        } else {
            Ok("no launchd agent installed".to_string())
        }
    }
    #[cfg(target_os = "linux")]
    {
        let path = systemd_unit_path();
        if path.exists() {
            let _ = std::process::Command::new("systemctl")
                .args(["--user", "disable", "--now", "damond"])
                .status();
            std::fs::remove_file(&path)?;
            let _ = std::process::Command::new("systemctl")
                .args(["--user", "daemon-reload"])
                .status();
            Ok(format!("removed systemd user unit: {}", path.display()))
        } else {
            Ok("no systemd unit installed".to_string())
        }
    }
    #[cfg(target_os = "windows")]
    {
        let status = std::process::Command::new("cmd")
            .args(["/C", "schtasks /Delete /TN damond /F"])
            .status()?;
        anyhow::ensure!(status.success(), "schtasks /Delete failed");
        Ok("removed Task Scheduler task: damond".to_string())
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    anyhow::bail!("service uninstall not supported on this platform")
}

/// Whether a service definition exists and (best-effort) whether the
/// service manager considers it running. Never fails — status is a
/// diagnostic, not a gate.
pub fn status() -> String {
    #[cfg(target_os = "macos")]
    {
        let path = launchd_plist_path();
        if !path.exists() {
            return "not installed".to_string();
        }
        // launchctl list exits 0 and prints the job when loaded.
        let loaded = std::process::Command::new("launchctl")
            .args(["list", "dev.damon.damond"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        return format!(
            "installed: {}\nloaded: {}",
            path.display(),
            if loaded { "yes" } else { "no" }
        );
    }
    #[cfg(target_os = "linux")]
    {
        let path = systemd_unit_path();
        if !path.exists() {
            return "not installed".to_string();
        }
        let active = std::process::Command::new("systemctl")
            .args(["--user", "is-active", "damond"])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        return format!("installed: {}\nactive: {}", path.display(), active);
    }
    #[cfg(target_os = "windows")]
    {
        let out = std::process::Command::new("cmd")
            .args(["/C", "schtasks /Query /TN damond"])
            .output();
        return match out {
            Ok(o) if o.status.success() => "installed: damond task".to_string(),
            _ => "not installed".to_string(),
        };
    }
    #[allow(unreachable_code)]
    "unsupported platform".to_string()
}
