//! OS service registration: launchd (macOS), systemd user unit (Linux),
//! Task Scheduler (Windows). Generators are pure functions for testability.

use std::path::PathBuf;

/// launchd plist for a user agent.
pub fn launchd_plist(exe: &str, config: &str, log: &str) -> String {
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

/// systemd user unit.
pub fn systemd_unit(exe: &str, config: &str) -> String {
    format!(
        "[Unit]\n\
         Description=Damon agent core daemon\n\
         After=network.target\n\
         \n\
         [Service]\n\
         ExecStart={exe} --config {config}\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    )
}

/// schtasks command for Windows logon start.
pub fn schtasks_command(exe: &str, config: &str) -> String {
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
        let log = format!(
            "{}/Library/Logs/damond.log",
            std::env::var("HOME").unwrap_or_default()
        );
        let path = launchd_plist_path();
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(&path, launchd_plist(exe, config, &log))?;
        let status = std::process::Command::new("launchctl")
            .args(["load", "-w"])
            .arg(&path)
            .status()?;
        anyhow::ensure!(status.success(), "launchctl load failed");
        return Ok(format!("installed launchd agent: {}", path.display()));
    }
    #[cfg(target_os = "linux")]
    {
        let path = systemd_unit_path();
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(&path, systemd_unit(exe, config))?;
        let status = std::process::Command::new("systemctl")
            .args(["--user", "enable", "--now", "damond"])
            .status()?;
        anyhow::ensure!(status.success(), "systemctl enable failed");
        return Ok(format!("installed systemd user unit: {}", path.display()));
    }
    #[cfg(target_os = "windows")]
    {
        let cmd = schtasks_command(exe, config);
        let status = std::process::Command::new("cmd")
            .args(["/C", &cmd])
            .status()?;
        anyhow::ensure!(status.success(), "schtasks failed");
        return Ok("registered Task Scheduler task: damond".to_string());
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    anyhow::bail!("service install not supported on this platform")
}

/// Print the service definition without installing (for review/pipes).
pub fn print_definition(exe: &str, config: &str) -> String {
    #[cfg(target_os = "macos")]
    {
        let log = "~/Library/Logs/damond.log";
        return launchd_plist(exe, config, log);
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
