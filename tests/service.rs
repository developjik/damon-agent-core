use damon_core::service::*;

#[test]
fn launchd_plist_is_valid_xml() {
    let plist = launchd_plist("/usr/local/bin/damond", "/etc/damon.toml", "/tmp/d.log");
    assert!(plist.contains("dev.damon.damond"));
    assert!(plist.contains("/usr/local/bin/damond"));
    assert!(plist.contains("RunAtLoad"));
    assert!(plist.contains("KeepAlive"));
}

#[test]
fn systemd_unit_has_restart() {
    let unit = systemd_unit("/usr/bin/damond", "/etc/damon.toml");
    // Paths are quoted — a space in the install dir must not split ExecStart.
    assert!(unit.contains("ExecStart=\"/usr/bin/damond\" --config \"/etc/damon.toml\""));
    assert!(unit.contains("Restart=on-failure"));
    assert!(unit.contains("WantedBy=default.target"));
}

#[test]
fn schtasks_escapes_paths() {
    let cmd = schtasks_command("C:\\damon\\damond.exe", "C:\\damon\\config.toml");
    assert!(cmd.contains("ONLOGON"));
    assert!(cmd.contains("damond.exe"));
}

#[test]
fn launchd_plist_runs_config_and_logs() {
    let plist = launchd_plist(
        "/opt/damon/damond",
        "/opt/damon/damon.toml",
        "/var/log/damond.log",
    );
    // ProgramArguments: exe, --config, config path — in order.
    let exe_at = plist.find("/opt/damon/damond").unwrap();
    let flag_at = plist.find("--config").unwrap();
    let cfg_at = plist.find("/opt/damon/damon.toml").unwrap();
    assert!(exe_at < flag_at && flag_at < cfg_at);
    // stdout and stderr both go to the log path.
    assert_eq!(plist.matches("/var/log/damond.log").count(), 2);
    assert!(plist.contains("StandardOutPath"));
    assert!(plist.contains("StandardErrorPath"));
}

#[test]
fn systemd_unit_has_all_sections() {
    let unit = systemd_unit("/usr/bin/damond", "/etc/damon.toml");
    for section in ["[Unit]", "[Service]", "[Install]"] {
        assert!(unit.contains(section), "missing {section}");
    }
    assert!(unit.contains("After=network.target"));
    assert!(unit.contains("RestartSec=5"));
}

#[test]
fn schtasks_command_is_well_formed() {
    let cmd = schtasks_command("C:\\damon\\damond.exe", "C:\\damon\\config.toml");
    assert!(cmd.starts_with("schtasks /Create"));
    assert!(cmd.contains("/TN damond"));
    assert!(cmd.contains("/SC ONLOGON"));
    assert!(cmd.ends_with("/F"));
    // exe and config are quoted inside /TR.
    assert!(cmd.contains("\\\"C:\\damon\\damond.exe\\\""));
    assert!(cmd.contains("\\\"C:\\damon\\config.toml\\\""));
}

#[test]
fn systemd_unit_escapes_percent() {
    let unit = systemd_unit("/usr/bin/da%mond", "/etc/da%mon.toml");
    // `%` starts a specifier — a literal percent must be doubled.
    assert!(unit.contains("da%%mond"), "{unit}");
    assert!(unit.contains("da%%mon.toml"), "{unit}");
}

#[test]
fn schtasks_escapes_embedded_quote_and_trailing_backslash() {
    let cmd = schtasks_command("C:\\da\"mon\\damond.exe\\", "C:\\c.toml");
    // An embedded quote becomes \" inside the /TR value…
    assert!(cmd.contains("\\\"C:\\da\\\"mon\\damond.exe"), "{cmd}");
    // …and the trailing backslash is doubled so it can't escape the
    // generated closing quote.
    assert!(cmd.contains("damond.exe\\\\\\\""), "{cmd}");
    // Segments with nothing to escape stay byte-identical.
    assert!(cmd.contains("\\\"C:\\c.toml\\\""), "{cmd}");
}

#[test]
fn install_paths_have_expected_filenames() {
    assert_eq!(
        launchd_plist_path().file_name().unwrap(),
        "dev.damon.damond.plist"
    );
    assert_eq!(systemd_unit_path().file_name().unwrap(), "damond.service");
}

#[test]
fn print_definition_matches_platform_generator() {
    let def = print_definition("/usr/bin/damond", "/etc/damon.toml");
    #[cfg(target_os = "macos")]
    assert!(def.contains("<plist"), "{def}");
    #[cfg(target_os = "linux")]
    assert!(def.contains("[Service]"), "{def}");
    #[cfg(target_os = "windows")]
    assert!(def.contains("schtasks"), "{def}");
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    assert_eq!(def, "unsupported platform");
}
