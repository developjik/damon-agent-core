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
    assert!(unit.contains("ExecStart=/usr/bin/damond --config /etc/damon.toml"));
    assert!(unit.contains("Restart=on-failure"));
    assert!(unit.contains("WantedBy=default.target"));
}

#[test]
fn schtasks_escapes_paths() {
    let cmd = schtasks_command("C:\\damon\\damond.exe", "C:\\damon\\config.toml");
    assert!(cmd.contains("ONLOGON"));
    assert!(cmd.contains("damond.exe"));
}
