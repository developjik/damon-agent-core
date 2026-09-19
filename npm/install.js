#!/usr/bin/env node
// Downloads the matching release binary from GitHub Releases.
const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const REPO = "developjik/damon-agent-core";
const VERSION = require("./package.json").version;

const targets = {
  "darwin-arm64": "aarch64-apple-darwin",
  "darwin-x64": "x86_64-apple-darwin",
  "linux-x64": "x86_64-unknown-linux-gnu",
  "win32-x64": "x86_64-pc-windows-msvc",
};

const key = `${process.platform}-${process.arch}`;
const target = targets[key];
if (!target) {
  console.error(`unsupported platform: ${key}`);
  process.exit(1);
}

const ext = process.platform === "win32" ? ".exe" : "";
const expected = [`damond${ext}`, `damon${ext}`, `damon-telegram${ext}`, `damon-discord${ext}`, `damon-slack${ext}`, `damon-relay${ext}`];

const url = `https://github.com/${REPO}/releases/download/v${VERSION}/damon-${target}.tar.gz`;
const sumUrl = `${url}.sha256`;
const bin = path.join(__dirname, "bin");
fs.mkdirSync(bin, { recursive: true });
const tarball = path.join(bin, "damon.tar.gz");

function fail(msg) {
  console.error(`install failed: ${msg}`);
  process.exit(1);
}

function hasCurl() {
  const probe = process.platform === "win32" ? "where curl" : "which curl";
  try {
    execSync(probe, { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

async function download() {
  try {
    if (hasCurl()) {
      // --connect-timeout: dead host fails fast; --max-time: a stalled
      // transfer must not hang the postinstall forever.
      execSync(`curl -fsSL --connect-timeout 15 --max-time 300 "${url}" -o "${tarball}"`, { stdio: "inherit" });
      return;
    }
    // No curl: fall back to Node's built-in fetch (Node >= 18).
    const res = await fetch(url, { redirect: "follow", signal: AbortSignal.timeout(300_000) });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText}`);
    }
    fs.writeFileSync(tarball, Buffer.from(await res.arrayBuffer()));
  } catch (err) {
    // A failed transfer must not leave a partial tarball in bin/ — it
    // would fail the checksum anyway, but the stale file muddies the
    // next run's diagnostics.
    try {
      fs.unlinkSync(tarball);
    } catch {
      // nothing to clean if the file was never created
    }
    throw err;
  }
}

async function fetchText(u) {
  const res = await fetch(u, { redirect: "follow" });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return res.text();
}

async function main() {
  try {
    await download();
  } catch (err) {
    console.error(`failed to download ${url}`);
    console.error(`  ${err.message}`);
    console.error(
      `hint: check that release v${VERSION} exists — https://github.com/${REPO}/releases/tag/v${VERSION}`
    );
    process.exit(1);
  }

  if (!fs.existsSync(tarball) || fs.statSync(tarball).size === 0) {
    fail(`downloaded tarball is empty: ${url}`);
  }

  // Verify the release checksum before extraction — a tampered or
  // truncated artifact must never reach the install.
  let expectedSha;
  try {
    expectedSha = (await fetchText(sumUrl)).trim().split(/\s+/)[0];
  } catch (err) {
    fail(`could not fetch checksum ${sumUrl}: ${err.message}`);
  }
  const actualSha = require("crypto")
    .createHash("sha256")
    .update(fs.readFileSync(tarball))
    .digest("hex");
  if (actualSha !== expectedSha) {
    fs.unlinkSync(tarball);
    fail(`checksum mismatch for ${path.basename(tarball)}: expected ${expectedSha}, got ${actualSha}`);
  }

  try {
    execSync(`tar xzf "${tarball}" -C "${bin}"`, { stdio: "inherit" });
  } catch {
    fail(`could not extract ${tarball} — corrupt download? (url: ${url})`);
  }
  fs.unlinkSync(tarball);

  const missing = expected.filter((f) => !fs.existsSync(path.join(bin, f)));
  if (missing.length > 0) {
    fail(`tarball missing expected binaries: ${missing.join(", ")} (url: ${url})`);
  }
  // npm links bin shims at install time, BEFORE postinstall extracts the
  // tarball — on Windows the extensionless targets (bin/damond) never
  // exist, so npm skips shim creation and no command lands on PATH.
  // Write extensionless launchers next to the .exe so the shims resolve.
  if (process.platform === "win32") {
    for (const f of expected) {
      const base = f.replace(/\.exe$/, "");
      const launcher = path.join(bin, base);
      if (!fs.existsSync(launcher)) {
        fs.writeFileSync(launcher, `#!/bin/sh\nexec "$(dirname "$0")/${f}" "$@"\n`);
      }
      // cmd.exe can't run the sh launcher — write a .cmd one-liner that
      // calls the real exe relative to the script's own directory.
      const cmd = path.join(bin, `${base}.cmd`);
      if (!fs.existsSync(cmd)) {
        fs.writeFileSync(cmd, `@"%~dp0${f}" %*\r\n`);
      }
    }
  }

  // chmod is a no-op on Windows (NTFS has no POSIX bits) — skip it
  // there rather than rely on its silent failure.
  if (process.platform !== "win32") {
    for (const f of fs.readdirSync(bin)) {
      fs.chmodSync(path.join(bin, f), 0o755);
    }
  }
  console.log(`damon ${VERSION} installed for ${target}`);
}

main().catch((err) => fail(err.message));
