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
const expected = [`damond${ext}`, `damon${ext}`, `damon-telegram${ext}`, `damon-discord${ext}`, `damon-slack${ext}`];

const url = `https://github.com/${REPO}/releases/download/v${VERSION}/damon-${target}.tar.gz`;
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
  if (hasCurl()) {
    execSync(`curl -fsSL "${url}" -o "${tarball}"`, { stdio: "inherit" });
    return;
  }
  // No curl: fall back to Node's built-in fetch (Node >= 18).
  const res = await fetch(url, { redirect: "follow" });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} ${res.statusText}`);
  }
  fs.writeFileSync(tarball, Buffer.from(await res.arrayBuffer()));
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

  for (const f of fs.readdirSync(bin)) {
    fs.chmodSync(path.join(bin, f), 0o755);
  }
  console.log(`damon ${VERSION} installed for ${target}`);
}

main().catch((err) => fail(err.message));
