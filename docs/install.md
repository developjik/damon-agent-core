# Install

**English** | [한국어](install.ko.md)

## Verified install paths

| Path | Status |
| --- | --- |
| GitHub Releases tarball | Built by CI on tag push (4 targets) |
| `npm install -g damon-agent` | CI publishes on tag push; postinstall downloads the release tarball |
| `cargo install damon-core` | CI publishes to crates.io on tag push |
| `brew install developjik/tap/damon` | Requires the tap repository — see below |

## Binaries

Platform tarballs on GitHub Releases:

```sh
curl -fsSL https://github.com/developjik/damon-agent-core/releases/latest/download/damon-aarch64-apple-darwin.tar.gz | tar xz
```

Each tarball contains `damond`, `damon`, `damon-telegram`, `damon-discord`, `damon-slack`, and `damon-relay`.

## npm

```sh
npm install -g damon-agent
```

Six binaries are linked onto your PATH. The package also exports a
zero-dependency Node client (`import { DamonClient } from "damon-agent"`) —
see [integration.md](integration.md).

## cargo

```sh
cargo install damon-core
```

## Homebrew

Once the tap repository (`developjik/homebrew-tap`) exists:

```sh
brew install developjik/tap/damon
```

The formula lives in this repository at `Formula/damon.rb` — its `sha256`
must be updated on each release.

## Running as a resident service

```sh
damond service install    # launchd (macOS) / systemd user (Linux) / Task Scheduler (Windows)
damond service print      # preview the definition before installing
```

## Configuration

```sh
damond --print-config-path   # where config.toml lives
```

See `config.example.toml`. Agent CLIs keep their own logins (each detected
CLI — `claude`, `codex`, `omp`, `cursor`, `amp`, `kimi`, `qwen`, `gemini` —
authenticates itself) — Damon stores no agent tokens.


## Remote relay

Run `damon-relay` on a public host (binds 0.0.0.0:8080), then add to the
daemon config:

```toml
[relay]
url  = "ws://your-relay:8080"
name = "my-daemon"
```

The daemon dials out — no inbound port needed. Clients connect with:

```sh
damon --relay ws://your-relay:8080 --relay-name my-daemon --token <auth_token> chat
```

E2E: X25519 key exchange + `sha256(auth_token || pubkey)` proof →
AES-256-GCM. The relay sees only ciphertext.

**The relay also serves the web UI.** Open `http://your-relay:8080/` in any
browser (a phone on cellular works too), enter the daemon name and
`auth_token`, and the page connects back through the relay end-to-end
encrypted — no VPN, no port forwarding, no hosting.
