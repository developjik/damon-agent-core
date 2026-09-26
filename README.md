# Damon

**English** | [한국어](README.ko.md)

[![ci](https://github.com/developjik/damon-agent-core/actions/workflows/ci.yml/badge.svg)](https://github.com/developjik/damon-agent-core/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/damon-core.svg)](https://crates.io/crates/damon-core)
[![npm](https://img.shields.io/npm/v/damon-agent.svg)](https://www.npmjs.com/package/damon-agent)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

**One daemon. Every coding agent.**

Damon is a local agent-control daemon written in Rust. It drives Claude Code, Codex CLI, and Oh My Pi through their native CLIs — the model, tools, credentials, and context management belong to the agents; Damon owns sessions, permission relaying, searchable history, chat channels, and remote access. The CLI, Telegram bot, desktop app, and web UI are all thin clients on the same resident daemon.

The name is a pun on *daemon*, and literally the architecture.

## Why Damon

If you already subscribe to an agent CLI, **there is nothing to configure**. Damon detects the installed CLIs and inherits their logins and tools. Token rotation, provider translation, API keys — not Damon's problem anymore.

- **Zero-config backends** — a catalog CLI binary on PATH registers itself: `claude` (stream-json), `codex` (app-server), `omp` (RPC mode), `cursor-agent`, `amp`, `kimi`, `qwen`, `gemini` (stream-json). Any other agent plugs in via an explicit `[backends.X]` block.
- **Permissions flow to your surface** — the agent's permission ask is relayed to the web UI, CLI, or a Telegram/Discord/Slack `allow`/`deny` reply.
- **Searchable history** — every conversation lands in SQLite + FTS5. `damon search "error timeout"` full-text-searches all of it.
- **Chat channels built in** — Telegram, Discord, and Slack adapters ship as separate binaries. Per-chat session mapping, streamed replies.
- **Damon holds no secrets** — agents manage their own auth. The Damon config is a port and maybe a token.
- **Reachable from anywhere** — serve `wss` with your own certs, or run `damon-relay` on a public host and the daemon dials out (no inbound port). The tunnel is X25519 + AES-256-GCM end-to-end encrypted.
- **Built to stay resident** — the Rust daemon itself stays lean (agent processes are the agents' business).

## Install

```sh
npm install -g damon-agent        # npm (prebuilt binaries)
cargo install damon-core          # crates.io
brew install developjik/tap/damon # Homebrew tap
```

Or platform tarballs from [GitHub Releases](https://github.com/developjik/damon-agent-core/releases) — macOS (arm64/x86_64), Linux, Windows.

Keep it running as an OS service:

```sh
damond service install   # launchd / systemd user / Task Scheduler
damond service print     # preview the definition before installing
```

## Quickstart

```sh
damond                          # writes a starter config on first run
damond doctor                   # which backends are installed?
```

With an agent CLI installed and logged in, that's the whole setup:

```sh
curl localhost:9470/health
# {"backends":["claude","omp"],"status":"ok",...}
```

Chat in the bundled web UI: `http://127.0.0.1:9470/ui` — sessions, streaming, permission prompts, no install.

## Architecture

```
[CLI] [Telegram] [Discord] [Slack] [web] [desktop app] [your code]
                              |
        one API: JSON-RPC over WebSocket (`/ws`, protocol v2)
                              |
                    Damon core (resident daemon · Rust)
                     ├─ backend registry — detect, spawn, restart installed CLIs
                     ├─ session manager — lifecycle, permission relay, cancel
                     ├─ session store — SQLite + FTS5 full-text search
                     └─ channel bridges / E2E relay
                              |
            Claude Code · Codex CLI · Oh My Pi  (native CLI subprocesses)
             — model, tools, subscription auth, context all agent-owned
```

See [docs/protocol-v2.md](docs/protocol-v2.md) for the wire protocol and [docs/integration.md](docs/integration.md) for copy-paste Node/Python/Rust clients.

## Configuration

One TOML file in the platform config dir; hot-reloaded:

```toml
# Works with nothing set. Everything below is optional.

# bind = "127.0.0.1:9470"          # default; changing requires restart
# auth_token = "env:DAMON_TOKEN"   # required for non-loopback binds
# default_backend = "claude"       # when session.create omits `backend`

# Backend launch overrides (e.g. a local adapter build):
# [backends.claude]
# command = "/opt/claude"
# [backends.claude.env]
# ANTHROPIC_MODEL = "claude-sonnet-4-5"

# MCP servers forwarded to every session (agents spawn and permission them):
# [mcp_servers.filesystem]
# command = "npx"
# args    = ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
```

Full reference: [config.example.toml](config.example.toml).

## Chat channels

```sh
damon-telegram --bot-token <token>
damon-discord  --bot-token <token>
damon-slack    --app-token xapp-… --bot-token xoxb-…
```

Each chat maps to its own agent session, replies stream as rendered markdown, and a prompt arriving mid-turn is queued (bounded) instead of rejected. Tool-permission requests arrive as native buttons on Telegram, Discord, and Slack (Socket Mode delivers the presses — enable Interactivity with Socket Mode in the app config); a plain `allow`/`deny`/`always` reply works everywhere, and files sent to any channel ride along as prompt attachments (Discord CDN URLs are fetched at prompt time like Slack's). `!cwd <dir>` and `!agent <backend>` steer a conversation's project and backend, alongside `!new`/`!fork`/`!delete`/`!cancel`/`!usage`. Cross-surface pickup: `!sessions` lists recent sessions from every surface, `!resume <id|title>` continues one in this chat (auto-watched), and `!watch`/`!unwatch <id|title>` follow a session so completions and permission asks ping the chat — and `allow`/`deny` answers work there too — even when the turn runs on the web UI or CLI. A new channel implements `damon_core::channel::ChannelApi` (`ready`/`recv`/`send`/`send_permission`/`send_media`) and hands it to `Bridge` — session mapping, event demux, and the permission flow are already there.

## Remote access

- **Tailscale** (recommended): attach at `ws://<tailscale-ip>:9470/ws` — WireGuard E2E, no daemon changes.
- **Direct TLS**: set `tls_cert`/`tls_key` to serve `wss`. Non-loopback binds refuse to start without `auth_token`.
- **Self-hosted relay**: run `damon-relay` on a public host and add `[relay]` — the daemon dials out, so no inbound port. X25519 key exchange + `sha256(auth_token ‖ pubkey)` proof → AES-256-GCM; the relay sees only ciphertext.
- **Web UI anywhere**: the relay itself serves the bundled UI at its root — open `http(s)://your-relay-host/` on any phone or laptop, enter the daemon name and auth token, and the page runs the E2E handshake back through the relay. No VPN, no port forwarding, no app; plain `ws://` hosting works because the browser does its own end-to-end crypto.

## Docs

- **[Project homepage](https://developjik.github.io/damon-agent-core/)** — what Damon is, at a glance
- [docs/protocol-v2.md](docs/protocol-v2.md) — wire protocol (methods, events, types)
- [docs/install.md](docs/install.md) ([ko](docs/install.ko.md)) — install paths, service registration
- [docs/integration.md](docs/integration.md) ([ko](docs/integration.ko.md)) — agent wiring, minimal clients
- [docs/release.md](docs/release.md) ([ko](docs/release.ko.md)) — release checklist
- [config.example.toml](config.example.toml) — every option, commented
- `import { DamonClient } from "damon-agent"` — dependency-free Node client in the npm package
- `pip install damon-agent` — asyncio Python client ([python/](python))

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). CI runs `cargo test` on macOS, Windows, and Linux.

## License

Dual-licensed [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) — your choice.
