# Damon

**English** | [한국어](README.ko.md)

[![ci](https://github.com/developjik/damon-agent-core/actions/workflows/ci.yml/badge.svg)](https://github.com/developjik/damon-agent-core/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/damon-core.svg)](https://crates.io/crates/damon-core)
[![npm](https://img.shields.io/npm/v/damon-agent.svg)](https://www.npmjs.com/package/damon-agent)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

**One daemon. Every surface.**

Damon is a local, always-on agent core written in Rust. It owns the hard parts — the tool loop, sessions, memory, provider quirks, secrets — and exposes them through a single API. Your CLI, Telegram bot, desktop app, or web UI all become thin clients that attach to the same resident daemon.

The name is a pun on *daemon* — and it's also literally what it is.

## Why Damon

Most agent stacks make you pick a surface first, then bolt on tools and memory inside it. Damon inverts that: the core is the product, and every surface is disposable.

- **OpenAI-compatible endpoint** — point any existing OpenAI client at `http://127.0.0.1:9470/v1` and it just works. Anthropic, Gemini, and Responses-API models are translated to the OpenAI schema, so your client never cares which provider answered.
- **A real agent runtime** — sessions, streaming, tool calls, permission prompts, cancellation, context compaction. Exposed as ACP-style JSON-RPC over WebSocket at `/ws`.
- **Every provider, one config** — OpenAI, Anthropic, Gemini, OpenRouter, Groq, DeepSeek, vLLM, Ollama (auto-discovered, zero config). Model globs route requests; `model:low/medium/high` suffixes map to each provider's thinking controls.
- **Tools via MCP** — declare stdio MCP servers in TOML and their tools join the loop, namespaced as `server.tool`. Per-server `auto_approve` or interactive permission prompts.
- **Chat channels out of the box** — Telegram, Discord, and Slack adapters ship as separate binaries. Per-chat session mapping, streamed replies, approve tool calls by replying `allow`/`deny`.
- **Secrets never touch disk in plaintext** — `env:`, `keychain:`, or `!cmd` references only; literal keys are rejected. OAuth login for Anthropic stores tokens in the OS keychain and auto-refreshes.
- **Reachable from anywhere** — serve `wss` with your own certs, or run `damon-relay` on a public host: the daemon dials out (no inbound port), and the tunnel is end-to-end encrypted with X25519 + AES-256-GCM. The relay sees only ciphertext.
- **Built to stay resident** — ~12 MB idle RSS, streaming passes through with sub-millisecond overhead, fast cold start, no degradation under concurrent sessions.

## Install

```sh
npm install -g damon-agent        # npm (prebuilt binaries)
cargo install damon-core          # crates.io
brew install developjik/tap/damon # Homebrew tap
```

Or grab a platform tarball from [GitHub Releases](https://github.com/developjik/damon-agent-core/releases) — macOS (arm64/x86_64), Linux, Windows.

Run it as an OS service so it's always up:

```sh
damond service install   # launchd / systemd user / Task Scheduler
damond service print     # preview the definition first
```

## Quickstart

```sh
damond                          # first run writes a starter config
damond --print-config-path      # where the config lives
damond doctor                   # verify config, secrets, provider reachability
```

Add a provider key (env var, keychain, or `op read …` — never a literal), then:

```sh
curl localhost:9470/health
curl -N localhost:9470/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

Talk to it with the bundled CLI:

```sh
damon chat                      # interactive REPL (streams, tool prompts)
damon prompt "summarize this repo"
damon sessions                  # list sessions
damon resume <id>               # pick a session back up
damon search "error AND timeout"  # FTS5 full-text search over all history
```

## Architecture

```
[CLI] [Telegram] [Discord] [Slack] [web] [desktop app] [your code]
                              |
        Single API: OpenAI-compatible HTTP + ACP-style WS/JSON-RPC
                              |
                    Damon core (resident daemon · Rust)
                     ├─ agent runtime — tool loop, permissions, compaction
                     ├─ provider adapters — OpenAI / Anthropic / Gemini / Responses
                     ├─ session & memory store — SQLite + FTS5
                     └─ MCP client — tools from any stdio MCP server
```

Desktop apps (Electron/Tauri) are just another thin client: attach to `ws://127.0.0.1:9470/ws`, or spawn `damond` as a sidecar. See [docs/integration.md](docs/integration.md) for the wire protocol and copy-paste clients in Node, Python, and Rust.

## Configuration

One TOML file in the platform config dir, hot-reloaded on change:

```toml
[providers.default]
api      = "openai-completions"
base_url = "https://api.openai.com/v1"
api_key  = "env:OPENAI_API_KEY"     # env:VAR | keychain:svc/acct | "!op read …"
models   = ["gpt-*"]

[providers.claude]
api     = "anthropic-messages"
api_key = "oauth"                   # `damond login anthropic` → OS keychain
models  = ["claude-*"]

[mcp_servers.filesystem]
command = "npx"
args    = ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
auto_approve = false                # client gets a permission prompt per call
```

Ollama needs no config at all — damond probes `$OLLAMA_HOST` and routes discovered models automatically. Endpoint quirks (Mistral tool ids, `max_completion_tokens`, in-band tools for local models, …) are handled by per-provider `compat` flags. Full reference: [config.example.toml](config.example.toml).

## Chat channels

```sh
damon-telegram --bot-token <token>
damon-discord  --bot-token <token>
damon-slack    --app-token xapp-… --bot-token xoxb-…
```

Each channel chat maps to its own daemon session; replies stream in; tool-permission requests are approved by replying `allow` or `deny`. Add your own channel by implementing `damon_core::channel::ChannelApi` (`ready`/`recv`/`send`) and handing it to a `Bridge` — session mapping, event demux, and the permission flow are already done.

## Remote access

- **Tailscale** (recommended): attach to `ws://<tailscale-ip>:9470/ws` — WireGuard E2E, zero daemon config.
- **Direct TLS**: set `tls_cert`/`tls_key` to serve `wss`. Non-loopback binds refuse to start without `auth_token`.
- **Self-hosted relay**: run `damon-relay` on a public host (set `DAMON_RELAY_SECRET` to require it on `/register`, preventing name squatting — the daemon presents it via `[relay] secret = "env:RELAY_SECRET"`), add `[relay]` to the daemon config — the daemon dials out, so no inbound port is needed. Clients connect with `damon --relay ws://relay:8080 --relay-name <name> --token <auth_token>`. X25519 key exchange + `sha256(auth_token ‖ pubkey)` proof → AES-256-GCM; the relay never sees plaintext.

## Performance

Baseline on Apple M4, release build:

| Metric | Value |
|---|---|
| Idle RSS | 11.7 MB |
| Streaming overhead | +0.7 ms TTFB, +0.9 ms total (20-chunk SSE vs local mock) |

Reproduce: `cargo run --release --example bench`

## Docs

- [docs/install.md](docs/install.md) — install paths, service registration, OAuth login
- [docs/integration.md](docs/integration.md) — wire protocol, provider/compat reference, minimal clients
- [config.example.toml](config.example.toml) — every option, annotated
- [examples/client.rs](examples/client.rs) — Rust client (`cargo run --example client`)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). CI runs `cargo test` on macOS, Windows, and Linux.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
