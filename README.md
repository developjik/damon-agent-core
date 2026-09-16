# Damon agent core

**English** | [한국어](README.ko.md)

A pun on "daemon" and the actual architecture: an open-source, multi-provider agent core that stays running on macOS + Windows + Linux.

You build exactly one daemon. If the API is right, any program can attach as a thin client — yours or someone else's.

## Architecture

```
[web] [CLI] [telegram] [discord] [slack] [browser ext] [mobile] [desktop app]   all thin clients
                      |
        Single API: ACP over WS/JSON-RPC + OpenAI-compatible HTTP
                      |
               Damon core (local daemon · Rust)
                ├─ agent runtime (tool loop)
                ├─ provider adapters (OpenAI-compatible LLM APIs)
                ├─ session / memory store
                └─ tool system = MCP client (whale map, ECOS, TradingView connectors)
```

Desktop apps (Electron/Tauri) are the same kind of thin client — instead of building one, we ship docs and examples for the attach-to-resident-daemon pattern.

## Principles

1. Core first, surfaces later. Build the surface first and you die.
2. The core is exposed through a single API. That's what makes "reuse it from anywhere" free.
3. API keys live only in the OS credential store (macOS Keychain, Windows Credential Manager, Linux Secret Service). Never in code or the repo.
4. Reuse standard protocols (OpenAI-compatible schema, ACP, MCP). Don't invent your own adapter layer.
5. The core must run on macOS, Windows, and Linux. Platform-specific features (keychain, daemon registration, paths) go behind adapters.
6. The API is the product. A stable, versioned contract — once an API is public, it doesn't break.
7. It must be easy to attach: one-line install, single binary/package distribution, client examples and docs shipped with the core.
8. Performance is a feature. It's a resident process, so idle footprint (RAM/CPU) must be small, and the API must add ~0 overhead over provider calls — streaming passes through with no buffering. Fast cold start, no degradation under concurrent sessions.

## Decisions (locked in Phase 0)

- Core role: agent runtime — the core owns the tool loop, sessions, and tools. The proxy is exposed as an OpenAI-compatible endpoint
- Language/runtime: Rust — single binary, minimal footprint, ACP reference ecosystem
- API: native API (WS/JSON-RPC, full-featured) + OpenAI-compatible HTTP endpoint (drop-in)
- Protocols: ACP server (client↔core) + MCP client (core↔tools)
- Local auth: bind to 127.0.0.1 by default + optional token (for multi-user/remote)
- OS: macOS + Windows + Linux
- License: MIT + Apache-2.0 dual
- Config: TOML, platform directories, hot reload
- Reference client: CLI

## Open questions (decided when entering each phase — deciding early is over-engineering)

- Phase 1: daemon registration (launchd / Windows Service / Task Scheduler), auto-start, single-instance/port policy, log location/rotation
- Phase 2: storage (SQLite vs files), session model (forking/compaction), memory definition (conversation history vs long-term memory/RAG), tool source (MCP), tool execution permissions/sandbox
- Phase 3: client SDKs (hand-written TS/Python vs generated from an OpenAPI spec)
- Phase 4: remote relay — E2E encryption, pairing, NAT traversal (reuse Tailscale vs own relay)
- Phase 5: distribution channels (npm/brew/binaries), CI matrix, name/registry conflict check, contribution policy (DCO)

## Roadmap

- Phase 0: one-page spec + gate criteria (done)
- Phase 1: core daemon boot (Rust), one provider adapter, health-check endpoint, performance baseline bench (idle footprint, streaming overhead)
- Phase 2: memory + tool system (done — SQLite session store, MCP client, WS/JSON-RPC ACP API, tool loop)
- Phase 3: reference CLI client + integration guide (done — `damon` CLI, `damon::client` library, docs/integration.md)
- Phase 4: channel expansion + remote relay (E2E) (done — `damon-telegram`/`damon-discord`/`damon-slack` adapters, wss/TLS, mandatory token on non-loopback binds, `damon-relay` + X25519/AES-256-GCM E2E tunnel)
- Phase 5: open-source release (done — MIT/Apache-2.0, `damond service install`, CI matrix, npm `damon-agent`, crates.io `damon-core`)

## Quickstart

```sh
cargo run --bin damond            # generates a starter config on first run
damond --print-config-path        # show config location
curl localhost:9470/health
curl localhost:9470/v1/chat/completions -d '{"model":"gpt-4o","messages":[...],"stream":true}'
```

CLI: `damon health` / `damon sessions` / `damon chat` / `damon prompt "..."` — `--url`, `--token` (or `DAMON_TOKEN`)

Channel adapters: `damon-telegram --bot-token …` / `damon-discord --bot-token …` / `damon-slack --app-token xapp-… --bot-token xoxb-…` — automatic per-channel session mapping, streaming responses, tool-permission approval by replying "allow"/"deny". Add a new channel by implementing `damon_core::channel::ChannelApi` and wiring it to a `Bridge`.

Remote access: set `tls_cert`/`tls_key` to serve wss. Non-loopback binds require `auth_token`. Self-hosted relay: `damon-relay` (public server, 0.0.0.0:8080) + daemon `[relay]` config → outbound tunnel, X25519+AES-256-GCM E2E. Client: `damon --relay ws://relay:8080 --relay-name <name> --token <auth_token>`.

## Baseline (Apple M4, release)

- idle RSS: 9.3 MB (Phase 1) → 11.7 MB (Phase 2)
- streaming overhead: TTFB +0.7ms, total +0.9ms (20-chunk SSE, vs local mock)
- measure: `cargo run --release --example bench`
