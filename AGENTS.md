# Repository Guidelines

## Project Overview

**damon-core** (repo `damon-agent-core`) — "one daemon, every coding agent". A resident local daemon (`damond`) that drives nine coding-agent CLIs (claude, codex, omp, pi, qwen, droid, opencode, mimo, zcode) over their native protocols and exposes one JSON-RPC-over-WebSocket API (`/ws`, protocol v2) with SQLite+FTS5 session history, permission relay, chat-channel bridges (Telegram/Discord/Slack), a skills hub, and an E2E-encrypted outbound relay. CLI (`damon`), bots, and the embedded web UI are all thin clients of the daemon.

## Architecture & Data Flow

Single Rust crate, no workspace. All logic lives in the library (`src/lib.rs`, flat `pub mod` list); `src/bin/` holds six thin binaries: `damond` (daemon), `damon` (CLI client), `damon-relay` (public relay server), `damon-telegram`/`damon-discord`/`damon-slack` (channel bots).

Turn data flow:

```
agent CLI subprocess (NDJSON / stream-json / HTTP+SSE)
  → src/backend/transport.rs or opencode_server.rs   (raw frames)
  → src/backend/<brand>.rs                            (translate to normalized model)
  → tokio broadcast<StreamEvent>                      (AgentSession::subscribe)
  → src/session.rs ManagedSession + EventJournal      (late-subscriber replay, cap 2048)
  → src/rpc.rs run_turn/drive_turn                    (JSON-RPC dispatch, ~70 methods)
  → WebSocket client (src/client.rs / npm / python)   (src/api.rs axum /ws route)
```

Persistence goes through `src/store.rs` (`Store`, tokio-rusqlite wrapper, sessions/messages/projects/usage tables + FTS5 `search()`). Permission asks flow backend → RPC client → `permission.respond` back to the session via oneshot. Chat platforms plug into `src/channel.rs`'s generic `Bridge` via `trait ChannelApi`.

**Backend abstraction (the key pattern, `src/backend/`)**: every agent CLI keeps its native protocol; a per-backend module translates into the single normalized model in `backend/types.rs`. Two async-trait levels: `AgentClient` (detect/catalog/create/resume/import sessions) and `AgentSession` (one conversation: turn stream/steer/permission/interrupt). Three families:

- **stream-json dialects** — claude, qwen: `StreamJsonDialect` impls over the shared engine in `backend/streamjson.rs` (`StreamJsonClient`/`StreamJsonSession`, `ClaudeFrameParser`)
- **NDJSON app-servers** — codex, droid, zcode, omp/pi: `XClient`/`XSession` over `NdjsonTransport`; `omp.rs` has a `PiFamily` profile so pi shares `OmpClient`
- **HTTP+SSE** — opencode, mimo: `OpencodeClient` adopts-or-spawns `opencode serve`

`backend/registry.rs` holds the static `BackendSpec` catalog and `resolve_backends()`/`client_for()`. Unsupported features return typed `NOT_SUPPORTED` errors; `Capabilities` structs advertise what works.

## Key Directories

| Path | Purpose |
|---|---|
| `src/backend/` | Agent-CLI abstraction: traits, normalized types, 8 per-brand translators |
| `src/rpc.rs` | JSON-RPC v2 server over WebSocket — largest module, method dispatcher, `RpcError` |
| `src/api.rs` | Axum HTTP/WS layer: `AppState`, routes, background sweeps, metrics |
| `src/session.rs` | `SessionManager` / `ManagedSession`, idle reaping, event journal |
| `src/store.rs` | SQLite + FTS5 persistence (`Store::open` / `Store::in_memory`) |
| `src/skills_hub/` | Skill package manager: discovery, fetch, install lifecycle, target roots |
| `src/channel.rs` + `telegram.rs`/`slack.rs`/`discord.rs` | Chat bridge + per-platform adapters |
| `src/relay.rs`, `src/bin/damon-relay.rs` | E2E-encrypted outbound tunnel (X25519 + AES-256-GCM) |
| `tests/` | Integration tests (separate crate), shared harness in `tests/common/mod.rs` |
| `npm/`, `python/` | Client SDKs mirroring the Rust client contract (see Testing & QA) |
| `scripts/` | `set-version.sh` (version sync), `verify-release.sh` (release verification) |
| `docs/`, `examples/` | Protocol/install/integration docs (EN/KO pairs); minimal library-usage examples |

## Development Commands

```bash
cargo test                              # full Rust suite (canonical; CI runs on 3 OSes)
cargo run --bin damond                  # run daemon from source
cargo fmt --check                       # formatting gate
cargo clippy --all-targets -- -D warnings   # lint gate (warnings are errors)
cargo check --release --all-targets     # release-cfg compile check
cargo run --example client              # minimal client example (also backend_probe)
npm test                                # in npm/: node --test (Node ≥ 22)
pytest python/tests                     # Python SDK tests (pytest-asyncio, auto mode)
cargo llvm-cov --lcov --output-path lcov.info --fail-under-lines 60   # coverage floor: 60%
cargo audit                             # advisories (separate CI job)
cargo deny check licenses bans sources  # license/ban policy (deny.toml)
```

Linux dev needs `libdbus-1-dev pkg-config`. All six binaries get a `--help` smoke test in CI — keep clap help text valid.

## Code Conventions & Common Patterns

- **Errors**: `anyhow::Result` with `.context()` for internal fallibility; `thiserror` only for wire-typed/domain errors (`RpcError` in `rpc.rs`, `HubError`, `FetchError`, `AskNotFound`, `NameTaken`). RPC handlers return `Result<Value, RpcError>`; codes live in `rpc::error_code` constants.
- **Async**: `#[async_trait]` on all cross-backend traits. `tokio::sync::broadcast` for event fanout (lagging receivers are shed, never block); oneshot for pending permission answers; `Notify` for cancellation; `spawn_blocking` for fs-heavy work.
- **State**: `Arc` everywhere. Short-lived/guard state in `parking_lot::{Mutex, RwLock}`; cross-await state in `tokio::sync::Mutex`; atomics for busy flags/counters. Config is `SharedConfig = Arc<parking_lot::RwLock<Config>>`, hot-reloaded via a `notify` watcher — only `bind` and `[relay]` need a restart.
- **serde**: config structs use `#[serde(deny_unknown_fields)]` + `#[serde(default = ...)]` (see `config.rs`); wire DTOs use `#[serde(rename_all = "camelCase")]` + `skip_serializing_if = "Option::is_none"`. RPC layer is mostly untyped `serde_json::Value` with `req_str`/`json!` helpers.
- **Naming**: provider ids are lowercase CLI names (`"claude"`, `"omp"`); RPC methods are `domain.verb` strings (`session.create`, `turn.start`); test fns are descriptive snake_case behavior sentences (`bridge_question_letter_reply_picks_the_option`).
- **Secrets**: `SecretRef` (`env:`, `keychain:`, `!cmd`) re-resolves on TTL and fails closed. Non-loopback bind refuses to start without a resolvable `auth_token`; WS auth uses single-use tickets (`POST /v1/ws_ticket`), never raw query tokens. Preserve these invariants.
- Doc comments explaining invariants/rationale are the norm — read surrounding docs before changing behavior.

## Important Files

- `src/backend/mod.rs` — `AgentClient`/`AgentSession` traits; read first for any backend work
- `src/backend/types.rs` — the normalized model (`StreamEvent`, `Capabilities`, `PermissionRequest`, …)
- `src/rpc.rs` — method dispatcher + `run_turn`; new RPC surface lands here
- `src/api.rs` — `AppState` wiring, routes; `src/config.rs` — config schema
- `src/bin/damond.rs` — daemon boot sequence
- `tests/common/mod.rs` — `mock_config()` + `MockClient`/`MockSession` harness
- `docs/protocol-v2.md` — wire protocol contract; `config.example.toml` — every option, commented
- `Cargo.toml` — MSRV 1.95 pinned by `rust-toolchain.toml`; CI enforces the two stay in lockstep

## Runtime/Tooling Preferences

- **Rust 1.95** (edition 2024), pinned via `rust-toolchain.toml` with rustfmt + clippy; bumping MSRV means updating `rust-toolchain.toml` in the same commit.
- **Node ≥ 22** for `npm/` (relies on global `WebSocket`; zero runtime deps; `postinstall` downloads release binaries — don't add build steps).
- **Python ≥ 3.11** for `python/` (uv-managed `uv.lock`; sole runtime dep `websockets`; dev extra via `pip install -e 'python[dev]'`).
- **Version bumping**: never hand-edit versions — run `scripts/set-version.sh <semver>`; it syncs Cargo.toml, npm/package.json, Formula/damon.rb, pyproject.toml, and `__init__.py`. Release CI verifies tag == all package versions.
- **Docs are EN/KO pairs** (`README.md`/`README.ko.md`, `docs/*.md`/`*.ko.md`): user-facing doc changes should update both sides.

## Testing & QA

- **Unit tests**: inline `#[cfg(test)] mod tests` at the bottom of each src module (36 files). `streamjson.rs` exposes a reusable recording `TestCtx` fixture for dialect tests.
- **Integration tests**: top-level `tests/` per subsystem (`rpc.rs` 57 fns, `channels.rs`, `store.rs`, `worktree.rs` with real git fixtures, `client.rs`, `cli.rs`, `api.rs`, `boot.rs`, `service.rs`, `relay.rs`). Style: real axum servers on `127.0.0.1:0`, `Store::in_memory()`, no network.
- **Mock backend**: `tests/common/mod.rs` provides `mock_config(auth_token)` and `MockClient`/`MockSession` (registered via `SessionManager::insert_client`). Prompt text is the control channel: contains `"perm"` → permission ask, `"question card"` → question card, `"hang"` → blocks until interrupted.
- **Cross-language contract**: client behavior (reconnect, auto-resume, ticket auth, relay E2E) is mirrored across `tests/client.rs` ↔ `npm/client.test.mjs` ↔ `python/tests/test_client.py` with cross-references in comments. Protocol/client changes must update all three.
- **Gates in CI**: fmt, clippy `-D warnings`, 3-OS test matrix, `cargo check --release`, coverage ≥ 60% lines, cargo-audit, cargo-deny (licenses allow-list in `deny.toml`; only crates.io sources).
