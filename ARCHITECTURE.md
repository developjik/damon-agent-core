# Damon 아키텍처

이 문서는 damon-core의 실제 구조를 코드 기준으로 기술한다. 모든 모듈명·타입명·
함수명은 `src/`의 실제 이름이며, 설계 판단의 배경은 [docs/adr/](docs/adr/)의
ADR에 커밋 해시와 함께 정리해 두었다.

> Crate: `damon-core` 0.8.0 — 단일 Rust 크레이트(workspace 없음), 라이브러리
> (`src/lib.rs`, flat `pub mod` 목록) + `src/bin/`의 얇은 바이너리 6개.
> MSRV 1.95 (`rust-toolchain.toml` pin).

## 전체 구조

```
[클라이언트 — 모두 damond의 thin client]
  damon CLI (src/bin/damon.rs)      내장 웹 UI (/ui, src/ui.rs)
  chat-desk 등 데스크톱 앱         npm / python 클라이언트 SDK
  damon-telegram · damon-discord · damon-slack 사이드카 (src/bin/)
  원격 클라이언트 — damon-relay 경유 E2E 터널 (src/relay.rs)
        │
        │  WebSocket GET /ws · JSON-RPC v2 (docs/protocol-v2.md)
        │  인증: Bearer auth_token 또는 1회용 ws_ticket (POST /v1/ws_ticket)
        ▼
┌─ damond (src/bin/damond.rs → src/service.rs 부트) ──────────────────┐
│                                                                    │
│  src/api.rs (axum)        src/rpc.rs (메서드 디스패치)              │
│  AppState·라우트·인증      114개 domain.verb 메서드                  │
│  티켓·rate limit·sweep     run_turn / drive_turn                    │
│        │                        │                                  │
│        │     ┌──────────────────┼───────────────────────────┐      │
│        │     │                  │                           │      │
│        ▼     ▼                  ▼                           ▼      │
│  src/session.rs         src/store.rs              src/memory/      │
│  SessionManager         SQLite + FTS5             메모리 원장       │
│  ManagedSession         sessions/messages/…       (게이트 체인)     │
│  EventJournal (cap 2048, 늦은 구독자 replay)                        │
│        │                                                           │
│        ▼                                                           │
│  src/backend/registry.rs — BACKENDS 카탈로그(9) → client_for()     │
│        │                                                           │
│        ├─ stream-json 계열: StreamJsonClient + StreamJsonDialect    │
│        │     claude (claude.rs) · qwen (qwen.rs)                    │
│        ├─ NDJSON app-server 계열: *Client over NdjsonTransport      │
│        │     codex · droid · zcode · omp · pi(pi는 OmpClient        │
│        │     ::with_profile(&omp::PI)로 공유)                       │
│        └─ HTTP+SSE 계열: OpencodeClient → OpencodeServer            │
│              opencode · mimo (serve 채택 또는 spawn)                │
│        │                                                           │
│        ▼                                                           │
│  에이전트 CLI 자식 프로세스 (각자의 native protocol)                │
│  claude · codex · omp · pi · qwen · droid · opencode · mimo · zcode │
│                                                                    │
│  [부가 서브시스템]                                                  │
│  channel.rs(Bridge/ChannelApi) → telegram/slack/discord.rs          │
│  relay.rs(E2E 터널) · bots.rs · mission.rs(DAG) · prompts.rs        │
│  skills_hub/ · worktree.rs · file_index.rs · history_scan.rs        │
│  mcp_inventory.rs · discovery.rs · logs.rs · ratelimit.rs           │
│  slash_catalog.rs · cli_lifecycle.rs · config.rs(hot-reload)        │
└────────────────────────────────────────────────────────────────────┘
```

한 턴의 데이터 흐름:

```
agent CLI subprocess (stream-json / NDJSON / HTTP+SSE)
  → backend/transport.rs 또는 backend/opencode_server.rs   (raw frame)
  → backend/<brand>.rs                                     (정규화 모델로 번역)
  → tokio broadcast<StreamEvent>                           (AgentSession::subscribe)
  → session.rs ManagedSession + EventJournal               (replay, cap 2048)
  → rpc.rs run_turn / drive_turn                           (session.event push)
  → WebSocket 클라이언트                                    (api.rs /ws 라우트)
```

## 모듈 맵

`src/lib.rs`의 `pub mod` 전수 기준. 대표 진입점은 코드에 실재하는
타입·함수명이다.

| 경로 | 책임 | 대표 진입점 |
|---|---|---|
| `src/api.rs` | axum HTTP/WS 계층: 라우트, WS 티켓, 인증 캐시, rate limit, 백그라운드 sweep | `AppState`, `ws_ticket`, `auth_token_cached`, `WsTicket` |
| `src/rpc.rs` | JSON-RPC v2 서버 — 최대 모듈(4,551줄), 메서드 디스패처, 턴 실행 | `ws_handler`, `handle_socket`, `run_turn`, `drive_turn`, `RpcError`, `error_code` |
| `src/session.rs` | Damon session id → live `AgentSession` 매핑, idle reap, 이벤트 저널 | `SessionManager`, `ManagedSession`, `EventJournal` (`JOURNAL_CAP = 2048`) |
| `src/store.rs` | SQLite + FTS5 영속 (sessions/messages/projects/usage/mission/memory 등) | `Store::open`, `Store::in_memory`, `Store::search_filtered`, `fts5_query` |
| `src/memory/mod.rs` | 영속 메모리 원장(user/bot 스코프), 쓰기 게이트 체인 | `validate_scope`, `ledger_cap`, `SCOPE_USER`, `SCOPE_BOT` |
| `src/memory/scan.rs` | 메모리 쓰기 게이트: prompt-injection·시크릿·보이지 않는 문자 탐지 | `scan` 모듈 (memory/mod.rs 게이트 1단계) |
| `src/backend/mod.rs` | 백엔드 추상화 2단 트레이트 + 네이티브 히스토리 discovery 헬퍼 | `AgentClient`, `AgentSession`, `peek_head_lines`, `is_noise_title` |
| `src/backend/types.rs` | 정규화 모델 — 데몬이 보는 유일한 백엔드 표현 | `StreamEvent`, `StreamEventKind`, `Capabilities`, `PermissionRequest`, `SessionConfig`, `PersistenceHandle` |
| `src/backend/registry.rs` | 정적 `BackendSpec` 카탈로그(9개 id), PATH 감지, config override 해석 | `BACKENDS`, `resolve_backends`, `client_for`, `which` |
| `src/backend/transport.rs` | 프로토콜 무관 stdio NDJSON 트랜스포트(스폰·라인 캡·stderr drain·pending map) | `NdjsonTransport::spawn`, `request_roundtrip` |
| `src/backend/streamjson.rs` | stream-json 방언 공용 엔진(claude 계열 프레임 파서) | `StreamJsonClient`, `StreamJsonSession`, `StreamJsonDialect`, `ClaudeFrameParser`, `ClaudeQuirks` |
| `src/backend/claude.rs` | Claude Code 방언 | `ClaudeDialect::dialect()` |
| `src/backend/qwen.rs` | Qwen Code 방언(claude형 control plane) | `QwenDialect::dialect()` |
| `src/backend/codex.rs` | Codex app-server (JSON-RPC over stdio) | `CodexClient` |
| `src/backend/droid.rs` | Droid (Factory, stream-jsonrpc) | `DroidClient` |
| `src/backend/zcode.rs` | ZCode app-server (Z.ai 프로토콜 v1) | `ZcodeClient` |
| `src/backend/omp.rs` | Oh My Pi rpc-mode + Pi 패밀리 프로파일 | `OmpClient`, `OmpClient::with_profile`, `PI` |
| `src/backend/opencode.rs` | OpenCode/MiMo HTTP+SSE 클라이언트 | `OpencodeClient::new(resolved, "opencode"|"mimo")` |
| `src/backend/opencode_server.rs` | `opencode serve` 수명주기 — probe→adopt→spawn | `OpencodeServer` |
| `src/channel.rs` | 채널 브릿지: 채널 채팅 ↔ damon 세션 매핑, 권한 프롬프트, 큐잉 | `Bridge`, `ChannelApi`, `Incoming`, `Attachment` |
| `src/telegram.rs` | Telegram 어댑터 (Bot API long-poll) | `ChannelApi` 구현 |
| `src/slack.rs` | Slack 어댑터 (Socket Mode + Web API) | `ChannelApi` 구현 |
| `src/discord.rs` | Discord 어댑터 (Gateway WS + REST, RESUME 재접속) | `ChannelApi` 구현 |
| `src/relay.rs` | 아웃바운드 E2E 터널: X25519 핸드셰이크 + AES-256-GCM 프레임 | `E2e`, register/dial 루프 |
| `src/client.rs` | 레퍼런스 WS 클라이언트(protocol v2) — 통합 예제 겸용 | `DamonClient`, `DamonClient::conn_state` |
| `src/config.rs` | 설정 스키마, `SecretRef`, notify 기반 hot-reload | `Config`, `SharedConfig`, `SecretRef`, `MemoryConfig`, `RelayConfig` |
| `src/discovery.rs` | `~/.damon/daemon.json` 데몬 발견 파일 | publish/읽기 헬퍼 |
| `src/service.rs` | OS 서비스 등록(launchd/systemd user/Task Scheduler) — 생성기는 순수 함수 | `Action` |
| `src/cli_lifecycle.rs` | 백엔드 CLI 버전 프로브·npm-latest 체크·업데이트 실행 | `RunOutcome`, `CommandRunner`, `RealRunner` |
| `src/bots.rs` | 봇 페르소나: `<data_dir>/bots/<id>/` 파일 기반 아이덴티티 | bot.json/SOUL.md 로더 |
| `src/mission.rs` | Mission DAG 러너 — 단계형 에이전트 워크플로(헤드리스 우선) | `MissionScheduler`, `MissionRegistry`, `MissionAgentRunner`, `MissionDefinition` |
| `src/prompts.rs` | 프롬프트 라이브러리(`<workspace>/.damon/prompts/`, `<data_dir>/prompts/`) | scope별 목록/CRUD |
| `src/skills_hub/` | 스킬 패키지 매니저: discovery·fetch·install lifecycle·target roots | `TargetRoot`, `InstalledSkill`, `HubError`, `ScanRoots` |
| `src/worktree.rs` | git worktree 워크스페이스 — "PR 하나당 격리 워크스페이스 하나" | `WorktreeInfo`, `CreateArgs`, `CreateOutcome`, `PrPreview` |
| `src/file_index.rs` | gitignore-aware 워크스페이스 파일 인덱스(`@path` 멘션용) | `ignore` walker 기반 인덱서 |
| `src/history_scan.rs` | 네이티브 세션 스캐너 — 발견된 외부 세션에 bookmark row 생성 | rescan 루프 (`session.rescan` 백엔드) |
| `src/mcp_inventory.rs` | 각 CLI의 네이티브 MCP 설정 인벤토리 + 연결 probe | `sources_for` |
| `src/slash_catalog.rs` | 컴포저 `/` 피커용 슬래시 커맨드 카탈로그 | 카탈로그 스캔 |
| `src/logs.rs` | 데몬 로그 링 + `logs.follow` 라이브 브로드캐스트 | 링 버퍼 + tee writer |
| `src/ratelimit.rs` | 채널 어댑터 공용 outbound HTTP 재전송(429 처리·버킷 페이싱) | send helper |
| `src/ui.rs` | 내장 단일-파일 웹 UI(`/ui`) + 잠금 헤더 | `ui`, `relay_client_js`, `UI_HEADERS` |
| `src/bin/damond.rs` | 데몬 바이너리 — 부트 시퀀스 | `main` |
| `src/bin/damon.rs` | CLI 클라이언트 바이너리 | `main` |
| `src/bin/damon-relay.rs` | 공용 릴레이 서버 바이너리 | `main` |
| `src/bin/damon-{telegram,discord,slack}.rs` | 채널 사이드카 바이너리 3종 | `main` |

## 등록 백엔드: 실물 기준 9개

`src/backend/registry.rs`의 `BACKENDS` 상수에 등록된 id는 **9개**다 —
`claude, codex, omp, pi, qwen, droid, opencode, mimo, zcode`
(`catalog_lists_supported_backends` 테스트가 `BACKENDS.len() == 9`와 정확한
id 목록을 단언한다). README의 "nine coding-agent CLIs" 주장과 일치한다.

다만 `src/backend/`의 per-brand 파일은 8개뿐이다. `pi`와 `mimo`는 전용
파일이 없이 패밀리 구현을 공유한다:

- `pi` → `client_for()`에서 `OmpClient::with_profile(resolved, &omp::PI)`
  (omp의 포크 부모라 같은 rpc wire — registry.rs 주석)
- `mimo` → `OpencodeClient::new(resolved, "mimo")` (opencode 포크로 같은
  `serve` 표면)

즉 **9개 등록 id → 7개 클라이언트 생성자 → 3개 프로토콜 패밀리**가 실물
구조다.

## 관련 문서

- [docs/protocol-v2.md](docs/protocol-v2.md) — wire 프로토콜 계약
- [docs/redesign.md](docs/redesign.md) — v2 리디자인(ACP → native 어댑터) 결정 기록
- [docs/features.ko.md](docs/features.ko.md) — 사용자 관점 기능 안내
- [docs/adr/](docs/adr/) — 설계 판단 기록(ADR)
