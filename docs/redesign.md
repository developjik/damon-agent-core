# Damon v2 재설계 — 네이티브 프로토콜 어댑터

> 상태: **구현 완료** (2026-09-24). ACP 제거, 에이전트 3종(Claude Code /
> Codex / Oh My Pi)을 각자의 네이티브 프로토콜로 직접 구동. 클라이언트
> API v2 확정 — `docs/protocol-v2.md` 참조.

## 1. 목표와 범위

**바뀌는 것**

- 에이전트 연동: ACP v1 → 에이전트별 네이티브 stdio 프로토콜
- 지원 에이전트: 카탈로그 19종 → 3종 (claude, codex, omp)
- 프로세스 모델: 에이전트당 프로세스 1개(ACP 멀티플렉스) → 세션당 프로세스 1개
- 클라이언트 API: ACP-shaped JSON-RPC → Damon 자체 프로토콜

**유지하는 것**

- SQLite + FTS5 세션 저장소, 검색
- 채널 브릿지 (Telegram/Discord/Slack)
- 릴레이 터널 (X25519 + AES-256-GCM)
- 인증/레이트리밋/메트릭스/SSE 팬아웃
- 배포 채널 (npm, crates.io, Homebrew, 서비스 등록)

## 2. 에이전트별 네이티브 프로토콜

| | Claude Code | Codex | Oh My Pi |
|---|---|---|---|
| 실행 | `claude -p --output-format stream-json --input-format stream-json --verbose` | `codex app-server` | `omp --mode rpc` |
| 와이어 | stdio NDJSON (비-RPC 프레임) | stdio NDJSON, JSON-RPC 2.0 | stdio NDJSON, typed frames |
| 핸드셰이크 | 첫 `system.init` 프레임 (session_id 획득) | `initialize` | `ready` 프레임 → `negotiate_protocol` v2 |
| 세션 재개 | `--resume <session_id>` 재스폰 | `thread/resume` | `switch_session` |
| 프롬프트 | stdin user 메시지 | `turn/start` | `prompt` 커맨드 |
| 권한 | `control_request.can_use_tool` → `control_response` | approval 요청/응답 | `extension_ui_request` → `extension_ui_response` |
| 중단 | `control_request.interrupt` | `turn/interrupt` | `abort` |
| 히스토리 | 세션 파일 재생 | thread 히스토리 | `get_messages_page` |
| 네이티브 세션 파일 | `~/.claude/projects/{cwd}/*.jsonl` | `~/.codex/sessions/**/rollout-*.jsonl` | 세션 파일 (jsonl) |

## 3. 새 아키텍처

```
[clients] ──WS──▶ damond
                    │  SessionManager: damon_session_id → Box<dyn AgentSession>
                    │
              AgentClient (브랜드 담당)          AgentSession (대화 1개)
              - is_available                    - start_turn / steer
              - fetch_catalog (models+modes)    - subscribe → StreamEvent
              - create_session                  - respond_to_permission
              - resume_session                  - interrupt / close
              - list_importable_sessions        - history / persistence_handle
                    │
        ┌───────────┼───────────┐
   ClaudeBackend CodexBackend OmpBackend
        └───────────┴───────────┘
              NdjsonTransport (공용)
         spawn / send / subscribe / stderr ring / exit
```

### 정규화 모델 (Paseo 차용)

- `StreamEvent { turn_id, kind }` — ThreadStarted, TurnStarted/Completed/Failed/Canceled,
  UsageUpdated, ModeChanged, ModelChanged, Timeline(item), PermissionRequested/Resolved,
  AttentionRequired
- `TimelineItem` — UserMessage, AssistantMessage, Reasoning, ToolCall, Todo, Error,
  Compaction, **Unknown{raw}** (번역 실패 시 원문 보존)
- `ToolCallDetail` — Shell/Read/Edit/Write/Search/Fetch/SubAgent/Plan/Unknown
- `PermissionRequest` — kind(tool/plan/question/mode/other) + actions[](에이전트 제공 버튼)
  + suggestions(항상 허용 계열); `PermissionResult.follow_up_prompt`로 plan 승인 후속 턴 표현
- `Capabilities` — streaming, session_persistence, session_listing, dynamic_modes,
  mcp_servers, reasoning_stream, steer, rewind, subagent_events
- `PersistenceHandle { provider, native_handle, metadata }` — provider 세션 파일이 진본,
  Damon DB는 책갈피+검색 색인

## 4. 파일별 작업 계획

### 삭제

| 파일 | 사유 |
|---|---|
| `src/acp.rs` | ACP 호스트 전체 폐기 → `backend/transport.rs` + 백엔드로 대체 |
| `src/agents.rs` | ACP 카탈로그 19종 폐기 → `backend/registry.rs` (3종) |
| `src/bin/damon-acp-mock.rs` | ACP mock → `src/bin/damon-mock.rs` (네이티브 mock)로 교체 |
| `tests/rpc.rs` | ACP-shaped API 테스트 → 새 API로 재작성 |
| `tests/channels.rs` | 새 이벤트 모델로 재작성 |
| `tests/client.rs`, `tests/relay.rs`, `tests/service.rs`, `tests/api.rs`, `tests/boot.rs`, `tests/cli.rs` | 새 API/백엔드 기준 재작성 (store.rs 테스트는 대부분 유지) |
| `docs/audit/` | 구 설계 감사 기록 — 재설계 후 무의미. 삭제 또는 `docs/audit-v1/`로 이동 |
| `.gjc/` | 외부 도구 산출물 — gitignore 추가 후 제거 |

### 재작성

| 파일 | 변경 |
|---|---|
| `src/rpc.rs` | 디스패치를 새 API로 전면 재작성. 세션/권한 라우팅은 SessionManager로 이동 |
| `src/client.rs` | 새 와이어 프로토콜용 레퍼런스 클라이언트 |
| `src/ui.html` | 새 이벤트 모델/메서드로 재작성 (UI 골격 재사용) |
| `src/config.rs` | `[agents.X]` → `[backends.X]` (command/args/env 오버라이드만). `mcp_servers` 유지 |
| `src/channel.rs` | `ClientEvent` → `StreamEvent` 소비로 변경, permission 카드는 actions[] 렌더 |
| `src/bin/damon.rs` | CLI를 새 API로 (chat/search/backup 유지) |
| `npm/client.mjs` + `client.test.mjs` | 새 프로토콜 클라이언트 |
| `python/src/` + tests | 새 프로토콜 클라이언트 |
| `README.md` / `README.ko.md` | ACP 언급 제거, 3 에이전트, 새 아키텍처 |
| `docs/integration.md` (+ko) | 새 와이어 프로토콜 문서 |
| `config.example.toml` | 새 설정 스키마 |
| `CHANGELOG.md` | v0.3.0 (breaking) 엔트리 |

### 유지 (수정 최소)

| 파일 | 변경 |
| `src/store.rs` | 무수정 — 기존 `agent`/`agent_session` 컬럼이 provider/native_handle 역할을 그대로 수행 (마이그레이션 불필요로 확정) |
| `src/api.rs` | 라우터/인증/레이트리밋/메트릭스 유지. `acp_sessions`/`permission_routes` 맵 제거 |
| `src/relay.rs` | 무수정 (전송층은 프로토콜 무관) |
| `src/telegram.rs` / `discord.rs` / `slack.rs` | 무수정 (ChannelApi 경계 유지) |
| `src/service.rs` / `discovery.rs` / `ui.rs` | 무수정 |
| `src/bin/damond.rs` | doctor를 3 백엔드 감지로 수정 |
| `src/bin/damon-{telegram,discord,slack}.rs` | 무수정 |
| `src/bin/damon-relay.rs` | 무수정 |
| `Formula/damon.rb`, `npm/install.js`, `scripts/` | 무수정 |

### 신규

```
src/backend/
  mod.rs        — trait + re-export
  types.rs      — StreamEvent, TimelineItem, ToolCallDetail, Permission*, Usage, Capabilities
  transport.rs  — NdjsonTransport (acp.rs의 프로세스/라인 관리 일반화)
  registry.rs   — 3개 백엔드 등록, PATH 감지, 설정 오버라이드
  claude.rs     — stream-json 백엔드
  codex.rs      — app-server 백엔드
  omp.rs        — omp --mode rpc 백엔드
src/session.rs  — SessionManager (세션 id → AgentSession, 권한 라우팅, 유휴 회수)
tests/common/mod.rs — 인프로세스 mock 백엔드 (damon-mock 바이너리 대신 확정)
```

## 5. 클라이언트 API 재설계 방향

ACP 용어에서 탈피. WS 단일 연결, JSON 텍스트 프레임.

- 핸드셰이크: `hello { client, protocolVersion, capabilities }` → `server_info { version, backends[], capabilities }`
  - 스키마 append-only, 새 이벤트/필드는 capability 게이트 (Paseo 규칙 차용)
- 요청/응답: `{ id, type: "request", method, params }` → `{ id, type: "response", result|error }`
- 이벤트: `{ type: "event", sessionId, turnId?, event: StreamEventKind }` — 정규화 모델 그대로 노출
- 메서드(초안): `session.create/resume/list/messages/search/export/fork/delete/rename/usage`,
  `turn.start/steer/cancel`, `permission.respond`, `backend.list/status`
- 인증: 기존 bearer + ws_ticket 유지

## 6. 마이그레이션 순서

1. **기반**: `backend/types.rs` + `transport.rs` + `registry.rs` + `session.rs`
   (acp.rs와 공존 — 아직 기존 경로 유지)
2. **Claude 백엔드** — stream-json 구현 + mock 대신 실제 CLI로 수동 검증
3. **Codex 백엔드** — app-server
4. **OMP 백엔드** — rpc mode
5. **데몬 통합** — rpc.rs 재작성, store 마이그레이션, 새 WS API
6. **클라이언트** — client.rs, damon CLI, ui.html, channel.rs, npm, python
7. **정리** — acp.rs/agents.rs/mock/구 테스트 삭제, 문서 재작성, CHANGELOG

각 단계는 컴파일 가능 상태 유지. 5단계에서 ACP 경로 제거(clean cutover).

## 7. 미해결 질문

1. Claude stream-json의 control_request 전체 목록 — 공식 문서가 SDK 뒤에 있어 실측 필요
   (`claude -p --input-format stream-json`으로 init 프레임과 control 흐름 관찰)
2. Codex app-server 정확한 메서드/승인 페이로드 — `codex-rs/app-server` README 기준 검증
3. OMP `switch_session`의 인자(세션 파일 경로)와 resume 의미론
4. 유휴 세션 프로세스 kill 시 in-flight 권한 요청 처리
5. 세션당 프로세스 모델의 리소스 상한 — 동시 세션 수 제한 필요 여부
