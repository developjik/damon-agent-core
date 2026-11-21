# ADR-0002: LLM 직결 대신 데몬 중앙형 — 모든 클라이언트를 thin client로

- Status: Accepted
- Date: 파운데이션 2026-09 (`06b507c`~`8afed02`), v2 리디자인 확정 `4a3f5c6` (redesign.md 기록일 2026-09-24)

## Context

히스토리에 두 번의 방향 전환이 있다:

1. **LLM provider 직결기**: `a291f2f`가 openai/anthropic/gemini/
   responses/compat/discovery/inband provider 어댑터를 넣었다 —
   데몬이 API 키를 쥐고 모델과 직접 말하는 설계.
2. **ACP기**: `33f7985` feat(rpc): ACP session/load — 표준 프로토콜로
   에이전트를 붙이는 시도.
3. **Native CLI 백엔드기(현재)**: `4a3f5c6` feat!: v2 redesign —
   native CLI backends replace ACP. `docs/redesign.md`가 기록을 남긴다:
   "catalog of 19 → 3", "one process per agent (ACP multiplexing) →
   one process per session".

세 방향 모두에서 변하지 않은 것이 **데몬 중앙 구조**다. `docs/redesign.md`
"What stays"가 그대로 열거한다: SQLite+FTS5 store, channel bridges,
relay tunnel, auth/rate limiting/metrics.

중앙화가 필요한 근본 이유는 피벗과 무관하게 유지됐다:

- **다중 서피스**: 웹 UI, CLI, telegram/discord/slack 사이드카,
  npm/python SDK가 같은 세션을 공유한다 (`eedbcca` channel adapters,
  `cf8e7ab` channel bridge binaries).
- **세션 영속 + replay**: `src/session.rs`의 `EventJournal`
  (cap 2048)이 늦게 붙은 구독자에게 이벤트를 재생하고,
  `902268f`(0.5.0)의 detached turn·cross-surface pickup — 한 표면에서
  시작한 턴을 다른 표면이 이어받는 기능 — 은 상태를 가진 중앙 데몬
  없이는 성립하지 않는다.
- **권한 릴레이**: 백엔드 프로세스의 `can_use_tool`류 요청을 임의의
  연결된 클라이언트로 push하고 `permission.respond`로 되돌리려면
  pending ask의 소유자가 중앙에 있어야 한다(`docs/protocol-v2.md`).

## Decision

`damond` 하나가 세션·저장·권한·채널·릴레이를 소유하고, CLI·웹 UI·
채널 봇·외부 SDK는 전부 같은 JSON-RPC-over-WebSocket 클라이언트로
둔다. 프로바이더 인증·모델·도구는 각 에이전트 CLI 소유로 돌린다
(데몬은 자격증명을 대신 쥐지 않는다 — registry.rs의 9개
`auth_hint`가 전부 "인증은 CLI 소유"를 가리키는 것이 코드상의
흔적. 문구는 제각각이다: "log in with `claude`/`codex` once; the
subscription follows"는 2개뿐이고, 나머지는 "configure `omp` once;
its provider auth follows", "log in with `droid` or set
FACTORY_API_KEY", "configure providers in opencode itself"처럼 각
CLI의 인증 경로를 안내한다. 공통인 것은 원칙뿐 — 데몬이 키를 보관·
대행하지 않고 각 CLI의 자체 인증을 따른다).

## Consequences

- 클라이언트는 상태 없이 붙었다 떨어져도 된다 — 저널 replay와
  `session.resume`이 복구한다.
- 데몬은 단일 장애점이다: 죽으면 모든 표면이 끊긴다. 그 대가로
  일관된 권한 게이트와 검색 가능한 단일 히스토리를 얻는다.
- provider 어댑터기의 유산이 코드에 남아 있다: 직결 설계를 치우면서
  데몬이 모델 API 키를 쥐는 경로가 사라졌고, `src/memory/mod.rs`의
  "post-session background review"가 "the daemon hold an API key"
  문제로 phase 2 보류 명시된 것이 그 흔적이다.

## 근거 (커밋·경로)

- `06b507c` feat: core foundations — config, llm stream types, oauth, service, sqlite store
- `a291f2f` feat: provider adapters — openai, anthropic, gemini, responses, compat, discovery, inband
- `4e15156` feat: daemon api — openai-compatible http, ws/json-rpc, mcp client, tool loop
- `8afed02` feat: damond daemon binary, relay tunnel, service install, example config
- `33f7985` feat(rpc): ACP session/load — replay history as session/update notifications
- `4a3f5c6` feat!: v2 redesign — native CLI backends replace ACP
- `eedbcca` feat: channel adapters — telegram, discord, slack bridges + e2e tests
- `902268f` feat: 0.5.0 — detached turns, cross-surface pickup, relay web UI, file API, projects, gemini
- `docs/redesign.md` (What changes / What stays 표), `src/session.rs` (`SessionManager`, `EventJournal`), `src/backend/registry.rs` (`auth_hint`)
