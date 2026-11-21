# ADR-0004: 백엔드는 공통 표준이 아니라 per-agent 네이티브 어댑터

- Status: Accepted
- Date: `4a3f5c6` (v2 redesign, redesign.md 기록일 2026-09-24) — `6a24481`(0.4.0 방언 공용화), `8330edd`(카탈로그 정련)으로 확장

## Context

이전 시도(ACP)는 "모든 에이전트가 하나의 표준 프로토콜을 말한다"를
전제했다. 실패 양상이 `docs/redesign.md`에 기록돼 있다: 카탈로그
19개 중 실제로 완전히 구동 가능한 건 3개 — bidirectional 스트리밍,
mid-turn 권한 요청, interrupt까지 지원하는 CLI만 카탈로그에 남는다
라는 기준으로 정련됐다(`8330edd` "the catalog only admits
fully-drivable CLIs"; `src/backend/registry.rs` 헤더 주석: "no
generic 'any binary speaks the protocol' fallback … the registry is a
fixed set, not an open catalog").

각 CLI의 wire가 실제로 다르다(redesign.md §2 표):

- claude/qwen: stdio stream-json, claude형 control plane
- codex/droid/zcode/omp/pi: stdio NDJSON이지만 서로 다른 RPC/프레임
  방언 (JSON-RPC 2.0, stream-jsonrpc, typed frames, Z.ai 커스텀)
- opencode/mimo: stdio가 아니라 HTTP+SSE (`serve` 채택 또는 spawn)

## Decision

표준 프로토콜 전제를 버리고, 각 백엔드가 **자기 네이티브 프로토콜**
그대로를 말하게 한 뒤 데몬 안에서 정규화한다:

1. **2단 트레이트**(`src/backend/mod.rs`, "borrowed from Paseo"):
   - `AgentClient` — 브랜드 수준: `is_available`, `fetch_catalog`,
     `create_session`, `resume_session`, `list_importable_sessions`
   - `AgentSession` — 대화 하나: `subscribe`, `start_turn`, `steer`,
     `interrupt`, `respond_to_permission`, `persistence_handle`
2. **정규화 모델**(`src/backend/types.rs`): `StreamEvent`,
   `Capabilities`, `PermissionRequest`, `SessionConfig` —
   `pub use types::*`로 데몬 전체가 이 모델만 본다.
3. **3개 프로토콜 패밀리의 공용 기계**:
   - `streamjson.rs` — `StreamJsonClient`/`StreamJsonSession` +
     `StreamJsonDialect` 트레이트; 공통 번역은 `ClaudeFrameParser`
     (`ClaudeQuirks`로 차이를 데이터화). claude·qwen이 탄다.
   - `transport.rs` — `NdjsonTransport`(spawn·라인 캡·stderr drain·
     pending map) 위에 codex·droid·zcode·omp(+pi)가 얹힌다.
   - `opencode_server.rs` + `opencode.rs` — `serve` probe→adopt→
     spawn 생명주기와 HTTP+SSE 세션. opencode·mimo가 탄다.
4. **고정 카탈로그 + typed NOT_SUPPORTED**: `BACKENDS`는 정적
   `BackendSpec` 목록; config은 launch line만 override할 수 있고
   프로토콜을 추가하지는 못한다. 미지원 기능(모델/모드 전환, steer)은
   기본 구현이 `-32002` typed error를 돌려 클라이언트가 affordance를
   숨기게 한다(`AgentSession::set_mode`/`set_model` 기본 구현,
   `docs/protocol-v2.md` 오류표).

## Consequences

- 신규 백엔드 추가 비용이 패밀리에 의해 감쇠한다는 것이 실물로
  증명됐다: `pi`는 파일 하나 없이 `OmpClient::with_profile(&omp::PI)`
  프로파일로 등록됐고, `mimo`는 `OpencodeClient` 재사용이다
  (`registry.rs` `client_for`). 새 패밀리는 새 파일 + trait 구현
  + 카탈로그 한 줄.
- 각 CLI는 자기 인증·도구·모델을 계속 소유한다 — 데몬은 프로토콜
  번역기일 뿐 credentials를 대행하지 않는다(ADR-0002와 짝).
- 비용: 이기종 9개 wire의 변역 테이블을 코드가 안고 간다 —
  `ClaudeQuirks`·각 backend 파일의 주석("verified live against
  X.Y.Z")이 그 유지 비용의 흔적이다. live 검증이 어려운 백엔드는
  카탈로그에서 빠진다.

## 근거 (커밋·경로)

- `4a3f5c6` feat!: v2 redesign — native CLI backends replace ACP
- `6a24481` feat!: 0.4.0 — cursor/amp/kimi/qwen via shared stream-json dialects, dead-code sweep, release prep
- `8330edd` feat(backends): the catalog only admits fully-drivable CLIs
- `docs/redesign.md` — §2 네이티브 프로토콜 비교표, §3 신구조도
- `src/backend/mod.rs` — `AgentClient`/`AgentSession` 트레이트
- `src/backend/types.rs` — 정규화 모델
- `src/backend/registry.rs` — `BACKENDS`, `client_for`(pi/mimo 공유 매핑), `catalog_lists_supported_backends` 테스트
- `src/backend/streamjson.rs` — `StreamJsonDialect`, `ClaudeFrameParser`/`ClaudeQuirks`
- `src/backend/transport.rs` — `NdjsonTransport`
- `src/backend/opencode_server.rs` — `OpencodeServer` 생명주기
