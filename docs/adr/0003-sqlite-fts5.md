# ADR-0003: 영속 계층은 SQLite + FTS5 (bundled)

- Status: Accepted
- Date: 파운데이션 `06b507c` (2026-09) — 첫 커밋부터 sqlite store; v2 리디자인 `4a3f5c6`에서도 유지 목록에 명시

## Context

데몬은 로컬 퍼스트다: 사용자 머신에서 상주하며 세션 트랜스크립트,
프로젝트, usage, 채널 상태, mission run, 메모리 원장을 쌓는다.
요구 조건:

- 별도 서버 프로세스·네트워크 서비스 없이 설치 즉시 동작
- 트랜스크립트 전문 검색 — 한국어·CJK 포함
- 단일 프로세스(데몬)만 쓰는 워크로드

외부 DB(postgres 등)는 설치·운영 의존을 만들고, 파일 append만으로는
검색·필터·집계가 안 된다.

## Decision

`rusqlite`(bundled feature) + `tokio-rusqlite`로 SQLite를 정적
포함하고, `src/store.rs`의 `Store`가 전부를 소유한다. 전문 검색은
FTS5 `messages_fts` 가상 테이블로, **trigram tokenizer**를 쓴다:

- `tokenize = 'trigram'` — 3글자 윈도우 인덱스라 MATCH가 substring
  검색이 된다(`store.rs` 스키마 주석). unicode61 기본 토크나이저는
  CJK 한 덩어리를 하나의 토큰으로 접어 부분 검색이 깨지므로 trigram을
  택했고, 3글자 미만 짧은 용어는 `fts5_query`가 LIKE 경로로
  우회한다(`store.rs` 주석·테스트
  `fts5_query_splits_short_terms_onto_the_like_path`).
- 스키마 마이그레이션은 `Store` 내부 버전 체인 — trigram 재토큰화도
  v4 마이그레이션으로 처리됐다(`store.rs` 880행대).

## Consequences

- 설치물이 단일 바이너리를 유지한다(bundled sqlite는 소스에서
  컴파일 — 빌드 시간을 먹는 대가).
- 쓰기는 사실상 직렬화된다: `tokio-rusqlite`의 단일 커넥션 스레드가
  큐잉한다. 데몬 워크로드(세션 로그 append + 검색 read)에는 충분.
- **의도적 배제**: 메모리 원장은 `memory_entries` 자체 테이블에
  두고 FTS 인덱스에서 뺀다 — "conversation search must never
  surface memory contents"(`src/memory/mod.rs` 헤더 주석).
- `docs/redesign.md` "What stays" 첫 항목이 "SQLite + FTS5 session
  store, search" — ACP→native 대개편에서도 흔들리지 않은 축이다.

## 근거 (커밋·경로)

- `06b507c` feat: core foundations — config, llm stream types, oauth, service, sqlite store
- `7d71948` feat(store): last_active_at retention, transactions, FTS cleanup
- `4a3f5c6` feat!: v2 redesign — native CLI backends replace ACP (유지 결정)
- `Cargo.toml` — `rusqlite = { version = "0.40.2", features = ["bundled"] }`, `tokio-rusqlite = "0.8.0"`
- `src/store.rs` — `CREATE VIRTUAL TABLE … messages_fts USING fts5(… tokenize='trigram')`, `fts5_query`, `Store::open`/`in_memory`
- `src/memory/mod.rs` — 원장이 FTS5 바깥에 있는 이유 명시
