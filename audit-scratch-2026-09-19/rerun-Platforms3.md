# 재감사 — Platforms3 (telegram.rs / discord.rs / slack.rs 리미디에이션 회귀)

날짜: 2026-09-19 · 대상: 미커밋 패치의 3플랫폼 채널 수정 (+251/−26) · 읽기 전용

## 결론: 결함 0건 (신규 결함 후보 없음)

---

## 1. 검증 상세 (집중 영역별)

### (1) telegram offset 지연확정 (PendingUpdate + take_confirmed)
- **커밋 지점**: `take_confirmed`가 pop하며 `offset.fetch_max(id+1)` → 반환 시점이 커밋. 서버 확정은 다음 getUpdates 요청의 offset 파라미터로만 전달되므로, 크래시 시 fetch_max된 in-memory offset은 소실되고 서버는 미확정 윈도우만 재전달 — at-least-once 성립(코드 주석의 설계 문서와 일치).
- **마커(edits/reactions)**: `PendingUpdate{msg:None}`로 순서 보존, 다음 메시지 스캔 중 pop되며 확정. "확정이 미전달 메시지를 앞질러 점프" 불가 확인.
- **폴링 불변식**: `recv`는 take_confirmed가 None(=pending 완전 소진)일 때만 폴링 → 폴링 offset은 항상 모든 pop된 항목을 커버. 폴링 루프 내 `id < offset` dedup 체크는 폴링 직전 로드값과 동일(폴링 await 중 offset 변이 없음 — recv 단일 소비자, channel.rs:116-130 단일 루프 확인).
- **중복 상한**: 프로세스 내 dedup + 크래시 시 재시작 offset=0 → 서버 미확정분만(기본 limit 100/폴링, 24h 보존) 재전달 — 유계. 재전달 메시지 재처리(중복 에이전트 턴)는 주석에 명시된 at-least-once 설계 선택.
- **롤백 불필요**: fetch_max 단조 증가만 존재, 확정은 인계 시점만 발생.

### (2) discord 백오프 (next_backoff/reset_backoff)
- 배가 5→10→…→300 캡, READY(op0,t=READY) dispatch 시 5s 리셋 — READY는 매 신규 identify 수락마다 도착하므로 리셋 조건 정확.
- op9 반복: 매회 next_backoff 호출 → 캡 300s에서 시간당 ~12 identify로 제한(쿼터 보호 목적 달성).
- op7은 고정 5s 유지(백오프 무관) — 서버 요청 정상 재접속에는 배가 불필요한 설계와 일치.
- connect 실패 경로도 동일 백오프 공유 — READY 전 실패는 리셋 안 됨(올바름: identify 수락 전이므로).
- resume은 v1에서 미구현(모듈 문서 명시) — resume 재사용 조건 자체가 존재하지 않아 검사 대상 없음.
- 락: backoff Mutex는 lock 후 await 없이 즉시 해제 — lock-across-await 없음.

### (3) slack SeenEnvelopes FIFO 축출
- cap에서 신규 id 삽입 시 최신만 축출: `set.insert` 성공(신규) 시에만 pop_front+set.remove → set/order 동기 유지, len ≤ cap 불변식 확인(4096 초과 불가).
- 기존 전체 clear 대비 최신 1개만 망각 — 엄격한 개선, 회귀 없음. ack가 dedup 체크보다 먼저 모든 envelope(중복 포함)에 전송 — Socket Mode 프로토콜 요구와 일치.
- 메모리: id가 set+order 이중 보관(~2×)이나 4096 상환 유계 — 결함 아님.

### (4) 429/Retry-After · 타임아웃 · UTF-8 분할
- 3파일의 send/post 429 재시도 및 floor_char_boundary 분할 코드는 본 패치 diff에 미포함(HEAD 선행 코드) — 패치가 건드리지 않은 영역, 회귀 불가능 확인.
- 신규 코드는 HTTP 호출·문자열 분할·타임아웃 없음(백오프 sleep, VecDeque, Atomic 조작뿐).

### (5) 경계/테스트 품질
- 신규 타입(PendingUpdate/SeenEnvelopes/backoff)은 전부 파일 비공개 — 모듈 경계 횡단 없음, 소비자(channel.rs)는 불변 ChannelApi 트레이트만 관측.
- 신규 테스트 5개 모두 실행위 관측(모킹된 폴링 offset 기록, 백오프 수열, 축출 순서) — 동어반복/거짓통과 없음.
- 실행 증거: `cargo test --lib telegram::tests` 3 passed / `discord::tests` 1 passed / `slack::tests` 1 passed.

## 2. 커버리지 (실제 읽은 범위)
- src/telegram.rs 전체(1-365), src/discord.rs 전체(1-409), src/slack.rs 전체(1-354) — raw 정독
- git diff 3파일 전문(artifact://44,45 포함 전체 hunk)
- src/channel.rs:116-130 (recv 소비 계약 — 단일 루프, Ok(None)→5s sleep, Err 처리)
- HEAD 버전 대조: git show HEAD:src/telegram.rs (구 recv 구조 확인 — 신규/선행 구분)
- Cargo.toml (parking_lot 의존성 — 신규 테스트 컴파일 요건 확인)

## 3. 조사 후 반증/폐기한 후보
1. **discord: READY 도달 전 close/heartbeat-timeout/read-error 경로 재접속 무지연 루프**(identify 쿼터 소진 가능) — 패치 미변경 경로(구 코드 동일), 선행 결함. 기록된 기존 finding은 op7/9 고정 5s로 증상 상이하나 어느 쪽이든 본 패치가 도입한 것이 아님 → 신규 아님.
2. **discord: 재접속 후 신규 세션에도 seq 미리셋**(낡은 seq로 heartbeat) — seq 로직은 패치 미변경, 선행.
3. **telegram: 크래시 재시작 후 미확정 메시지 재처리 = 중복 에이전트 턴, offset/미영속화** — 코드 주석에 명시적 at-least-once 설계 선택(의도치 않은 결함 아님). 영속화는 선택된 리미디에이션 방식이 아님.
4. **telegram: update_id 없는 말폼 업데이트가 매 폴링 재전달**("delivered once" 주석과 상이) — 실 Telegram은 update_id 필수인 가설적 경계 + 구 코드 동일 동작, 패치 도입 아님.
5. **telegram: 단일 배치 내 중복 id가 `id < offset` dedup 우회** — Telegram 보증(배치 내 update_id 단조 증가) 위반 시에만 발생, 방어 코드 부재가 아니라 계약상 불가능.
6. **slack: SeenEnvelopes 이중 보관 메모리 2×** — 유계(4096), 성능 열화 미미.
7. **discord/slack connect() 내 hello read 무타임아웃** — 패치 미변경 선행 코드.
