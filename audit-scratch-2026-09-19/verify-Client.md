# Phase 2 검증 — VerifyClient (클라이언트 경계 후보 5건) · 2026-09-19

기준: rerun-existing-digest.md 중복 배제 + 패치 도입 여부는 `git diff`(=미커밋 리미디에이션 전체, 40파일 +2956/−488 확인)로 판정. HEAD가 패치 전 상태.

## 판정 요약
| # | 후보 | 판정 | 심각도 |
|---|---|---|---|
| 1 | Rust client 4MiB 수신 캡 vs 데몬 무상한 응답 프레임 | CONFIRM | High |
| 2 | npm redial fresh-ticket 페치 무타임아웃 | REFUTE | — |
| 3 | npm #ready() 10s 타이머 유출 + 백오프 슬립 잔존 | REFUTE | — |
| 4 | npm 핸드셰이크 타임아웃 시 WebSocket 미폐쇄 | CONFIRM | Low |
| 5 | npm close()가 #connected 미정리 → 이후 호출 10s 지연 | CONFIRM | Low |

## 1. [ClientRelay] src/client.rs 4MiB 캡 — CONFIRM (High)
- 재검증 경로: `git diff`로 ws_connect_config(+connect_async_with_config 2곳: connect/내부 redial)가 **패치 신규** 확인(src/client.rs:104-131, 441-448). 펌프 `Err(_) => break`(client.rs:474-476) → 채널 폐쇄 → drain_pending이 진행 중 **모든** 요청/프롬프트 실패(client.rs:604-627) → Reconnect 재다이얼(client.rs:506-541) → request() 3회 재전송(client.rs:207-252).
- 데몬 측: 인바운드 4MiB 허용(rpc.rs:83-85) ↔ **아웃바운드 상한 부재** — respond()가 단일 JSON 프레임 직렬화(rpc.rs:419-441, 236-249), 아웃바운드 펌프 무상한(rpc.rs:96-105), SEND_TIMEOUT은 mpsc 전송 대기만. store.messages() LIMIT 없음 + 컴팩션 이후 행 무상한(store.rs:403-432). messages_paged도 limit=u32::MAX 허용.
- 시나리오 재구성 성공: 세션 누적 원본 ≥4MiB(단일 ~3.9MiB 프롬프트 1건+응답, 또는 장기 세션 — 툴 출력 기본 8KiB 절단이나 수백 회 누적/`max_tool_output` 설정 상향/어시스턴트 텍스트 무절단[collect cap 64MiB]) → Rust 클라이언트(pub mod client, lib.rs 공개 API)의 무페이지 `request("session/messages")` → 응답 프레임 >4MiB → tungstenite Capacity(MessageTooLong) → 링크 사망 → 재시도마다 동일 사망 3회 → "connection closed". 해당 세션 영구 조회 불가 + 매 시도 연결 공유 타 세션 진행 중 호출 동반 실패.
- 반증 시도: (a) 컴팩션이 반환 바이트를 묶는가 — cutoff 이후 행은 무상한, 반증 실패. (b) 데몬 쓰기 측에서 먼저 실패해 증상이 달라지는가 — 어느 쪽이 끊어도 동일(링크 사망+재전송 루프), 반증 실패. (c) CLI 노출 — bin/damon.rs에 session/messages 커맨드 없음(노출은 Rust 임베더 공개 API 경유; session/list 무페이지는 `damon sessions`로 도달하나 ~2만 세션 필요해 약함) — 심각도 High→Medium 논거였으나, 데몬이 자체 허용하는 입력 크기로 합법 세션이 영구 파손되는 경로가 코드로 완전 추적됨. (d) npm 클라이언트는 무영향(수신 캡 없음) — 범위를 Rust 클라이언트로 한정하는 요소일 뿐 반증 아님.
- 최종 심각도: **High** (합법적 입력으로 공개 API 경로 영구 파손 + 오진단 에러 + 연결 전체 동반 실패; 다만 npm/CLI 직접 노출 아님)

## 2. [NpmCiDeps] redial ticket 페치 무타임아웃 — REFUTE
- 재검증: `git diff` 및 `git show HEAD:npm/client.mjs` — `await ticketedUrl(...)`(redial)은 **HEAD에 이미 존재**(diff에서 context 라인). 패치는 같은 루프에 WS open 10s 게이트만 추가(페치 라인 불변). connect()의 ticketedUrl도 HEAD 코드.
- 판정: 패치 도입 아님(사전 존재). 더군다나 다이제스트 전례(install.js fetchText 잔존 무타임아웃 → '같은 파일+같은 증상 계열' 중복 폐기)와 동일하게, 기록된 "npm/client.mjs — redial open 무타임아웃"의 수정이 open 대기만 묶고 페치를 남긴 잔존.
- 기술 내용 검증: undici 기본 headersTimeout ~300s/connect 10s로 기본 설정 무한행 아님; 데몬이 회복하면 대기 중 페치가 그대로 완료되어 재접속 지연 실질 미미; 무한 정지는 timeout 없는 fetchImpl 주입 시만. 증상 자체도 사전 존재 코드의 것 → 폐기.

## 3. [NpmCiDeps] #ready() 타이머 유출 — REFUTE
- 재검증: `git show HEAD:npm/client.mjs` — `#ready()`(HEAD :177-181, setTimeout 미정리)와 redial 백오프 슬립(HEAD :134) 모두 **HEAD에 이미 존재**, 패치가 해당 함수 불변(diff 4개 헝크: redial 게이트/respond 가드/prompt·cancel·close 가드/ticketedUrl 파서).
- Node 의미론 자체는 보고자 주장대로(미-unref 타이머 이벤트루프 유지 → 자연 종료 최대 10s 지연) 타당하나, **패치 도입 결함이 아님**(Wave1의 '리미디에이션 신규 코드' 표기는 diff상 사실 아님). 기록 증상 아닌 사전 존재 결함이므로 본 패치 감사 범위 밖 → 폐기.

## 4. [NpmCiDeps] 핸드셰이크 타임아웃 소켓 유기 — CONFIRM (Low)
- 재검증: redial의 10s 게이트는 **패치 신규**(diff +137~159). 타임아웃 reject 시 `finally { clearTimeout(timer) }`뿐, CONNECTING 상태 ws를 close()하지 않음 → 소켓 유기. (connect()의 동일 패턴 :69-73은 HEAD 코드 — 범위 외, 참고만.)
- 시나리오 재구성 성공: 데몬 느린 응답/블랙홀 → redial 게이트 10s reject → ws 방치 → 백오프 후 신규 시도(구 소켓은 undici connectTimeout/OS 타임아웃까지 핸들 점유, 느린 open 성공 시 유휴 open 소켓 영구 방치). 반증: 소켓이 OS/undici 타임아웃으로 결국 정리되어 유한 → Low 유지(보고자 하향과 동일).

## 5. [NpmCiDeps] close() #connected 미정리 — CONFIRM (Low)
- 재검증: close() 본체는 **패치가 다시 씀**(diff :283-302 헝크, `#ws?.close()`+pending reject+`#push(null)`). 재작성이 재접속 창의 close를 안전하게 만들면서 #connected는 settle하지 않음(#onClose의 closed 분기도 치환 안 함, client.mjs:110-117).
- 시나리오 재구성 성공: 링크 단절 → 재접속 창(#ws=null, #connected pending)에서 close() → 이후 호출(#call/respond/cancel/prompt)이 #ready에서 settle되지 않은 #connected와 race → 10s 대기 후 "timed out waiting for reconnect"(기대: 즉시 "connection closed"). 링크 UP 상태 close() 경로는 #connected가 이미 resolved라 즉시 실패(단 #call/prompt는 null send TypeError) — 증상은 재접속 창 한정.
- 패치 도입 여부: 패치 전 동일 상황은 close()의 `this.#ws.close()`가 null 역참조 TypeError로 즉시 크래시(패치가 고친 대상) — hang 경로 자체가 새 close() 본체가 만든 것 → 패치 도입. Low(종료 경로 10s 지연+오진단 에러, 손상 없음).

## 집계
- 승인: 후보 1(High), 후보 4(Low), 후보 5(Low)
- 폐기: 후보 2(사전 존재 HEAD 코드 + 기록 증상계열 잔존 → 중복/범위밖), 후보 3(사전 존재 HEAD 코드 — 패치 미변경)
