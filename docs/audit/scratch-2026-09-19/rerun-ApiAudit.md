# 재감사: src/api.rs (ApiAudit, 2026-09-19)

대상: 리미디에이션 패치 후 src/api.rs (+54/−31, 미커밋). 신규 결함만 후보.
중복 배제: `rerun-existing-digest.md` 사전 로드 — api.rs 기존 4건(오탐 promotion, >1MB/타임아웃 빈 본문, 에러문자열 URL 노출[Low], u64 재직렬화[NO-FIX 문서화])은 이번 패치로 수정된 것들이며, 동일 증상 재보고 없음.

## 결론: 결함 0건

## 패치 변경점 검토 (7개 hunk 전수)

1. **forward verbatim arm 주석 추가** (api.rs:447-455) — 로직 불변, u64 정밀도 NO-FIX 문서화. 이상 없음.
2. **forward Err → warn! + 고정 메시지** (api.rs:483-487) — openai.rs:126/151이 `.context("upstream request failed")`로 래핑 → anyhow Display에는 URL 미포함(로그 포함 무유출). 클라이언트 502 고정문. 이상 없음.
3. **promote_or_error 버퍼 판정 재구성** (api.rs:501-528) —
   - `Ok(Err(e))` arm이 `format!("upstream error body read failed: {e}")`를 **클라이언트 응답 본문**에 넣지만, 청크 에러는 reqwest 0.12.28 `bytes_stream` → `error::decode` (Kind::Decode, `with_url` 미부착) → Display에 URL 없음 검증(벤더 소스 확인). collect_stream 64MiB 캡 에러도 고정문. 공개 무해.
   - `&buf[..64*1024]` 슬라이스: len>1MiB 보장으로 바운드 안전, UTF-8 분할은 from_utf8_lossy가 U+FFFD 처리 — 패닉 없음.
   - 1MiB 가드 제거로 인한 무버퍼링 우려: collect_stream이 COLLECT_STREAM_MAX=64MiB로 상한(1차 수정). 구 코드도 len 체크 **이후** 전체 버퍼링이므로 할당 상한 동일 — 신규 노출 없음.
   - 타임아웃/실패 arm이 원인 구분 메시지 반환(구: 빈 본문) — 개선.
4. **is_context_overflow 축소** (api.rs:570-585) — "too many tokens"/"request too large" 제거(오탐 방지, 의도·주석 명시). 미탐 방향 검증: Anthropic "prompt is too long" 유지✓, OpenAI/vLLM "maximum context length"→"maximum context"/"context length" 유지✓, Gemini "exceeds the maximum number of tokens"는 **패치 전에도** 5+2 시그니처 어디에도 불일치(선행 결함, 비신규). 회귀테스트(tests/providers.rs:1103) 본문은 "context_length_exceeded" 포함로 계속 일치. 미탐 시 원본 에러 그대로 전달(우아한 강등) — 결함 아님.
5. **SSE mid-stream 에러 고정 메시지** (api.rs:679-692) — 청킹 프레이밍/`[DONE]`/usage 이벤트/ThinkingBlock 필터 불변, 메시지 문구만 변경. 이상 없음.
6. **translate_forward Err 2곳 고정 메시지** (api.rs:721-730, 749-758) — RateLimited 429+Retry-After 경로·depth==0 promotion 게이트 순서 불변. 이상 없음.
7. **promotion 재시도 경로** — passthrough→translate depth=1, translate→translate depth=1, 직접 forward(비재귀) 구조 유지, 순환 캡 정상. `_thinking`/model rewrite 보존. 이상 없음.

## 회귀 점검 (기존 '결함 없음' 인증 영역)
- require_token/auth_token_cached/ws_ticket/rate_limit/bucket_allow/router 미들웨어 순서: **diff 미포함 영역** — 전문 대조 결과 훼손 없음.

## 반증해서 버린 후보
- (a) `Ok(Err(e))` 클라이언트 노출 URL 누출 의심 → reqwest 소스 검증으로 Decode 에러는 URL 미부착, 반증.
- (b) warn! 로그 시크릿 노출 의심 → 3 프로바이더 모두 헤더 인증(Bearer/x-api-key/x-goog-api-key), URL에 키 없음 + openai.rs context 래핑, 반증.
- (c) 64KiB 절단 UTF-8 패닉/시그니처 파괴 → from_utf8_lossy 안전, 경계 일치 확률 무시 가능, 반증.
- (d) 1MiB 가드 제거 = 무제한 할당 → collect_stream 64MiB 캡이 실질 상한(구 코드도 전체 버퍼링 후 폐기), 반증.
- (e) Gemini 컨텍스트 초과 미탐 → 패치 전에도 불일치(선행 동작, 신규 아님), 제외.
- (f) 고정 메시지가 application/json 헤더와 비JSON 본문 → 구 동작(빈 본문)과 동등, 코스메틱, 반증.
- (g) >1MiB 에러 본문 64KiB echo가 잘린 JSON → 의도된 개선(구: 폐기), 클라이언트는 상태코드 수신, 반증.

## 커버리지
- src/api.rs 1-916 전문 (4분할 정독)
- git diff src/api.rs 전체 (7 hunk, +54/−31 확인)
- src/provider/mod.rs:224-268 (collect_stream/COLLECT_STREAM_MAX)
- src/provider/openai.rs:99-178 (forward 에러 래핑, strict-retry)
- src/provider/gemini.rs (인증 헤더/URL 구성), src/provider/anthropic.rs (bail! 에러 Display)
- reqwest-0.12.28 벤더 소스: async_impl/response.rs bytes_stream, error.rs Display/decode
- tests/providers.rs:1086-1233 (promotion 회귀테스트)
- audit-scratch-2026-09-19/rerun-existing-digest.md (중복 배제)
