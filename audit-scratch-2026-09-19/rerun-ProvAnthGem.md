# 재감사 보고 — ProvAnthGem (src/provider/anthropic.rs, src/provider/gemini.rs)

날짜: 2026-09-19 · 대상: 리미디에이션 패치(미커밋) 중 anthropic.rs(+191/−35), gemini.rs(+196/−46) · 읽기 전용

## 결론: 결함 0건 (신규 도입 결함 없음)

집중 항목별 검증 결과:

### (1) Retry-After 파싱/전달 스레딩
- `retry_after_opt`(mod.rs:40) 음수/0/NaN/Inf 가드 → `retry_after`(기본 2s) 래퍼. anthropic.rs:363/399, gemini.rs:305/339 모두 `retry_after(&resp)`로 헤더 소비 전(본문 미소비 상태) 호출 — 정합. `Retry-After: 0`이 2s 기본값으로 바뀌는 것(즉시재시도→2s)은 보수적 방향 의도된 가드.
- 미들스트림 error 이벤트의 `RateLimited(2s)` 하드코딩: SSE 본문에는 표준 재시도 힌트가 없어 HTTP 경로 기본값과 동일. 결함 아님.

### (2) RateLimited 승격 경로(스트림 내 error 이벤트) 부분출력/중단 정합
- anthropic.rs:661-679 — overloaded_error/rate_limit_error → `Err(RateLimited(2s))` 1회 반환 후 `done=true`로 스트림 종료(이중 발화 없음, 상태 튜플 11요소 정확히 스레딩).
- 소비자 추적: runtime.rs run_prompt — 초기 `chat_stream` Err만 백오프+1회 재시도(라인 171); 미들스트림 Err는 부분 텍스트를 assistant로 영속화 후 에러 반환(rpc -32603) — 이미 방출된 텍스트가 있어 자동 재시도면 중복이 되므로 현 설계가 정합. api.rs 스트리밍 경로는 미들스트림 Err(타입 무관)를 SSE error 이벤트+[DONE]로 종료 — 패치 전과 클라이언트 가시 결과 동일(회귀 없음). 비스트림 경로는 미들스트림 에러 자체가 없음. `is_context_overflow` 오탐 없음("rate limited; retry after 2s" 미매칭 확인).
- 채널 브리지(channel.rs:290)는 에러를 채팅으로 전달 — 정상.

### (3) stop 정규화(문자열→배열 랩)
- anthropic.rs:241-250 — `stop.as_str()`만 랩, 배열 통과, null 필터 유지. `stop:""`→`[""]`→400, `stop:[]`→400, `stop_sequences:null`+`stop:"x"` 무시 — 모두 패치 전과 동일 동작(신규 아님). 문자열 케이스(원 결함)는 수정 확인, 테스트 4케이스 정합.

### (4) gemini promptFeedback/blockReason
- 비스트림(gemini_to_openai): candidates 부재/finishReason null + blockReason 문자열 → `finish_reason:"content_filter"` — 오판 경로 점검: finishReason 존재 시 후보 매치 우선(동작 유지), blockReason 비문자열(null) 무시, 빈 candidates 배열 → Null 인덱싱으로 정상 분기. 이중보고 없음(스트림과 비스트림이 각각 한 번만 보고).
- 스트림: block_reason을 상태에 저장, EOF에서만 에러화 — finishReason이 같은 청크에 오면 Done이 우선(block_reason 미사용, 정당). 안전차단 후 io 에러로 종료되면 block_reason 대신 io 에러가 그대로 표면화(수용 가능).

### (5) mangled_for 충돌우회 (2 프로바이더)
- mod.rs `mangled_for`: base 점유자가 동일 orig면 재사용, 다르면 `base[..54]+"__"+sha256[:8]`(정확 64자, `{1,64}` 경계 내). 해시는 orig 종속 — 서로 다른 orig 간 변형 충돌 불가(2^-32 무시 수준).
- 선언-먼저 등록 순서 변경(양쪽 동일): 선언→히스토리 walk→tool_choice 순 호출 모두 `mangled_for`로 정합. 미선언 히스토리 툴이 선언된 리터럴과 충돌 시 히스토리가 변형 이름을 받는데, Gemini/Anthropic 모두 과거 functionCall/tool_use 이름을 현재 선언에 대해 검증하지 않고(a few-shot 패턴) 호출/응답 쌍은 동일 이름으로 정합 — 기존 맵 오염(원 High 버그) 대비 엄격히 개선.
- unmangle 폴백 제거(replacen→map 전용): 런타임은 매 턴 MCP 전체 툴 세트를 선언해 맵이 항상 완전. /v1 클라이언트가 툴 없이 재요청 시 환각 호출은 가시적 unknown-tool 에러 — 의도된 설계(주석 명시).

### (6) thinking/redacted/usage 기존 로직 회귀
- anthropic_events의 thinking/redacted_thinking/Usage 증분(message_start 기준선 차감) 로직은 무변경(추가된 error 분기만). gemini thought:true→Thinking, finishReason 청크 Usage 무변경. 회귀 없음.

## 커버리지 (실제 읽은 범위)
- src/provider/anthropic.rs 전체(912줄), src/provider/gemini.rs 전체(685줄), 양 파일 diff 전문
- 소비자: src/provider/mod.rs(Provider 디스패치, mangled_for, retry_after/retry_after_opt, http_client), src/runtime.rs(run_prompt 스트림 루프·백오프·부분 영속화·maybe_compact), src/api.rs(translate_forward 스트리밍/비스트림 RateLimited 분기), src/rpc.rs(session/prompt 에러 응답)
- 테스트: 양 파일 #[cfg(test)] 전문, tests/providers.rs diff(4개 갱신 테스트 포함 전문 정독)
- 중복 배제: audit-scratch-2026-09-19/rerun-existing-digest.md 정독 — 기록된 4건(anthropic string stop 400 / mid-stream overloaded generic / Usage{input:0} / gemini blockReason 은폐)은 수정 확인만 하고 재보고 없음
- 검증: `cargo check --lib` 통과, `cargo test --lib provider::` 21 passed, `cargo test --test providers` 34 passed (스코프 실행만; 전체 스위트 미실행)

## 조사했으나 반증으로 폐기한 후보
1. **미들스트림 RateLimited 2s 하드코딩** — SSE error 이벤트에 표준 힌트 부재, HTTP 기본값과 동일. 폐기.
2. **api.rs SSE 경로에서 RateLimited 타입 정보 소실(주석 과대 주장)** — 클라이언트 가시 결과가 패치 전과 동일(SSE error 이벤트), 동작 회귀 없음. 주석/스타일. 폐기.
3. **런타임이 미들스트림 RateLimited를 재시도하지 않음** — 부분 텍스트 이미 방출+영속화되어 재시도 시 중복; 에러는 이유와 함께 표면화. 설계 정합. 폐기.
4. **`stop:""`/`stop:[]` → Anthropic 400** — 패치 전에도 400(무효 입력 부류), 동작 불변. 폐기.
5. **`stop_sequences:null`+`stop:"x"` 드롭** — get/or_else/filter 체인은 패치 미변경(문맥 라인). 폐기.
6. **unmangle 폴백 제거로 다중 점 이름 복원 불가 시나리오** — 맵 미스는 미선언 툴 환각뿐(런타임은 항상 전체 선언), 가시적 에러가 정합. 폐기.
7. **미선언 히스토리 툴의 변형 와이어명이 선언에 없음** — 두 API 모두 과거 호출 이름 미검증, 요청 내 쌍 정합 유지. 폐기.
8. **변형 이름 64자 경계/chars-기반 절단** — 정확 64자로 규칙 내; 멀티바이트는 원래 무효 이름 부류(선행). 폐기.
9. **gemini 후보 수준 finishReason:"SAFETY"→"stop" 침묵** — 패치 미변경 매칭(사전 존재). 폐기.
10. **gemini 미들스트림 429(RESOURCE_EXHAUSTED) 에러 청크가 RateLimited 미승격** — 패치가 건드리지 않은 사전 존재 코드(anthropic만 수정됨). 폐기(범위 밖).
11. **안전차단 청크 후 io 에러 시 block_reason 미표면화** — io 에러 자체가 상위 전파, 정보 손실 아님. 폐기.
