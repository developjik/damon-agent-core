# Phase 2 적대적 검증 — High 후보 3건 (VerifyHigh, 2026-09-19)

중복 배제: rerun-existing-digest.md 대조 완료. 3건 모두 "수정 코드 자체의 새 결함"(증상 상이) → 신규 해당.

## [RuntimeAudit-text_tokens] 판정: CONFIRM — 최종 심각도 High
- 재검증 경로:
  - git diff 확정: 구 `bytes().map(ascii1/nonascii3).sum()/4`(CJK 2.25 tok/char 과대 — 1차 감사 결함) → 신규 `s.len()/4`(0.75/char 과소). 패치 도입 확정.
  - runtime.rs:71-79 — run_prompt는 provider 호출 **전** user 행 append → 실패 turn에도 히스토리 성장.
  - runtime.rs:163-196 — chat_stream 400(비-RateLimited)은 warn 후 1회 재시도 → 동일 400 → `return Err(e)`. run_prompt 내 오버플로 특수분기 없음.
  - grep 전수: `is_context_overflow`/promotion 호출점 전부 api.rs(/v1 전용) — 내부 세션 경로 무관.
  - runtime.rs:551-553 — `if est < window*85/100 { return; }` 임계. est = 0.75×실제(CJK r=1.0일 때).
  - config.rs:83-86 — `claude-*` builtin context_window=200_000 → window=Some, 컴팩션 기계 활성.
  - 탈출구 부재: api.rs·channel.rs에 "compact/summarize" 수동 명령 없음(grep 전수).
  - 패치 주석 스스로 "close to the real ~1 token/char" — repo 내부 기준치로도 est/실측=0.75, 마진 15% > 오차 25%.
- 시나리오 재구성: 성공. 순수 한국어 대화 세션(r≥1.0, ASCII 희석 f<0.4): 실제 요청 토큰이 입력 상한(≈W−max_output, Claude는 200k에서 output 예약) 도달 → 400. est≈0.75W<0.85W → maybe_compact 조기 반환. 이후 매 프롬프트: user 행 +0.75r×chars est 적립뿐, est 0.85W 도달엔 실제 +0.133W(200k 기준 +26.6k tok ≈ 수백 회 실패 turn) 필요. Claude(cl100k계, 한국어 ≥1.0/자)·GPT-4(cl100k, 128k builtin) 확정 영향; o200k/gpt-4o·Gemini(한국어 ~0.6-0.8/자)는 마진 내 — 영향 모델 좁히나 대표 시나리오(Claude 한국어) 유효.
- 반증 시도: (1) "o200k/Gemini 괜찮음" — 부분 타당, claude-* builtin으로 현실 경로 유지 (2) "ASCII tool 출력 희석" — f>0.6 CJK 세션은 여전 웨지, 대화 세션 해당 (3) "400 별도 처리" — api.rs /v1 전용 확인 (4) "수동 탈출" — 없음 (5) "thinking/tool_calls est 계상" — 요청에도 동반 → 비율 중립. 모두 실패.
- 비판정 이유(하향): 프로세스 크래시·데이터 파괴 아님, 새 세션으로 우회 가능, est 적립으로 이론적 자력 회복 → Critical 아닌 High 유지.

## [ProvRest-reasoning-panic] 판정: CONFIRM — 최종 심각도 High
- 재검증 경로:
  - api.rs:382-389 — 모델 필드 `:low|:medium|:high` 접미사 split(config.rs:453-456 수용 목록 확인) → thinking.
  - api.rs:436-442 — 재직렬화 body에 `v["_thinking"]=json!(level)` 주입(클라이언트 원문 키 verbatim 보존).
  - api.rs:459-464 — 프로바이더가 OpenAiCompletions가 아니면(예: `api="openai-responses"`, config.rs:362 정식 타입) translate_forward → api.rs:614-620 `from_slice::<Value>` 원문 그대로 → chat_stream/chat.
  - responses.rs:201-213 — 신규 분기 `Some(v) if v.is_string() => out[key]=v.clone()`가 out["reasoning"]을 문자열로 set → 13줄 아래 `out["reasoning"]["effort"]=json!(level)`이 문자열에 IndexMut.
  - **실증(/tmp 독립 cargo, serde_json 1)**: `panicked: true`, payload `cannot access key "effort" in JSON string`. (repo 외부 /tmp에서 재현 — 소스 미수정)
  - Cargo.toml/.cargo에 `[profile]`/panic 설정 없음 → unwind. api.rs 라우터에 CatchPanic/catch_unwind 없음(rate_limit+require_token뿐, :246-267) → 핸들러 태스크 unwinding, 해당 커넥션 즉시 절단(응답 없음), 데몬 생존.
  - 인증: 루프백 바인드+auth_token 미설정 시 무토큰 통과(api.rs:852-888), 비루프백은 유효 토큰 필요.
- 시나리오 재구성: 성공. `POST /v1/chat/completions` {"model":"m:low","reasoning":"high"} (openai-responses 프로바이더) → 패치 전 `.filter(is_object)`는 문자열 드롭(Null→object 자동생성으로 안전) / 패치 후 100% panic. 패치 도입 확정(git diff).
- 반증 시도: (1) "문자열 reasoning 보내는 실 SDK 없음" — /v1는 임의 JSON verbatim 수용(스키마 검증 없음), 패치 주석 자체가 문자열 값을 현실 전제("truncation: 'auto'"), 1차 감사 Retry-After panic과 동일 기준 (2) "panic abort로 데몬 사망 → Critical" — 프로필 부재로 unwind, 데몬 생존 → Critical 아님 (3) "CatchPanic이 500 반환" — 부재 (4) "내부 경로 도달" — run_prompt body엔 reasoning 키 없음(/v1 전용, 맞음 — 현실 경로는 /v1). 모두 실패.
- 심각도 근거: 인증(또는 로우컬) 클라이언트가 결정론적으로 유발 가능한 핸들러 panic — 요청 단위 가용성 결함, 차기 사이클 수준(High). Critical 아님(전면 DoS 아님).

## [ProvRest-compat-array-system] 판정: CONFIRM — 최종 심각도 Medium
- 재검증 경로:
  - git diff 확정: `if mergeable` → `if mergeable && prev["content"].is_string() && m["content"].is_string()`(compat.rs:112) 패치 도입.
  - 소비점 전수: shape_messages 호출은 compat.rs:57(apply)뿐, apply 호출은 openai.rs:97(forward, POST /chat/completions 한정) — /v1 패스스루 경로.
  - 플래그: config.rs:169-170 default_true(기본 병합 안 함) — 그러나 config.example.toml:41 + docs/integration.md:37 + integration.ko.md:34가 `false`를 "연속 system 병합" **문서화된 계약**으로 광고. builtin 프로바이더 중 false 설정 없음(운영자 설정 전제).
- 시나리오 재구성: 성공. 운영자가 다중 system 거부 엔드포인트에 `supports_multiple_system_messages=false` 설정(그 목적 자체가 이것) + /v1 클라이언트가 연속 system 메시지 중 하나라도 배열 content(`[{"type":"text","text":...}]` — 정상 chat-completions wire 형식) 전송 → 패치 전 content_text 평탄 병합으로 단일 system(**요청 성공**) → 패치 후 병합 스킵 → system 2건 전송 → 엔드포인트 400. 텍스트 전용 배열에선 순수 회귀.
- 반증 시도: (1) "플래그 기본 true" — 그러나 플래그 존재 목적이 정확히 이 거부성 엔드포인트 (2) "배열 system 비현실적" — 유효 직렬화 형태, 멀티모달 지원 SDK 사용 (3) "하류 재병합" — apply/shape_messages 단일 소비점, 부재 (4) "1차 감사 중복" — 구 증상(평탄화 파괴)과 신규 증상(미병합→400) 상이, digest 규칙상 신규. 모두 실패.
- 심각도: 운영자 플래그 + 클라이언트 메시지 형태 이중 조건, 요청 단위 400(데이터 유실 없음), 확정적 재현 → Medium 유지(원 보고 제안과 일치).

## 요약
1) 승인: 위 3건 전부 (High 2 — runtime CJK 저과소산 웨지, responses reasoning panic / Medium 1 — compat 배열 system 미병합 400)
2) 폐기: 없음. 원 보고 3건 모두 반증 실패.
