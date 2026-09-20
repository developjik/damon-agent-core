# 재감사 — src/runtime.rs (+28/−22) — RuntimeAudit (2026-09-19)

대상: 리미디에이션 패치의 runtime.rs 변경 4개 헝크(컴팩션 플래그 제거, cancelled status, 루프말 주석, text_tokens 재작성) + 관련 lib.rs(변경 없음 확인). 중복 배제: rerun-existing-digest.md 및 audit-findings-2026-09-19.md 원문 대조 완료.

## 결론: 결함 후보 1건 (신규 증상 — 수정 코드 자체의 회귀)

### [High] CJK 토큰 추산 0.75/자(실측 ~1/자) 과소가 85% 컴팩션 마진을 무효화 — 한국어 세션 창 초과 400 후 장기 웨지
- 위치: src/runtime.rs:490-492 (text_tokens, diff 헝크), 551-553 (85% 판정)
- 분류: correctness | 경계(유니코드) × 컴팩션 임계 상호작용
- 시나리오: 한국어(CJK) 중심 세션에서 실제 토큰이 모델 창 W에 도달 → 새 추산 est = 실제×0.75 = 0.75W < 0.85W → `maybe_compact`가 551행에서 조기 반환 → 압축 없이 전체 tail로 `chat_stream` → upstream context-length 400 → 1회 재시도 후 turn 실패. 실패한 `run_prompt`는 user 행만 append하므로 est가 0.85W(=실제 1.133W)에 도달할 때까지 모든 프롬프트가 400으로 실패(200k 창 기준 실제 +26k 토큰 ≈ 수십~수백 회 실패). 토크나이저 비율 ≥1.13/자(cl100k·Claude 한국어)에서는 실제 1.7W 도달 전까지 사실상 영구 침체. ASCII 세션은 est/실제 = 1 그대로 85%에 정상 발동 — CJK만 체계적 열화. 구 감사 증상(2.25배 과대→조기 컴팩션)과 정반대 방향의 새 증상이므로 중복 아님.
- 근거:
```rust
fn text_tokens(s: &str) -> u64 {
    (s.len() / 4) as u64          // 3바이트 CJK 1자 = 0.75 token
}
...
if est < window * 85 / 100 {      // 551행 — 15% 마진은 추산이 정확해야 유효
    return;
}
```
repo 자체 실측 근거: 구 감사 항목원문 "실제 약 1 token/char", 신규 주석(489행) 스스로 "close to the real ~1 token/char" — 패치 문서 기준치로도 est/실제 = 0.75, 마진 15% > 오차 25% → 임계 목적 상실.
- 수정 제안: 비ASCII 바이트를 4/3 가중(3바이트 1자 = 1.0 token = 문서화된 실측치)해 마진을 복원:
```rust
fn text_tokens(s: &str) -> u64 {
    let non_ascii = s.bytes().filter(|b| !b.is_ascii()).count();
    ((s.len() + non_ascii / 3) / 4) as u64
}
```
- 자체반증: (1) "최신 토크나이저(o200k/Gemini) 한국어 0.5~0.9/자면 est 정확·과대" — 부분 타당하나 repo 문서 기준치(~1.0)로도 13% 창 초과가 발생하고 claude-*가 builtin 힌트 1순위이며 Claude/cl100k 계열은 ≥1/자로 악화. (2) "tool 출력 ASCII가 희석" — 희석은 est/실제를 1로 수렴시킬 뿐 상회 못함; 순수 대화 세션은 희석 없음. (3) "400 시 별도 오버플로 처리 존재" — grep 결과 context_window의 유일 소비점이 maybe_compact이며 chat_stream 에러는 재시도 1회 후 bail뿐 (api.rs promotion은 /v1 전용). (4) "MAX_TOOL_OUTPUT이 창 도달 방지" — 행당 상한일 뿐 행 수는 무제한. 반증 모두 실패, 보고 유지.

## 확인 무결(회귀 없음) — 패치 4헝크별
- **컴팩션 플래그 제거(+매 iteration maybe_compact)**: 구 동작 확인(git show HEAD — iteration 0만 실행, 이후 false 고정) → 수정이 실제 결함 해소. 회귀테스트 `compaction_reevaluates_after_tool_results_grow_history`(summarize_calls==2)가 정확히 고정. 컴팩션 전량 drop 시에도 store.messages가 summary를 선행 user 행으로 주입(store.rs:436-441)해 빈 배열 400 불가; keep_from while 루프가 tool-row 선두 불변식 유지(중간-turn 컴팩션 포함).
- **cancelled status**: content 분기와 status 분기가 동일 `cancelled` 플래키 사용 — 열거 전수 대조(Ok+취소=completed, Err(Cancelled)+취소=cancelled, 실제 Err=failed) 일치. 신규 값 "cancelled" 소비점 전수 확인: channel.rs:435-437(문자열 렌더), bin/damon.rs:265(println), npm/client.mjs·README(JSON 패스스루, status 열거 검증 없음) — 드롭/크래시/거부 없음.
- **execute_tool/mangle 연동**: runtime側 변경 없음. `call.name`은 provider별 per-request map(unmangle)로 원본명 복원 후 도달하며 has_tool/auto_approve/session_approved/approve_for_session 전부 동일 원본명 기준 — 승인 판정 일관. (mangle 내부 구현은 타 에이전트 범위)
- **text_tokens 경계**: 빈 문자열=0, 혼합 스크립트 단조, usize 나눗셈 오버플로 없음, u64 캐스트 무손실 — 경계 결함 자체는 없음(임계 상호작용 결함만 상기 1건).
- tests/runtime.rs 신규 3종(+229행): 거짓통과 없음 — mock이 실제 SSE/JSON 경로 통과, 실측 단언. mcp_server.py에 test.ping/test.sleep 실재 확인.

## 커버리지 (실제 읽은 범위)
- src/runtime.rs 1-782 전수 정독 + `git show HEAD:src/runtime.rs` 대조(구 check_compaction 동작 확정)
- git diff: src/runtime.rs(전 헝크), src/lib.rs·src/llm.rs(변경 없음 확인), tests/runtime.rs(전체)
- src/store.rs messages/messages_full/set_compaction/compaction(400-520 근방, summary 주입·cutoff 필터)
- src/config.rs model_meta/builtin_context_window/ModelMeta(35-90, 425-465)
- src/channel.rs:425-440, src/bin/damon.rs:260-270(tool_call_update 소비점), npm/client.mjs·npm/README(상태 소비)
- src/provider/{openai,anthropic,gemini,mod}.rs mangle/unmangle 연동 지점(이름 복원 후 승인 판정 검증)
- tests/mcp_server.py(툴 실재), audit-findings-2026-09-19.md(중복 배제 원문 대조)

## 조사 후 반증/제외 후보
1. **maybe_compact 매 iteration messages_full 전체 로드** — 성능: 25회 상한·로컬 SQLite·의도적 문서화("early-returns cheaply" 표현은 다소 과장이나 결함 아님). 폐기.
2. **summarize 실패·빈 요약 시 iteration마다 재시도(최대 25회 provider 호출)** — maybe_compact 주석 "next iteration retries"로 명시적 의도. 폐기.
3. **컴팩션 전량 drop → 빈 messages 400** — store.messages의 summary 선행 주입으로 불가. 폐기.
4. **권한 프롬프트 대기 중 취소 → "permission denied" persisted + status:failed (cancelled 아님)** — execute_tool 해당 경로는 패치 미변경(사전 존재)이며 신규 아님 → 스코프 규칙상 제외. 원 감사 항목(453행)의 row/status 불일치 증상과도 별개 경로.
5. **ACP 표준에 없는 "cancelled" status 값** — 전 소비점이 임의 문자열 처리, 검증 열거 부재. 폐기.
6. **ASCII 4 chars/token도 JSON/코드엔 ~15-30% 과소** — 사전 존재 + 휴리스틱 허용오차(본 보고의 보강 맥락으로만 언급). 폐기.
