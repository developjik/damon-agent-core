# rerun-ProvRest — provider responses/mod/inband/compat/discovery + llm 재감사 (2026-09-19)

대상: 리미디에이션 패치(미커밋) 중 src/provider/{responses,mod,inband,compat,discovery}.rs, src/llm.rs.
원칙: 신규 결함만. `rerun-existing-digest.md`와 같은 파일+같은 증상 배제.

## 결함 후보 (2건)

### [High] 문자열 `reasoning` 패스스루가 `_thinking` effort 병합에서 panic 유발
- 위치: src/provider/responses.rs:198-213 (핵심 :205, :212)
- 분류: correctness
- 시나리오: `/v1/chat/completions` 클라이언트가 `{"model":"gpt-5:low","reasoning":"high",...}` 전송 (openai-responses 프로바이더) → api.rs:440-442가 `body["_thinking"]="low"` 주입, 나머지 키는 원문 그대로 `chat()/chat_stream()` → `translate_request` → 이번 패치가 추가한 문자열 병합 `Some(v) if v.is_string() => out[key] = v.clone()` (:205)이 `out["reasoning"]`을 JSON **문자열**로 설정 → 13줄 아래 `out["reasoning"]["effort"] = json!(level)` (:212)이 serde_json IndexMut로 문자열에 키 삽입 시도 → **panic "cannot access key \"effort\" in JSON string"** → axum 핸들러 태스크 unwinding, 해당 요청 커넥션 즉시 절단(응답 없음). panic=abort 아님(Cargo.toml 프로필 없음), CatchPanic 레이어 없음 — 데몬은 생존하나 해당 요청은 100% 실패.
- 근거 (패치 전후 차이 — 패치 전에는 문자열이 `.filter(|v| v.is_object())`로 걸러져 `out["reasoning"]`이 존재하지 않아 :212이 Null→object 자동생성으로 안전했음):
  ```rust
  for key in ["reasoning", "text", "truncation"] {
      match body.get(key) {
          Some(v) if v.is_object() => { ... }
          Some(v) if v.is_string() => out[key] = v.clone(),   // 신규: reasoning="high" 문자열 그대로 설정
          _ => {}
      }
  }
  // ...
  if let Some(level) = body["_thinking"].as_str() {
      out["reasoning"]["effort"] = json!(level);              // 문자열에 인덱싱 → panic
  }
  ```
  실증: serde_json 1로 재현 — `out["reasoning"]=json!("high")` 후 `out["reasoning"]["effort"]=json!("low")` → `panicked: true` ("cannot access key \"effort\" in JSON string").
- 수정 제안: `_thinking` 병합을 인덱싱 대신 안전하게 — `let mut r = out["reasoning"].take(); if !r.is_object() { r = json!({}); } r["effort"]=json!(level); out["reasoning"]=r;` 또는 문자열 패스스루를 `truncation`으로 한정.
- 자체반증: 문자열 `"reasoning"`을 보내는 실제 SDK가 있는지 확인 — 표준 OpenAI SDK는 미사용이나 /v1은 임의 JSON 통과(translate_forward가 `from_slice::<Value>` 원문 그대로 전달, api.rs:614-620). 1차 감사의 "Retry-After 음수→panic"과 동일 기준(비정상 입력 panic)으로 결함. panic만으로 데몬 전체가 죽지 않음( unwind 확인) → Critical 아님.

### [Medium] 배열 content 시스템 메시지 병합 스킵 — 다중 system 미지원 엔드포인트에 400 회귀
- 위치: src/provider/compat.rs:110-118 (핵심 :112)
- 분류: contract
- 시나리오: 프로바이더 compat에 `supports_multiple_system_messages: false` 설정(엔드포인트가 다중 system을 거부해서 운영자가 설정) + /v1 클라이언트가 연속 system/developer 메시지 전송, 그중 하나라도 content가 배열(예: `[{"type":"text","text":"..."}]` — 일부 SDK의 직렬화 형태) → 패치 전에는 두 메시지가 `content_text`로 평탄 병합되어 **요청 성공**(텍스트 손실 없음) → 패치 후 `if mergeable && prev["content"].is_string() && m["content"].is_string()` 가드가 병합을 건너뛰어 system 2건이 그대로 전송 → 해당 플래그가 존재하는 이유인 "다중 system 거부" 엔드포인트에서 400.
- 근거:
  ```rust
  let mergeable = matches!(prev_role, "system" | "developer") && prev_role == cur_role;
  if mergeable && prev["content"].is_string() && m["content"].is_string() {
      let prev_text = crate::provider::content_text(&prev["content"]);
      ...
  ```
  컴파니언 테스트(`system_merge_skips_array_content`)는 배열 보존만 검증하고 "엔드포인트로 가는 system 개수" 계약은 검증 안 함.
- 수정 제안: 배열 content는 평탄화 대신 이전 메시지에 텍스트 파트를 추가하는 방식(inband::render_tools의 배열 append 패턴)으로 병합하거나, 전 파트가 text인 배열만 문자열 병합 — 다중 system 전송 계약과 구조 보존을 동시에 만족.
- 자체반증: (a) 플래그 기본값이 true(config.rs:169-170 `default_true`)라 명시 설정 프로바이더만 영향 — 단 그 설정이 정확히 이 거부성 엔드포인트를 위한 것; (b) 배열 system + 연속 조합의 빈도는 낮음 → Medium. 다만 1차 감사의 "구조화 content 평탄화" 증상(평탄화 파괴)과는 다른 신규 증상(미병합→400)이므로 중복 아님.

## 결함 0건 판정한 후보 (반증 기록)
1. **mangled_for variant 삽입 덮어쓰기** (mod.rs:285-305): variant가 기존 claim과 충돌 시 무조건 `insert` 덮어쓰기 — 그러나 충돌엔 sha256[:4]==32비트 일치 + 54자 prefix 일치 + 세 번째 도구가 문자열리터럴로 variant명을 가지는 이중 우연 필요(관측 가능 경로 성립 안 함). openai.rs `variant_name`의 재해시 루프와 격차는 있으나 실피률 ~2⁻³². 폐기.
2. **mangled_for base 경로 64자 무제한** (Gemini functionDeclarations 64자 제한 위반 가능): 구 per-provider `mangle()`(=replace만)과 동일 동작 — 패치 전이랑 같아 신규 아님. 폐기.
3. **input_content_parts: url 없는 image_url 파트 무경고 드롭** (responses.rs:303-308): "loudly" 주석과 불일치하나 유효 파트에는 무영향, 현실 트리거 없음. 폐기(품질).
4. **image_url `detail` 필드 드롭**: 충실도 저하 뿐, 오동작 아님. 폐기.
5. **responses_to_openai 요약 텍스트 무분리 concatenation**: 스트리밍 Thinking 이벤트와 형태만 다르고 소비자는 불투명 텍스트 취급. 폐기.
6. **user content null/빈 배열 → `"content": []`**: 구 코드도 빈 input_text 전송(양안 거부 가능성 동일), 회귀 입증 불가. 폐기.
7. **`"text"` 문자열이 response_format 유도 객체 덮어쓰기**: top-level text 문자열 + response_format 동시 전송이라는 이중 비표준 조합 필요. 폐기.
8. **api.rs promote_or_error가 up.retry_after 미전달(패스스루 429 Retry-After 헤더 상실)**: 필드는 신규지만 동작은 패치 전후 동일(회귀 아님, 커버리지 기록으로만 남김).
9. **retry_after HTTP-date 형식 → 기본 2s**: 구현 교체 전과 동일 의미론. 폐기.
10. **anthropic mid-stream overloaded→RateLimited(2s) 하드코딩**: SSE error payload엔 Retry-After 부재, 명시적 기본값 설계. 폐기.
11. **inband 배열 system에 프롬프트 append 시 분리자 부재(평탄화 시)**: 유일 소비자(openai passthrough)는 평탄화 안 함. 폐기.

## 커버리지
- 전문 정독: src/provider/mod.rs(329), responses.rs(674), inband.rs(226), compat.rs(204), discovery.rs(166, 패치 미변경), src/llm.rs(207).
- 호출처 추적: mangled_for → anthropic.rs:109/204/272(선언→replay→tool_choice 순서 정합 확인), gemini.rs:86/149/177/271(선언 선행 + functionCall/functionResponse 동일 wire명 확인, 충돌 테스트 검증), openai.rs mangle_tool_names/mangle_name/variant_name(독자 구현, 정합). retry_after/retry_after_opt → responses×2, anthropic×2, gemini×2, openai forward(업스트림 헤더 캡처 후 bytes_stream). UpstreamResponse.retry_after → openai chat/chat_stream 소비(기본 2s). collect_stream → openai×3 + api.rs:507(30s 타임아웃). content_text → 다수 읽기 전용. inband render_tools/events_with_tool_calls/response_with_tool_calls → openai.rs inband 경로. shape_messages → compat::apply → openai forward. ToolCallAccumulator.finish(id&&name 요건) vs 5개 생산자(openai Slots[기존], responses added+delta, anthropic block_start+input_json_delta, gemini functionCall, inband 합성 델타) 정합. _thinking 주입: api.rs:441, runtime.rs:155.
- 컨텍스트: openai.rs(forward/strict-retry/chat 경로 전체), anthropic.rs(translate/events), gemini.rs(translate/events), api.rs(translate_forward, promote_or_error, SSE 재송출), config.rs(split_thinking_level, ProviderCompat 기본값), Cargo.toml(panic 프로필 부재 확인).
