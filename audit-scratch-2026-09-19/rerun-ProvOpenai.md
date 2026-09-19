# 재감사 — src/provider/openai.rs (ProvOpenai, 2026-09-19)

대상: 리미디에이션 패치(+379/−56, 미커밋). 신규 결함만 조사 — 특히 수정 코드의 회귀.

## 결론: 신규 결함 0건

패치가 이 파일에 도입한 변경 5개 영역을 전수 검증했고, 재현 가능한 신규 결함을 찾지 못했다.
근거 없는 억지 후보 배제 (컨텍스트 규칙 준수).

## 검증 내역 (패치 변경점별)

### 1. mangled_for/충돌우회 경로 (mangle_tool_names 사전 스캔 + variant_name)
- originals 사전 수집 패스는 침투적이지 않음(읽기 전용 클로저, mutating borrow만으로 write 없음).
  맹글링 전 원본 이름 전체를 확보하므로 처리 순서(도트명 먼저/리터럴 먼저)와 무관하게
  충돌 판정이 동일하게 성립함을 순서 조합별로 확인:
  - `fs.read` + 리터럴 `fs__read`: 어느 쪽이 먼저 와도 도트명만 `__<sha256[:8]>` 변형을 받고
    리터럴은 원명 유지. map 키는 변형명뿐이라 역방향 오염 없음.
  - 두 개의 서로 다른 도트명이 같은 base로 맹글되는 경우(`a_.b`/`a._b` → `a___b`):
    첫 명칭은 base, 두 번째는 `map.get(&base).is_some_and(|orig| orig != &name)` 조건이 잡아 변형.
  - 3중 충돌, 변형명이 또 다른 원본 리터럴과 충돌하는 경우: `taken()`이 originals∪map 양쪽을
    검사하고 재해시 루프로 우회 → 종료 보장(유한 집합 vs 2^32 후보공간).
- 역방향 맵 오염 불가 증명: `sent`가 (a) base 경로면 base ∉ originals 또는 map[data]가 동일 name,
  (b) 변형 경로면 taken()이 originals·map 모두 배제. 따라서 서로 다른 원본이 같은 wire명을
  주장하는 경우가 존재하지 않고, `sent != name`인 모든 wire명은 map에 기록됨 → 복원 완전.
- 같은 원본의 반복 사이트(tools/messages/tool_choice)는 variant_name이 map의 동일-name 예외로
  항상 동일 wire명을 반환(결정론적).
- 길이 불변식: mangle 64자 초과 시 54+2+8=64자, variant_name도 take(54)+2+8=64자로 일치.
  출력이 ASCII 전용이므로 chars==bytes.
- 복원 정합: 비스트리밍 `unmangle_tool_calls`(choices[].message.tool_calls)와 스트리밍
  ToolCallDelta `names.get(&n).unwrap_or(n)`가 동일 map 사용. map에 없는 응답명은 원통과.
  OpenAI는 이름을 첫 델타에 완전형으로 보내고 llm.rs ToolCallAccumulator는 name을 치환 방식
  (`slot.1 = Some(name)`)으로 저장하므로 부분 이름 조각 문제 없음.
- compat::apply/strict-retry는 이름을 건드리지 않음(확인: compat.rs는 role/id/필드명만,
  strict 제거만) → map 키와 실제 wire명의 불일치 없음.
- 회귀테스트 `mangle_collision_keeps_both_tools_distinct` 실행 → 통과.

### 2. SSE 파서/버퍼링 (Slots + 멀티라인 재파싱)
- `flush_event` 재파싱 경로: parse_chunk는 serde 파싱 실패 시 slots를 mutate하지 않으므로
  이중/순서 오염 없음. 라인별 순서 보존, finish는 마지막 유효값 채택.
  전 라인 실패 시 join 오류 유지(완전 부패 데이터는 여전히 에러).
- `Slots::slot`: index 있으면 index 우선 + id→index 재지각(후속 id-only 델타 일관),
  index 없으면 id별 순차 슬롯. 정상 OpenAI(index 항상)·llama.cpp(index 없음/id 항상)
  스트림 모두 올바름. 혼합 게이트웨이(첫 델타만 id, 이후 index 없음)는 슬롯 0 낙하가
  패치 전과 동일한 동작(회귀 아님).
- 회귀테스트 2건 실행 → 통과.

### 3. strict-retry 에러 경로 재조립
- content_type을 text() 소비 전 캡처(수정 의도대로). retry_after: None(400에는 무의미).
- 재시도 body는 shaped(맹글+stream:true+compat 반영 포함)에서 strict만 제거 — 이중 compat 적용 없음.
- 재시도 응답은 strict 검사에 재진입하지 않아 무한루프 없음. 재시도의 429는 하단 경로에서
  retry_after_opt 캡처 → chat/chat_stream의 RateLimited(hint)로 연결됨.

### 4. usage 수집
- 패치 미변경 영역. stream_options 미지원 게이트웨이의 마지막 usage 전용 청크(choices: [])도
  Usage 이벤트 발행됨, usage:null 청크는 is_object 필터로 스킵 — 기존 감사 범위와 동일.

### 5. 기존 로직 재섬
- forward 형성(POST /chat/completions만 shaped), authed/헤더, list_models, inband 분기,
  EOF 무터미널 에러(선감사 검증 영역, 패치는 slots 스레딩만 추가) — 신규 결함 없음.
- retry_after_opt(신규 호출점)는 음수/NaN/무한 가드 + 60s 캡 내장. 소비자 2곳 모두 안전:
  runtime.rs:171 백오프는 cancel과 select(60s 대기 취소 가능), api.rs:706은 클라이언트에
  429+retry-after로 전파.

## 커버리지
- src/provider/openai.rs 1–944 전체 정독 (raw 4분할).
- git diff -- src/provider/openai.rs 전체(±379/−56) 대조로 신규/기존 코드 구분.
- 보조 파일(상호작용 검증): src/provider/mod.rs(retry_after_opt, UpstreamResponse, RateLimited),
  src/provider/compat.rs(전체, 이름 무변경 확인), src/llm.rs(ToolCallAccumulator push/finish),
  src/runtime.rs(ToolCallDelta 소비·RateLimited 백오프), src/api.rs(translate_forward 재직렬화·
  RateLimited 전파 560–745).
- cargo test --lib provider::openai → 3 passed (읽기 전용 증거, 전체 스위트 미실행).

## 조사 후 반증/폐기한 후보
1. `Slots::slot`의 `self.next.max(i + 1)` — index=2^64-1 시 i+1 usize 오버플로(디버그 패닉,
   릴리스는 wrap). 그러나 (a) 악성/병적 업스트림 전용 입력, (b) 릴리스에서는 next만 wrap되고
   반환값·다운스트림(MAX_INDEX=256 드롭) 동작은 올바름 — 실패 시나리오가 디버그 빌드에서만
   관측 가능해 "현실적 경로 오동작" 기준 미달. 폐기.
2. 하나의 SSE 이벤트에 `[DONE]` + 다른 data 라인 혼재(`data: [DONE]`+`data: x`): 라인별
   재파싱이 [DONE]을 JSON이 아니라 스킵 → 터미널 누락 가능. 그러나 그런 이벤트를 송신하는
   업스트림이 관측된 바 없고(비전형·사양상 가능하나 실존 안 함) 재현 경로가 비현실적. 폐기.
3. EOF flush 후 소진된 스트림 재폴링(futures 계약상 None 후 재폴링 금지): unfold EOF 구조는
   패치 이전 코드(diff context로 확인)이며 reqwest/hyper는 반복 None 반환. 기존 동작. 폐기.
4. strict-retry 재구성 응답이 content-type 외 헤더를 소실: 1차 감사 동일 증상
   ("strict-retry 에러가 content_type 위조+헤더 소실") — 중복 배제 규칙 적용. 폐기.
5. 같은 파일 SSE buf.drain O(n²): 1차 감사 등재 증상, 패치 미변경 라인. 폐기.
