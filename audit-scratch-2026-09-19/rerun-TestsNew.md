# 재감사 — tests 신규·변경분 (TestsNew, 2026-09-19)

결론: **결함 0건** (신규 후보 없음). 중복 다이제스트의 기존 3건(tests/runtime cancel 미도달, tests/phase4 tautology, tests/mcp_server.py ping, examples/bench 2건)은 이번 패치에서 수정되었으며, 수정의 품질을 아래에서 개별 검증함.

## 검증 방법
- 전체 diff(`git diff -- tests/ examples/bench.rs`, +441/−33) 대 조작 후보 전수 대조
- 소스 교차검증: src/runtime.rs(툴루프·execute_tool·maybe_compact·모델해석), src/store.rs(fts5_query·search), src/service.rs(systemd_escape·schtasks_escape), src/provider/anthropic.rs(mangle/unmangle·RateLimited 매핑)
- 실행 증명(전체 스위트 아님, 변경분만): `cargo test --test runtime -- <신규3>` 3 passed(0.09s), `--test store -- <신규3>` 3 passed, `--test service -- <신규2>` 2 passed, `--test phase4 -- mcp_tool_call_with_permission` 1 passed, `--test providers -- <변경3>` 3 passed, `cargo check --example bench` OK

## 수정 품질 검증 (거짓통과 렌즈)
1. **cancel_reaches_inflight_tool_and_persists_cancelled** (runtime.rs:338-419): 실제 회귀테스트임. 토큰이 in_progress notify(src/runtime.rs:330-344, join_all 직전)에서 먼저 설정되고 execute_tool의 select(src/runtime.rs:739-742)가 Cancelled sentinel로 선점 → "cancelled" 행 지속. sentinel select가 회귀로 사라지면 test.sleep(5s)이 실제 실행되어 content 불일치로 실패(그리고 5초 이상 소요). 통과 시간 0.09s로 선점 경로 활성 확인. 행 누락(len≠3), error 위장(content≠"cancelled"), 2차 LLM 호출(len=4) 각각 감지.
2. **cancel_does_not_mask_real_tool_errors** (개명): unknown-tool 실 에러가 `has_tool` 조기 bail(src/runtime.rs:697-699)에서 확정적으로 발생 — 취소 select 이전이므로 tokio::select 무작위성 없이 결정적. 회귀(루프 상단 취소 검사가 execute_tool 선두로 이동 등)는 msgs[3] "error: unknown tool b" 단언으로 실패.
3. **compaction_reevaluates_after_tool_results_grow_history** (runtime.rs:1189-1289): 모델 해석 경로 확인 — default_model=None → "default"(src/runtime.rs:98-101) → models["default"].context_window=1 적용, 반복당 1회 평가(src/runtime.rs:143-146). 요약은 비스트리밍 호출로 카운트(모의가 stream 플래그로 분기). 구 플래그 회귀 시 1≠2 실패. 결정적.
4. **store search 2건**: fts5_query(src/store.rs:312-396) 토큰·연산자 규칙과 전 수치 단언 대조 — "error NOT timeout"=0(FTS5 이진 NOT 배제语义), "rust AND NOT gc"=1(pending_op 리셋), 선행/후행 NOT 강등, "content:error"/"NEAR(...)"=0(구절 토큰화 강등), "AND OR"/"NOT"/"\"\""=None→빈집합 전부 정합. tautology 없음.
5. **open_tightens_dir_and_db_permissions**: 0o700/0o600 정확 단언 + 이완 퍼미션 재오픈 재조임. pid 스코프 임시 디렉터리, 선제 remove_dir_all — 스위트 오염 없음.
6. **service 2건**: schtasks_escape 수동 추적(C:\da"mon\damond.exe\ → C:\da\"mon\damond.exe\\) — /TR 리터럴 3단언 정확. systemd %→%% 단언 정확.
7. **providers 3건**: 요청에 tools 선언으로 mangle 맵 복원 경로 활성(anthropic.rs:108-113), 복원명 `fs.read` 단언은 진짜 감별자(mangled `fs__read`와 구분). midstream overloaded → RateLimited 타입 downcast 단언(기존 문자열 포함보다 강함), saw_err 유지.
8. **phase4 tautology 수정**: `contains(r#"{\"msg\": \"hi\"}"#)` — 이스케이프된 페이로드 형태만 매칭(요청 인자 객체 직렬화로는 우연히 통과 불가) + isError:true 부정 단언. 진짜 종단 간 인자→MCP→행 검증.
9. **mcp_server.py**: ping → `result {}` (MCP 스펙 정합, 기존 -32601 수정). sleep 툴은 cancel 테스트가 실사용(has_tool("test.sleep") 단언). BrokenPipe 조용한 종료. tools/list 2개 확장의 영향 없음(툴 수를 고정하는 단언 전무 — grep 확인).
10. **bench.rs**: 포트 폴링+10s 데드라인+킬 후 panic(이전 "deadline 후 블로킹 read" 수정), 배수 스레드로 파이프 가득 참 방지, idle_rss 0/미조회 시 panic. 컴파일 확인.

## 반증해서 버린 후보
- "inflight 이름 과장"(토큰이 실행 전 설정) — src/runtime.rs:739 select가 곧 실행 중 선점 메커니즘; 제거 시 테스트 실패. 증상 아님.
- "compaction 모델맵 미적용/이중카운트 가능" — 해석 경로·결정적 카운트 실행으로 확인, 폐기.
- "error NOT timeout=0가 잘못된 기대" — FTS5 이진 NOT 배제语义와 정합, 폐기.
- "tools/list 2개로 다른 테스트 오염" — 툴 수 고정 단언 없음, 폐기.
- "perms 테스트 이름 충돌/오염" — pid 스코프+선제 정리, 폐기.
- "cancel_does_not_mask의 msgs[2] content 미단언" — call_1/call_2 동일 코드 경로(has_tool bail), 회귀 시 msgs[3]으로 반드시 실패 — 커버 갭 아님, 폐기.
- "bench 포트 TOCTOU(예약-후-해제)" — 변경 전 코드(패치 밖), 도입 아님, 폐기.
- "회귀 시 5s 슬립으로 스위트 지연" — 통과 시 0.09s; 실패 시에만 지연되는 fail-loud 설계, 폐기.
