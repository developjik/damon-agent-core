# rerun-BinsOauthCfg — 2차 감사 (2026-09-19, 리미디에이션 패치 대상)

범위: src/bin/damond.rs, src/bin/damon.rs, src/bin/damon-{slack,discord,telegram}.rs,
src/config.rs, src/oauth.rs, config.example.toml — 전 파일 정독 + git diff로 신규 코드 분리.
중복 배제: rerun-existing-digest.md 적용. 1차 Low 5건(config glob ?, constant_time_eq,
ModelMeta deny_unknown_fields, ensure_config chmod 창, damond RUST_LOG 순서, damon EPIPE,
--token ps 노출, oauth 상태코드 소실) 모두 수정 확인 — 재보고 아님.

## 결론: 신규 결함 후보 0건

패치가 만진 모든 경로를 검증했고 결함 없음. 항목별 검증 요약:

1. **config glob char 매칭 (config.rs:464-497)** — `?`가 1바이트→1스칼라 전환은 정확함.
   - `?`는 si+=1 후 연속 바이트(0x80-0xBF)를 스킵하므로 항상 스칼라 경계에 착지
     (mid-scalar 진입 시에도 잔여 continuation을 소진해 경계 종료 — 4바이트 포함 확인).
   - 백트래킹(`star_si += 1`)이 mid-scalar에 착지해도 오탐/누탐 없음: 패턴 재개점(pi=star+1)은
     항상 패턴 스칼라 경계이고, 리터럴 lead 바이트(≥0xC2)는 문자열 continuation 바이트(0x80-0xBF)
     와 절대 불일치 → 다중바이트 리터럴은 lead 정렬 시에만 매치, 이후 바이트 락스텝이라
     스칼라 단위 동치 보장. `?`가 스칼라 꼬리를 먹는 경우는 `*`가 같은 스칼라 머리를 먹은 것과
     동일 스팬이라 정규 의미론과 결과 동일.
   - 연속 `**`(star 재설정), 빈 패턴(""→빈 문자열만 true), 말단 `*`, 패턴 소진 후 잔여 문자열
     백트래핑 종료 모두 확인. 선형 시간 유지.
   - 호출처(route_model_strict:407, model_meta:438) 미변경, ASCII 의미론 불변 → 라우팅 회귀 없음.
2. **dotenvy/tracing 순서 (damond.rs:59-70)** — .env가 EnvFilter 이전에 로드되어 RUST_LOG 반영.
   Config::load와 동일 우선순위(config-dir 먼저, cwd는 debug만)로 재로드해도 dotenvy가
   기설 변수를 덮지 않아 no-op. Args::parse가 dotenv 앞으로 이동했지만 damond Args에는
   env 연동 인자가 없어 무영향. default_config_path가 .env 이전에 XDG env를 읽는 것은
   디렉터리 해상도로서 정상.
3. **oauth 상태코드 보존 (oauth.rs:216-224, 293-301)** — 비JSON 에러본문 시 ({status}) 보존
   bail 추가 확인. bail!의 {v} 출력은 !is_success에서만 도달 → access_token 등 성공 바디
   시크릿은 절대 에러 문자열에 유입 불가. refresh의 refresh_token 부재 시 구값 유지는 기존 코드.
   신규 유닛테스트의 env 변수 race 공제문 검증: DAMON_TEST_TOKEN_URL/DIR을 만지는 lib 유닛테스트는
   이 테스트뿐(tests/oauth.rs는 별도 통합 바이너리 + 자체 ENV_LOCK) → 안전.
4. **CLI EPIPE/토큰 인자 (damon.rs:171-190, platform bins)** — outln/out이 write/flush 오류 시
   exit(141) — 관례 준수, 기존 panic 101 해소. 스트림 중단 시 미응답 권한 요청은 데몬 측
   permission timeout이 정리. platform 3종의 value_source("token")==CommandLine 경고는
   clap id 정확, get_matches+from_arg_matches는 parse와 동일 의미론 → 회귀 없음.
5. **기타 신규 코드** — ensure_config create_new+mode(0600) 원자성 및 AlreadyExists 레이스 회수,
   constant_time_eq 길이 XOR 누산(동작·테스트 정확), ModelMeta deny_unknown_fields,
   damond mcp.shutdown() — mcp.rs:132-162에서 서버당 5s 타임아웃+take() 패스트페일 확인,
   두 서브 분기 모두 서브 반환 후 1회 호출.

## 조사했으나 반증해서 버린 후보

- **release에서 `--config <베어파일명>` 시 dir("").join(".env")로 cwd .env가 로드됨(디버그 게이트 우회로 보임)**
  → 반증: 이 동작은 패치 전부터 Config::load의 dir 분기(미변경 영역, config.rs:336-339)에 존재했고
  damond 신규 코드는 이를 충실히 미러링할 뿐(코멘트 명시). 신규 효과는 RUST_LOG 타이밍뿐.
  리미디에이션 회귀 아님 → 폐기(선존재 불일치로 기록만).
- **oauth 에러 bail!에 서버 JSON {v} 출력 — 시크릿 유출?** → 에러 상태에서만 도달 + OAuth 에러
  바디에는 토큰이 없음. 성공 바디 경로는 bail 미도달. 기각.
- **urlencoding의 `c as u32` → 비ASCII Latin-1 인코딩** → 호출처가 REDIRECT_URI/SCOPE ASCII
  상수 2곳뿐. 도달 가능한 비ASCII 입력 없음. 기각.
- **신규 oauth 유닛테스트 unsafe set_var/remove_var — 병렬 테스트 race** → 같은 프로세스(lib 테스트
  바이너리)에서 해당 변수를 만지는 다른 테스트 없음. 통합 tests/oauth.rs는 별도 프로세스. 기각.
- **glob 백트래킹이 mid-scalar에 착지 — 오탐 가능?** → 위 1번의 lead/continuation 바이트 대역
  분리 논증으로 불가능 입증. 기각.
- **constant_time_eq가 max(len) 루프로 길이 노출** → 상수시간 비교의 본질적 한계로 문서화된 동작,
  조기반환 제거가 수정 의도 자체. 결함 아님.
- **ensure_config 쓰기 중단(ENOSPC) 시 부분 스타터 설정 잔존** → 기존 write+chmod도 동일 실패류,
  다음 로드에서 TOML 파스 에러로 표면화. 신규 아님.

## 커버리지

- 전독: src/config.rs(1-685), src/oauth.rs(1-397), src/bin/damond.rs(1-430), src/bin/damon.rs(1-315),
  src/bin/damon-slack.rs, src/bin/damon-discord.rs, src/bin/damon-telegram.rs(전체), config.example.toml.
- diff 전수: audit-scratch-2026-09-19/diff-BinsOauthCfg.txt(488줄, 대상 8파일).
- 부분: src/mcp.rs:120-178(shutdown 검증), tests/oauth.rs 선두(ENV_LOCK 확인), tests/providers.rs glob 테스트.
- 미수행: cargo check/test(동시 편집 형제 에이전트 존재 — 프로젝트 전역 검증은 Main 몫, 지시상 read-only).
