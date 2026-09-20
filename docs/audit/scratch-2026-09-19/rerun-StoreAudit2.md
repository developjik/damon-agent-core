# rerun-StoreAudit2 — src/store.rs 전수 재감사 (2026-09-19, 읽기 전용)

대상: src/store.rs 미커밋 패치 (+146/−3) — tighten_permissions(0700/0600) 도입, search()의 fts5_query 토크나이저 교체.
중복 배제: rerun-existing-digest.md 대조 완료. 기존 store.rs 2건(fts5 단항 NOT, DB/디렉터리 기본 퍼미션)은 모두 이번 패치로 수정된 항목 — 아래는 수정 코드 자체의 신규 결함만.

## 결함 후보: 1건

### [Medium] WAL/SHM 사이드카 파일이 0600 조치 범위 밖 — 트랜스크립트 데이터 group/world-readable 잔존
- 위치: src/store.rs:60-69 (추가 라인 65-66 `tighten_permissions(path, 0o600)`, 맥락 69 `PRAGMA journal_mode = WAL`), 함수 본체 30-53
- 분류: security
- 시나리오: umask 022(통상 기본) 프로세스에서 damond 기동 → `Connection::open(path)`(60)이 db를 0644로 생성 → `tighten_permissions(path, 0o600)`(66)이 메인 db만 조임 → `conn.call`의 `PRAGMA journal_mode = WAL`(69) 이후 첫 쓰기 트랜잭션(create_session/append/FTS backfill)에서 SQLite가 `damon.db-wal`·`damon.db-shm`을 기본 0644 & ~umask로 생성하고 어디서도 tighten하지 않음 → 커밋된 트랜스크립트 페이지를 담은 `-wal`이 데몬 수명 내내(마지막 커넥션 종료 시에만 삭제, 크래시 시 영구 잔존) world-readable. 동일 호스트 로컬 사용자가 `cat damon.db-wal`로 대화 내용 열람 가능. 패치의 명시적 위협 모델("Session transcripts ... must not be group/world-accessible") 위반.
- 근거:
  ```rust
  let conn = Connection::open(path)                       // 파일이 0644 & ~umask로 생성
      .await ...?;
  #[cfg(unix)]
  tighten_permissions(path, 0o600);                       // 메인 db만 조임
  conn.call(|c| {
      c.execute_batch(
          "PRAGMA journal_mode = WAL;                     // -wal/-shm은 이후 0644로 생성, 미조치
  ```
  실측 재현(umask 022, python sqlite3): `w.db`/`w.db-shm`/`w.db-wal` 모두 `-rw-r--r--` 생성 → `w.db`만 0600 chmod 후에도 `-wal`(트랜스크립트 행 포함 12KB)·`-shm`은 `rw-r--r--` 유지. umask 022 환경은 수정이 실제로 필요한 환경이며, 바로 그 환경에서 사이드카가 누수.
- 수정 제안: `Connection::open` 전에 `path`, `path-wal`, `path-shm` 세 파일을 create(빈 파일)+0600 chmod로 사전 생성하라 — SQLite는 기존 파일을 재사용하며 퍼미션을 변경하지 않으므로 사이드카 갭과 첫 오픈 시 메인 db 0644 창이 함께 닫힌다. (open 후 chmod 방식은 체크포인트/재오픈 시 사이드카가 재생성되므로 불완전.)
- 자체반증: (a) auto-checkpoint가 -wal을 비우지 않는가 → PASSIVE 체크포인트는 파일을 truncate/삭제하지 않고, 데몬은 장수 명 커넥션이라 -wal은 기본 존재하며 직전 커밋 페이지를 담음. (b) umask 0077이면 증상 없음 → 맞으나 그 환경은 수정 자체가 무의미한 환경. (c) tests/store.rs `open_tightens_dir_and_db_permissions`(197-217)가 통과함 → 해당 테스트는 dir/main db 모드만 검증하고 사이드카는 미검증이라 갭을 못 잡음(맥락, 테스트 자체 결함 아님).

## 반증하여 버린 후보
1. **첫 오픈 시 메인 db 0644 창**(open→tighten 사이) — 창 동안 파일은 0바이트(모든 쓰기는 conn.call 이후)라 유출 내용 없음. 단독 결함 불성립, 제안에 흡수.
2. **parent="" 상대경로에서 "cannot tighten" warn** — `Path::new("x.db").parent()`가 Some("")일 때 set_permissions 실패 warn. 실제 호출자(damond.rs:132 `data_dir.join("damon.db")`, tests tempdir)는 모두 절대경로 → 도달 불가.
3. **fts5_query 파서 회귀 전수 추적** — `"a" NOT "b"`(binary NOT), `x AND NOT y`→`x NOT y` 재작성, 선행/후행 연산자 drop, 연속 연산자 collapse(last-wins), 미종결 따옴표, `""` escape, 빈 phrase skip, 유니코드 chars() 순회(바이트 경계 없음), `NEAR(...)`/`col:value`/`*` 인용 처리 — 모든 입력에서 출력이 `term (op term)*` 형태로만 생성되어 FTS5 구문 오류 경로 없음. tests/store.rs:83-111, 180-195가 동일 변환 고정. 결함 없음.
4. **선행 `NOT error`가 "error" 포함 검색으로 전환** — 의미론적 놀라움 있으나 코드 주석·doc comment·테스트(189-190)로 명시적 설계 → 의도적, 비보고.
5. **동시 open 시 chmod 경쟁** — set_permissions 멱등, 마이그레이션 ALTER 재확인은 기존 코드. 무해.
6. **0o400→0o600처럼 기존 더 엄격 모드를 느슨화** — "Force mode" 명시적 설계.
7. **Windows no-op(cfg(unix))** — 의도적.
8. **search 호출부 계약**(rpc.rs:515-519, bin/damon.rs:133) — None→빈 벡터 조기 반환은 구 에러 경로와 동일하게 Ok 처리, 연산자-only 쿼리의 빈 결과는 주석+테스트로 명시. 회귀 없음.

## 커버리지
- `git diff -- src/store.rs` 전체(3 hunk) / src/store.rs 1-698 전문 정독
- tests/store.rs: 전체 신규/변경 테스트 문맥(25-26, 52, 83-111, 180-195, 197-217)
- 호출부: src/rpc.rs:515-519, src/bin/damon.rs:132-135, src/bin/damond.rs:110-135
- 실험: /tmp/store-audit에서 umask 022 WAL 사이드카 퍼미션 재현(근거 스크린샷 텍스트 위 참조)

결론: 결함 1건(Medium, security). fts5_query 자체는 결함 없음.
