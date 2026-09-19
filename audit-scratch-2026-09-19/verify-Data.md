# Phase 2 적대적 검증 — VerifyData (데이터/퍼미션 후보 2건) / 2026-09-19

대상: [StoreAudit2] store.rs WAL 사이드카 퍼미션, [ChanService] channel.rs 24h 에빅션.
중복 기준: rerun-existing-digest.md 대조 완료.

---

## [StoreAudit2] 판정: REFUTE

### 재검증 (직접 읽은 코드 경로)
- src/store.rs:53-146 `Store::open` 전체 흐름: `create_dir_all(parent)` → `tighten_permissions(parent, 0o700)`(59-63) → `Connection::open(path)`(빈 파일 0644&~umask 생성, 부수 효과 없음) → `tighten_permissions(path, 0o600)`(65-66) → `conn.call`의 `execute_batch`에서 `PRAGMA journal_mode = WAL`(69) + CREATE TABLE들 — **사이드카는 이 시점(첫 WAL 쓰기)에 생성되며, 그때 DB는 이미 0600**.
- 번들 SQLite 소스 직독: `rusqlite 0.40.2 features=["bundled"]`(Cargo.toml:33) → libsqlite3-sys 0.38.2 = **SQLite 3.53.2**(vendored sqlite3.c:470).
  - sqlite3.c:46651-46687 `findCreateFileMode`: `SQLITE_OPEN_WAL`(및 MAIN_JOURNAL) 플래그로 열리는 파일은 `-wal` 접미사에서 DB 경로를 역유도해 `getFileType(zDb,…)`으로 **DB 파일의 현재 퍼미션을 조회해 그대로 생성**. 폴백(0644 기본)은 8.3-names/FAT 모드뿐(주석 46639-46643: "permissions do not matter there"). `damon.db-wal`은 '-' 포함 표준 네이밍 → 항상 상속 경로.
  - sqlite3.c:45162-45173 `-shm` 생성: DB fd를 `osFstat`해 "the same permissions"으로 생성.
- 유일한 production 호출자: bin/damond.rs:132 `Store::open(&data_dir.join("damon.db"))` — data_dir은 `cfg.data_dir` 또는 `default_data_dir()`(config.rs:507-511, 절대경로). parent= data_dir 자체가 0700으로 조임.

### 시나리오 재구성: 실패
- 실측(umask 022, python sqlite3 3.53.4, /tmp/verify-data-wal):
  - **A(=damond 순서)**: connect(db 0644 생성) → chmod 0600 → `PRAGMA journal_mode=WAL` → 쓰기+커밋 → 결과 `a.db-wal: 0600`, `a.db-shm: 0600`. **사이드카가 DB 퍼미션을 상속해 0600으로 생성됨.**
  - **B(=원 보고의 재현 순서)**: WAL 전환·쓰기 후 db만 chmod → `-wal`/`-shm` 0644 잔존. 원 보고(rerun-StoreAudit2.md)의 실측이 이 순서 — **damond 코드 순서(tighten이 WAL pragma보다 선행)와 반대**. 재현은 유효하나 대상 코드 경로가 아님.
  - D(크래시 잔존): pre-fix 실행이 남긴 0644 사이드카는 재오픈 시 퍼미션 유지(재사용 시 chmod 안 함). 그러나 (1) 그 파일은 수정 전 세계에서 애초에 전부 0644였던 잔존물, (2) 동일 패치가 data dir을 0700으로 강제해 접근 불가, (3) 다음 clean close에 삭제되고 재생성 시 0600.
- 결론: 원 보고의 사실 전제("SQLite가 -wal/-shm을 기본 0644 & ~umask로 생성하고 어디서도 tighten하지 않음")가 **damond의 실제 순서에서 거짓**. "로컬 사용자가 cat damon.db-wal로 열람" 경로 재구성 불가(신규 생성 0600 + 잔존물은 0700 디렉터리 뒤).

### 반증 시도 기록
1. "SQLite 버전 차이로 번들 빌드는 0644로 만들 것" → 번들 3.53.2 소스에서 상속 코드 직독으로 부정(46651-46687, 45162-45173). python 3.53.4 실측과 일치.
2. "크래시 잔존 사이드카" → D 실측: 재사용 시 퍼미션 유지는 맞으나 pre-fix 잔존물뿐 + 0700 dir로 차단 + 재생성 시 0600. 유출 경로 부재.
3. "umask 022에서 여전히 노출" → A 실측으로 부정(0600 생성).
4. "호출자 중 상대경로/parent='' 변종" → production 호출자는 damond.rs:132 단일, 절대경로.

---

## [ChanService] 판정: REFUTE

### 재검증 (직접 읽은 코드 경로)
- src/channel.rs:299-345 `session_for`: 패스트패스는 자기 엔트리를 리프레시 후 반환(스스로는 에빅트 안 됨). `map.retain`(307-311)은 **미스 시에만** 실행. → 단일 채팅 봇(개인 봇 일반 형태)은 엔트리가 항상 존재해 에빅션 경로 자체에 도달하지 않음.
- src/api.rs:119-148: retention 스윕은 `if let Some(days)`일 때만 스폰. src/config.rs:49-50: `Unset = keep all` 문서화.
- #212 원문(audit-findings-2026-09-19.md:212-216): "**chat_sessions와 데몬 세션이** 삭제 경로 없이 무한 증가 … 각 세션의 히스토리는 daemon Store에 계속 적립 → **메모리/디스크 무한 증가**". 수정 제안: "idle 타임아웃 후 session/delete + 캐시 제거 또는 LRU 캡".
- channel.rs:361-385: stale-session 재시도 — "session not found" 시 매핑 드롭 + 신규 세션 투명 재개(retention 스윕이 지운 세션에 대한 **명시된 설계**).
- src/bin/damon-{telegram,discord,slack}.rs:54/57/53: 브리지는 별도 프로세스에서 `Bridge::new`로 기동 — `chat_sessions`는 메모리 전용 HashMap, **브리지 재시작마다 모든 채팅의 매핑이 무음 리셋**(패치 전부터의 동작).

### 시나리오 재구성: 연속성 상실 자체는 재구성 가능하나 결함 아님
- 트리거: 멀티채팅 봇 + 채팅 A 24h+ 유휴 + 타 채팅(또는 신규 채팅)의 메시지가 retain 실행 → A의 다음 메시지가 신규 세션으로. 관측 가능(봇이 맥락 잊음). 그러나:
  1. **#212 중복(디스크 축적 부분)**: 기본 설정 누적 증상은 #212 원문 시나리오 그 자체("daemon Store에 계속 적립 → 디스크 무한 증가"). 리미디에이션은 메모리는 24h 에빅션, 디스크는 옵트인 `session_retention_days`(기본 keep-all은 문서화된 정책)로 응답. 같은 파일+같은 증상 → 중복 폐기 대상.
  2. **연속성 무음 리셋은 비의도가 아님**: (i) 24h 아이들 에빅션은 #212의 **승인된 수정 처방 그 자체**("idle 타임아웃 후 … 캐시 제거")의 구현이며, 패치는 처방의 delete 옵션보다 데이터를 보존하는 완화 변형; (ii) 동일한 "무음 신규 세션" UX가 이미 설계에 존재(retention 스윕 삭제 세션 → stale 재시도의 투명 재개); (iii) 매핑은 원래 메모리 전용 — 브리지 재시작 시 전 채팅 무음 리셋이 패치 전 동작이고 1차 감사에서도 결함으로 분류되지 않음; (iv) 단일 채팅 배포는 에빅션 미도달.
  3. 주석의 "the daemon's own sweep collects it"(322-324)이 기본 설정(keep-all)에서 사실이 아닌 것은 **주석 부정확성**이지 관측 가능 오동작 아님 — keep-all에서 행 보존이 문서화된 정책.
- "고아 세션에 delete_session 누락" 대칭 논거(318-328의 이중생성 경로는 delete 호출) 반박: 이중생성 경로의 delete는 **같은 시점 만들어진 즉시 미사용 세션** 정리(경합으로 버려진 방금 만든 세션), 에빅션 경로는 **사용 이력 있는 세션의 수명 정책** — 후자를 daemon retention에 위임한 것은 주석이 명시하는 의식적 설계("policy unchanged").

### 반증 시도 기록
1. "연속성 계약/매핑 영속화 경로가 있는가" → 부재(메모리 전용, 재시작마다 리셋이 원래 동작).
2. "신규 세션 생성 시 사용자 통지가 있는가(무음성 반증)" → allow/deny 외 커맨드 없음, 신규 세션 알림은 어떤 경로(stale 재시도 포함)에도 없음 — 무음 리셋이 전경로 공통 UX.
3. "#212와 증상이 다른가" → 디스크 축적은 동일 증상(원문 명시). 연속성은 다른 증상이지만 위 2-(ii)의 설계 일관성으로 비의도성 성립 불가.
4. "에빅션이 실제로 맵을 바운딩하는가(다른 결함 가능성)" → 활성 채팅 수로 유계, 턴 >24h 현실 경로 부재(원 보고 자체 기각). 범위 외.

---

## 최종 집계
1. **승인 목록**: 없음(2건 모두 폐기).
2. **폐기 목록**:
   - StoreAudit2(WAL 사이드카 world-readable): 원 보고의 사실 전제가 damond 실제 순서에서 거짓 — 번들 SQLite 3.53.2는 -wal/-shm을 DB 파일의 현재(이미 0600으로 조인) 퍼미션으로 생성(소스 직독 + umask 022 실측 0600). 원 보고 재현은 chmod 순서가 코드와 반대. 잔존 사이드카(크래시) 사례는 0700 데이터 디렉터리로 차단.
   - ChanService(24h 에빅션 고아+연속성 상실): 디스크 축적은 #212 원문 증상과 동일(중복) + keep-all은 문서화된 기본 정책; 연속성 무음 리셋은 #212의 승인된 수정 처방(idle 타임아웃 캐시 제거)의 직접적 결과이자 기존 설계(retention 삭제 세션의 투명 재개, 브리지 재시작 시 전체 리셋)와 일관된 동작. 주석 부정확성은 동작 결함 아님.
