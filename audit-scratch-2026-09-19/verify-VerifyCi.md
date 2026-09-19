# Phase 2 적대적 검증 — VerifyCi (CI/설정 후보 2건, 2026-09-19)

## [CI-1] audit 잡 job-level permissions → contents:none (ci.yml:36-37) 판정: REFUTE
- 재검증:
  - .github/workflows/ci.yml:36-37 `permissions: checks: write` (job-level), 워크플로 floor는 ci.yml:9-10 `contents: read`. 패치로 추가된 블록(diff hunk 확인).
  - GitHub 공식 문서(workflow-syntax `permissions` 항): "If you specify the access for any of these permissions, all of those that are not specified are set to none" → audit 잡 토큰은 contents:none. **보고자의 권한 의미론 주장은 사실로 확증.**
  - 퍼블릭 여부: 무인증 API/릴리스 다운로드 성공(curl, jq로 확인) — 리포는 public이며 install.js/Formula가 무토큰 공개 다운로드를 전제로 설계됨.
  - **결정적 반증 실험**: rustsec/cargo-audit(감사 액션 상류 프로젝트) 자체 `security_audit.yml`이 `permissions: {}`(전 스코프 none, contents 포함) + actions/checkout@v7 + audit-check를 public 리포에서 운영, 2026-09-18 실행 success(push/PR). → public 리포에서 contents:none 토큰으로 checkout은 실제로 성공.
  - 상류 문서 일치: rustsec/audit-check README "Granular Permissions" = `checks: write`(+`issues: write`는 cron 이슈 생성용 — 본 워크플로는 schedule 없음). 패치 구성은 상류 권장 패턴과 동일하며 contents:read를 명시하지 않는 것도 동일.
  - actions/checkout README는 일반 권고로 `contents: read`를 "recommended"로 기술 — 단 public 실패를 규정하지 않음(방어적 깊이 권고).
- 시나리오 재구성: 실패 경로는 "리포가 private 전환(또는 private 리포로 복사)"이라는 미래 가정 상태 필요. 현재 구성(public)에서 관측 가능한 오동작 없음 → 재구성 실패.
- 반증 시도 기록: (1) job-level 대체 의미론 — 문서로 확인, 보고자 주장과 일치(결함 지지 방향이나 실재성 요건 미충족); (2) public+contents:none checkout 실패 사례 탐색 — actions/checkout 이슈트래커/API 검색, 커뮤니티 스레드(#29019의 403은 private npm 패키지=packages:read 문제로 무관) — public checkout 실패 사례 없음, 상류 green run으로 작동 실증; (3) fork PR 경로 — fork에서는 permissions 키와 무관하게 GITHUB_TOKEN이 항상 읽기전용(패치 전에도 동일), audit-check는 check-run 생성 실패 시 stdout 폴백이 문서화됨(상류 README "Limitations") — 신규 실패 경로 아님.
- 폐기 사유: 현재 구성에서 결함 아님 + 상류 권장 패턴과 일치. "리포가 언젠가 private 되면"은 방어적 편향 폐기 사유. (비권고 기록: private 전환 시 contents: read 명시 필요 — actions/checkout README 권고.)

## [CI-2] Windows 릴리스 tarball .pdb 수납 (ci.yml:87-88) 판정: CONFIRM — 최종 심각도 Low
- 재검증 (전부 실물 산출물로 실증, 추론 없음):
  - 실제 v0.1.0 에셋 `damon-x86_64-pc-windows-msvc.tar.gz` (37,122,590 bytes) 다운로드·목록 확인: **.pdb 6개 수납** — damond.pdb 8,908,800 / damon.pdb 5,222,400 / damon_telegram.pdb 5,419,008 / damon_discord.pdb 5,459,968 / damon_slack.pdb 5,435,392 / damon_relay.pdb 4,493,312 = **34,938,880 bytes (비압축 페이로드의 ~35%)**.
  - 동일 설정 재팩 비교: `--exclude='*.pdb'` 추가 시 11,547,054 bytes 감소(아카이브의 ~24.5%) → Windows 사용자가 설치마다 약 9~11.5MB의 무용 심볼을 다운로드.
  - globs `damond* damon*`가 `*.pdb` 전부 매치(둘 다 "damon" 접두 glob). `--exclude='*.d'`만 존재, `*.pdb` 배제 없음(ci.yml:88).
  - 소비 경로: npm/install.js:28 URL → :38 tarball 다운로드(전체) → :114 `tar xzf -C bin` 멤버 필터 없음 → .pdb 6개(~35MB)가 사용자 node_modules package bin/에 디스크 상주. Formula(Homebrew)는 darwin/linux만 참조(ci.yml "Update Formula sha256" targets) — 미영향.
  - PDB 생성 조건: Cargo.toml(현행 및 v0.1.0)에 [profile.release] 부재(grep 0건) — 기본 프로파일로 msvc 태깅에서 PDB가 산출됨을 실물로 확인. [INFERENCE] 툴체인 기본 동작의 세부 메커니즘(어느 플래그가 /DEBUG를 유발하는지)은 미특정.
- 시나리오 재구성: 성공 — v* 태그 → windows-latest release 빌드 → damon*.pdb 산출(실물 목록으로 입증) → tar 글롭 포함(실물 아카이브로 입증) → npm 사용자 다운로드+디스크 비용(install.js 코드 경로로 입증).
- 기타 관찰(기록만, 미보고): v0.1.0엔 .d도 포함(--exclude='*.d'는 v0.1.0 이후 추가, 현행엔 있음); `damond* damon*` 이중 글롭이 damon.* 3파일을 하드링크 중복 엔트리로 재수납(0바이트 헤더, 무해).
- 정확성 정정: 원보고의 "v0.2.0 릴리스 산출물" → 실제 태그는 v0.1.0.
- 패치 소속: **pre-existing** — tar 라인은 패치에서 변경 없음(diff에서 context; HEAD:ci.yml:82와 동일). 다이제스트에 미기록 증상이라 신규 판정 대상으로 기록(집계자 판단 여지는 유지).
- 심각도: Low(대역폭/디스크 낭비만, 기능 결함 없음, Windows 한정). 수정 제안: `--exclude='*.d' --exclude='*.pdb'`.

## 오케스트레이터 확정 사항 재확인
- release 잡 ci.yml:48-49 `permissions: contents: write` 명시 확인 → SecDiff의 'release 권한 상실' 제기는 반증 유지(폐기 확정, 검증 대상 아님).

## 요약
1) 승인: CI-2 (.pdb 포함, Low, pre-existing — 패치 회귀 아님)
2) 폐기: CI-1 (audit permissions — public 리포에서 오동작 없음, 상류 권장 패턴 일치)
