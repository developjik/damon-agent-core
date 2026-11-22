# 로드맵

[English](roadmap.md) | **한국어**

열린 기능 이슈 트리아지(2026-11-01 등록분, 2026-11-22에 트리와 대조 —
이슈별 근거는 각 이슈의 코멘트 참조). **v0.9.0은 현재 `Unreleased`
체인지로그만 출시하며 아래 이슈는 그 범위에 없다.** 항목은 테마별로
서로를 해금하는 순서로 묶었다.

## 부분 구현 (먼저 마무리)

| 이슈 | 출시됨 | 잔여 | 제안 |
| --- | --- | --- | --- |
| [#29](https://github.com/developjik/damon-agent-core/issues/29) 결정론 DAG 오케스트레이터 | 미션 러너 (0.7.0): 플로 검증, 무LLM 스케줄링, 재시도, 저널된 전이, 크래시 정합성 | 내용 연쇄 저널, 타입 데이터 링크, 재계획 이벤트, 결정론 테스트, 문서 | v0.10.0 |
| [#15](https://github.com/developjik/damon-agent-core/issues/15) 4상태 머신 + `session.waitUntil` | `session.status`의 `phase`(`idle`/`busy`/`awaiting_input`), `session.watch`, `capabilities.sessionPhase` | `done` 상태, `waitUntil` RPC, 와이어 롤업, 미션 노드 피어 대기 | v0.10.0 |
| [#18](https://github.com/developjik/damon-agent-core/issues/18) 워크트리 팬아웃 | `worktree.create/list/remove`, PR 체크아웃, 진행 이벤트 + 취소 | 팬아웃 프리미티브, 훅, 세션 매니저 수명주기, 브랜치별 자동 PR | v0.11.0 |
| [#30](https://github.com/developjik/damon-agent-core/issues/30) 2계층 컴팩션 | `compacted_through` + 저널된 컴팩션 항목 | `t<N>` 스텁, `session.contextQuery`, 트리거 스케줄, 정리 | v0.11.0 |

## 미착수 — 기반 항목 우선

나머지 대부분이 이 위에 올라가는 순서다.

- [#14](https://github.com/developjik/damon-agent-core/issues/14) SCM_RIGHTS 데몬 핸드오프 → [#22](https://github.com/developjik/damon-agent-core/issues/22) PTY 홀더 사이드카를 해금
- [#19](https://github.com/developjik/damon-agent-core/issues/19) 실행 리스 풀 + 지속 continuation id → [#18](https://github.com/developjik/damon-agent-core/issues/18) 팬아웃과 [#28](https://github.com/developjik/damon-agent-core/issues/28) 클레임 리스를 제한
- [#26](https://github.com/developjik/damon-agent-core/issues/26) 워크트리 경화(훅 억제, 루트 제한 리졸버, GC) → 팬아웃의 안전 바닥
- [#20](https://github.com/developjik/damon-agent-core/issues/20) 검증자 게이트 — 독립적이고 이슈 스스로 "가장 먼저 착지할 항목"으로 표기
- [#33](https://github.com/developjik/damon-agent-core/issues/33) 샌드박스 드라이버 (bwrap/seatbelt/docker)

## 미착수 — 결정 선행 필요

- [#17](https://github.com/developjik/damon-agent-core/issues/17) 범용 ACP 어댑터 — registry.rs는 구 ACP 카탈로그가 *의도적으로 제거*됐음을 기록한다. 재도입은 그 결정을 뒤집으므로 ADR 선행이 필요하다.

## 미착수 — 독립 기능

| 이슈 | 테마 |
| --- | --- |
| [#21](https://github.com/developjik/damon-agent-core/issues/21) | 벤더 쿼터/레이트리밋 스케줄러 |
| [#23](https://github.com/developjik/damon-agent-core/issues/23) | zstd 콜드 스크롤백 계층 |
| [#24](https://github.com/developjik/damon-agent-core/issues/24) | 이축 라이프사이클/PR 폴러 |
| [#25](https://github.com/developjik/damon-agent-core/issues/25) | 2단 승인 감독자 |
| [#27](https://github.com/developjik/damon-agent-core/issues/27) | AST 네비게이션 래더 (tree-sitter) |
| [#28](https://github.com/developjik/damon-agent-core/issues/28) | 경로 소유 매트릭스 + 충돌 알람 |
| [#31](https://github.com/developjik/damon-agent-core/issues/31) | 퍼지 패치 엔진 + AST 사전검증 |
| [#32](https://github.com/developjik/damon-agent-core/issues/32) | 멀티모델 캐스케이드/크리티크 라우터 |

마일스톤 일자는 없다 — 위 순서는 의존 관계이며 각 이슈의 수용 기준이
완료 기준이다.
