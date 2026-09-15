# Damon

daemon의 말장난이자 실제 아키텍처. 내 맥에서 항상 떠 있는, 내 전용 멀티 프로바이더 에이전트 코어.

하나의 코어를 잘 만들고, 나머지는 전부 얇은 클라이언트로 가져다 쓴다.

## 구조

```
[web] [CLI] [telegram] [browser ext] [mobile-relay]   전부 thin client
                      |
                 단일 API (HTTP/WS)
                      |
               Damon core (local daemon)
                ├─ provider 어댑터 (OpenAI 호환 + ACP)
                ├─ 세션 / 메모리 저장소
                └─ 툴 시스템 (고래 지도 파이프라인, 쉼표 ECOS, TradingView 워크플로 연결)
```

## 원칙

1. 코어가 먼저, 서피스는 나중. 서피스부터 만들면 죽는다.
2. 코어는 단일 API로만 노출한다. 그래야 "여러 군데서 가져다 쓰기"가 공짜가 된다.
3. API 키는 로컬 키체인에만 둔다. 코드와 저장소에는 절대.
4. 프로토콜은 표준을 재사용한다(OpenAI 호환 스키마, ACP). 어댑터 레이어를 직접 발명하지 않는다.

## 미결정 사항 (Phase 0에서 확정)

- provider 층위: LLM API 수준인가, coding-agent harness 수준인가, 둘 다인가
- 첫 서피스: 웹 / CLI / 텔레그램 중 하나만 먼저

## 로드맵

- Phase 0: 스펙 한 장, 게이트 2개 확정
- Phase 1: 코어 데몬 부팅, 프로바이더 어댑터 1종, 헬스체크 엔드포인트
- Phase 2: 메모리 + 툴 시스템
- Phase 3: 첫 서피스 1종
- Phase 4: 채널 확장 + 원격 릴레이(E2E)
