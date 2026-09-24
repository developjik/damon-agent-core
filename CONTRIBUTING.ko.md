# Contributing

## 개발

```sh
cargo test                              # 전체 테스트
cargo run --bin damond                  # 데몬 실행
cargo run --release --example bench     # 성능 기준선
```

## 원칙

- 공개 API 안정성: 한 번 나간 wire 메서드나 설정 필드는 깨지지 않는다.
  추가 변경만 허용하고, 제거가 필요하면 프로토콜을 버전업한다.
- 시크릿은 코드나 설정 파일에 두지 않는다 — `env:`, `keychain:`, `!cmd`
  참조만 허용. 리터럴 API 키는 로드 시 거부된다.
- 성능 회귀는 버그다 — 스트리밍 경로를 가볍게 유지하고, 핫패스 변경은
  PR에 명시한다.
- 기존 컨벤션을 따른다 — 동작하는 패턴 옆에 두 번째 패턴을 추가하지 않는다.

## DCO

모든 커밋에 `Signed-off-by` 트레일러를 단다 (`git commit -s`):

```
Signed-off-by: Your Name <you@example.com>
```

이는 해당 기여가 본인의 것이며 프로젝트 라이선스(MIT/Apache-2.0)로 제출됨을 인증한다.
