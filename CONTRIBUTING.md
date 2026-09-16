# Contributing

## 개발

```sh
cargo test                              # 전체 테스트
cargo run --bin damond                  # 데몬 실행
cargo run --release --example bench     # 성능 기준선
```

## 원칙

README의 원칙 1-8을 따른다. 특히:

- 공개 API는 한 번 나가면 깨지지 않는다 (원칙 6)
- 시크릿은 코드/설정 파일에 두지 않는다 (원칙 3)
- 성능 회귀는 버그다 — PR에서 `cargo run --release --example bench` 결과를 첨부

## DCO

모든 커밋에 `Signed-off-by` 트레일러를 단다 (`git commit -s`):

```
Signed-off-by: Your Name <you@example.com>
```

이는 해당 기여가 본인의 것이며 프로젝트 라이선스(MIT/Apache-2.0)로 제출됨을 인증한다.
