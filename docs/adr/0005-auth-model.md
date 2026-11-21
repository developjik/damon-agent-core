# ADR-0005: 인증 모델 — loopback 개방, 원격은 fail-closed + 1회용 티켓

- Status: Accepted
- Date: `87bc6d5`(fail-closed), `8086839`/`5340d21`(티켓 인증), `0b8eb5b`(E2E 핸드셰이크) — 감사 리메디에이션 `7e39862`으로 마무리

## Context

데몬의 기본 바인드는 `127.0.0.1:9470`(`config.rs` 기본값)이지만,
relay로 외부에 열리거나 사용자가 non-loopback으로 바인드할 수 있다.
동시에 세 가지 위협이 실제로 처리돼야 했다:

1. **토큰 유출 경로**: URL 쿼리의 토큰은 로그·프록시에 남는다.
2. **fail-open 부팅**: non-loopback 바인드에서 auth_token 해석이
   실패하면 열린 상태로 뜨면 안 된다.
3. **릴레이 도청**: 공용 `damon-relay`를 통과하는 프레임이 평문이면
   릴레이 운영자(와 ws:// 관찰자)가 다 읽는다.

## Decision

- **loopback + 토큰 미설정 → 개방**; 그 외는 `Authorization: Bearer`
  또는 `POST /v1/ws_ticket`이 발행하는 **단일 사용·TTL 제한 티켓**.
  쿼리스트링 토큰은 거부한다(`docs/protocol-v2.md` Auth 절).
- **non-loopback은 resolvable `auth_token` 없이 부팅 거부** —
  `SecretRef`(`env:`/`keychain:`/`!cmd`)로 외부 해석하고 TTL 캐시,
  해석 실패는 fail-closed(`87bc6d5`, `api.rs` `auth_token_cached`,
  `config.rs` `SecretRef`).
- **릴레이 경로는 E2E**: X25519 임시 키 교환 후 client-first 증명
  (클라이언트가 `sha256(token‖client_pub‖daemon_pub)`을 먼저 증명 —
  데몬이 미인증 피어에 토큰 유도물을 노출하지 않게), 이후 프레임은
  방향 분리 키의 AES-256-GCM, 시퀀스 번호가 AEAD nonce 겸 replay
  방지(`src/relay.rs` 헤더). 등록 secret은 첫 프레임 바디로 — URL
  쿼리 금지(`ff4277a`).
- **웹 디바이스는 개별 크레덴셜**: pairing 승인 → 디바이스별
  deviceToken(폐기 가능)으로 티켓 발급 — 마스터 bearer는 유지
  (`protocol-v2.md` Auth 절, `store.rs` `web_pairing`/`web_devices`).

## Consequences

- 솔직한 잔여 위험이 문서화돼 있다: 저엔트로피 auth_token은 캡처된
  핸드셰이크 하나로 오프라인 무차별 가능 — 대응은 고엔트로피 토큰,
  wss://, 그리고 opt-in 스트레칭 증명(`"kdf": "s256"` 2^16라운드,
  자동 협상·구형 피어 폴백)이라고 `src/relay.rs` 헤더가 명시한다.
- 티켓은 살아있는 소켓이 아니라 발행 레코드에 묶여, 디바이스 폐기 시
  미사용 티켓도 함께 purge된다(`api.rs` `ws_tickets` 주석).
- 채널 표면에서도 같은 권한 체인이 적용된다 — channel 권한 응답의
  인가 수정이 `e00b626`에 별도 커밋으로 남아 있다.

## 근거 (커밋·경로)

- `87bc6d5` fix: fail closed on auth_token resolution; bound secret commands
- `8086839` feat(api): ws_ticket auth, rate limiting, metrics, origin checks
- `5340d21` feat(rpc): live_prompts drop-guard, session/delete cancel, ticket auth
- `0b8eb5b` feat(relay): client-first E2E handshake; stale-tunnel reclaim
- `ff4277a` feat: relay registration secret + tunnel hardening
- `e00b626` fix: channel permission authz, turn lifecycle, UTF-8 slicing
- `7e39862` fix(core): remediate 2026-09-19 full-audit findings
- `src/api.rs` — `WsTicket`, `ws_tickets`, `AuthTokenCache`, `/v1/ws_ticket` 라우트
- `src/config.rs` — `SecretRef`, `auth_token`, 기본 바인드 `127.0.0.1:9470`
- `src/relay.rs` — `E2e`, 핸드셰이크·KDF 스트레칭 설명 헤더
- `docs/protocol-v2.md` — Auth 절, Web devices 절
