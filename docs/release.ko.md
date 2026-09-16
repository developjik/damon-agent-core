# 릴리스 체크리스트

`v*` 태그 푸시 → CI가 4개 타겟 빌드 → GitHub Releases tarball → npm + crates.io publish.
아래는 사람이 직접 해야 하는 것들.

## 1. 시크릿 등록 (repo Settings → Secrets and variables → Actions)

| 시크릿 | 발급처 | 비고 |
| --- | --- | --- |
| `CARGO_REGISTRY_TOKEN` | crates.io → Account Settings → API Tokens | `cargo publish`용 |
| `NPM_TOKEN` | npmjs.com → Access Tokens → Granular Access Token | publish 권한. **2번(OIDC)을 택하면 불필요** |

`GITHUB_TOKEN`은 npmjs.com 인증 불가 (GitHub Packages 전용).

## 2. npm publish 방식 선택

### A. 토큰 방식 (지금 워크플로우 그대로)
`NPM_TOKEN` 등록만 하면 끝.

### B. Trusted Publishing (OIDC, 토큰 없음) — 권장
1. **첫 버전은 수동 발행** (패키지가 npm에 존재해야 trusted publisher 설정 가능):
   ```sh
   cd npm && npm login && npm publish --access public
   ```
2. npmjs.com → `damon-agent` 패키지 → Settings → Trusted Publisher:
   - repo: `developjik/damon-agent-core`, workflow: `ci.yml`
3. `ci.yml`의 `publish-npm` 잡 수정:
   - `permissions: id-token: write` 추가
   - `node-version: "24"` (npm ≥ 11.5.1 필요 — node 20은 npm 10)
   - `registry-url`, `NODE_AUTH_TOKEN` 제거
   - `npm publish --access public --provenance`

## 3. 버전 동기화 (태그 전 필수)

3곳이 태그와 일치해야 한다. 스크립트로 한 번에:

```sh
scripts/set-version.sh 0.1.0
```

갱신 대상:

- `Cargo.toml` → `version`
- `npm/package.json` → `version` (install.js가 이 값으로 tarball URL 생성)
- `Formula/damon.rb` → `version` + 다운로드 URL

## 4. 태그 푸시

```sh
git tag v0.1.0 && git push origin v0.1.0
```

## 5. Homebrew tap (별도 작업)

1. `developjik/homebrew-tap` 리포 생성
2. `Formula/damon.rb`를 tap 리포 `Formula/`로 복사
3. 릴리스 후 각 tarball의 실제 sha256으로 교체:
   ```sh
   curl -fsSL https://github.com/developjik/damon-agent-core/releases/download/v0.1.0/damon-aarch64-apple-darwin.tar.gz | shasum -a 256
   ```
4. 설치: `brew install developjik/tap/damon`

## 알려진 갭 (배포 전 결정 필요)

- `Formula/damon.rb`의 `sha256`은 릴리스마다 손으로 채운다(5절) — 이
  과정을 빠뜨린 태그는 체크섬 불일치로 실패한다.
- `damon-relay`는 tarball, `bin.install`, npm `bin`, `install.js`의
  `expected` 목록 모두에 포함됨 — 바이너리를 추가/이름 변경할 때 네
  곳을 함께 유지할 것.

## 향후 개선 후보 (초기 배포 리뷰에서)

- 릴리스마다 `SHA256SUMS` 포함 + `install.js`에서 검증 (현재 무결성
  검증 없음).
- CI 타겟 추가: `aarch64-unknown-linux-gnu`(`ubuntu-24.04-arm` 러너),
  `damon-relay` VPS 배포용 musl/Docker 이미지.
- rmcp `transport-streamable-http-client-reqwest` 피처로 원격 MCP 서버
  지원 (현재 stdio만).
- WS 프로토콜 JSON Schema, `Provider` trait 개방 (현재 closed enum).
- `Cargo.toml` `homepage`/`documentation` 필드, CHANGELOG.
