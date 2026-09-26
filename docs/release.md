# Release Checklist

**English** | [한국어](release.ko.md)

Pushing a `v*` tag → CI builds 5 targets → GitHub Releases tarballs →
npm + crates.io publish. The items below are manual. The
`aarch64-unknown-linux-gnu` tarball is built natively on GitHub's
`ubuntu-24.04-arm` runner — no cross-toolchain involved.

## 1. Register secrets (repo Settings → Secrets and variables → Actions)

| Secret | Where | Notes |
| --- | --- | --- |
| `CARGO_REGISTRY_TOKEN` | crates.io → Account Settings → API Tokens | for `cargo publish` |
| `NPM_TOKEN` | npmjs.com → Access Tokens → Granular Access Token | publish permission. **Unneeded if you choose OIDC (option 2)** |
| `MINISIGN_SECRET_KEY` | optional | passwordless minisign/rsign secret key — signs the release `SHA256SUMS` (see §6) |

`GITHUB_TOKEN` cannot authenticate to npmjs.com (GitHub Packages only).


## 2. Choose the npm publish method

### A. Token + provenance (current workflow)
Register `NPM_TOKEN` and you're done — the job publishes with
`--provenance` (Sigstore attestation via `id-token: write`), so
`npm audit signatures` verifies the package.

### B. Trusted Publishing (OIDC, no token) — recommended
1. **Publish the first version manually** (the package must exist on npm
   before a trusted publisher can be configured):
   ```sh
   cd npm && npm login && npm publish --access public
   ```
2. npmjs.com → `damon-agent` package → Settings → Trusted Publisher:
   - repo: `developjik/damon-agent-core`, workflow: `ci.yml`
3. Edit the `publish-npm` job in `ci.yml`:
   - add `permissions: id-token: write`
   - `node-version: "24"` (needs npm ≥ 11.5.1 — node 20 ships npm 10)
   - remove `registry-url` and `NODE_AUTH_TOKEN`
   - `npm publish --access public --provenance`

## 3. Version sync (required before tagging)

Five places must match the tag. Run:

```sh
scripts/set-version.sh 0.1.0
```

which updates:

- `Cargo.toml` → `version`
- `npm/package.json` → `version` (install.js builds the tarball URL from it)
- `Formula/damon.rb` → `version` + download URLs

## 4. Push the tag

```sh
git tag v0.1.0 && git push origin v0.1.0
```

## 5. Homebrew tap (separate step)

1. Create the `developjik/homebrew-tap` repository
2. Copy `Formula/damon.rb` into the tap's `Formula/` directory
3. After the release, replace each tarball's sha256 with the real value:
   ```sh
   curl -fsSL https://github.com/developjik/damon-agent-core/releases/download/v0.1.0/damon-aarch64-apple-darwin.tar.gz | shasum -a 256
   ```
4. Install: `brew install developjik/tap/damon`

## 6. Release signing (SHA256SUMS + minisign)

Every tag publishes a `SHA256SUMS` manifest covering all five tarballs,
assembled on the release leg after each tarball was verified against its
build-computed `.sha256` sidecar. When `MINISIGN_SECRET_KEY` is
registered, the manifest is also signed (`SHA256SUMS.minisig`,
minisign-format Ed25519). Without the secret the manifest ships alone
and the job prints a warning.

One-time key setup (any machine with cargo):

```sh
cargo install rsign2
rsign generate -s minisign.key -p damon-minisign.pub -W   # -W: passwordless
```

1. Commit `damon-minisign.pub` at the repo root (verifiers need it;
   `scripts/verify-release.sh` looks for it there).
2. Store the contents of `minisign.key` as the `MINISIGN_SECRET_KEY`
   repo secret. Keep the private file off disk after that — the secret
   is the only copy the release pipeline needs.
3. `npm install` checks tarballs against `SHA256SUMS` (falling back to
   per-target sidecars on old releases). The minisign signature itself
   is verified by humans/automation:

```sh
scripts/verify-release.sh v0.4.0                 # checksums + signature when present
scripts/verify-release.sh v0.4.0 --require-signature    # CI mode: unsigned = fail
(pass an assets dir as the only other argument to re-verify a download)



## Known gaps (decide before shipping)

- `damon-relay` is now in the tarball, `bin.install`, npm `bin`, and
  `install.js`'s `expected` list — keep all four in sync when binaries
  are added or renamed.
- Until `MINISIGN_SECRET_KEY` is registered, releases ship SHA256SUMS
  unsigned; the workflow warns but does not fail.

## Future improvements (from the initial deploy review)

- musl/Docker images for `damon-relay` VPS deploys (the
  `aarch64-unknown-linux-gnu` target shipped on the `ubuntu-24.04-arm`
  runner).
