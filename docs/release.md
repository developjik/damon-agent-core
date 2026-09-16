# Release Checklist

**English** | [한국어](release.ko.md)

Pushing a `v*` tag → CI builds 4 targets → GitHub Releases tarballs →
npm + crates.io publish. The items below are manual.

## 1. Register secrets (repo Settings → Secrets and variables → Actions)

| Secret | Where | Notes |
| --- | --- | --- |
| `CARGO_REGISTRY_TOKEN` | crates.io → Account Settings → API Tokens | for `cargo publish` |
| `NPM_TOKEN` | npmjs.com → Access Tokens → Granular Access Token | publish permission. **Unneeded if you choose OIDC (option 2)** |

`GITHUB_TOKEN` cannot authenticate to npmjs.com (GitHub Packages only).

## 2. Choose the npm publish method

### A. Token (current workflow as-is)
Register `NPM_TOKEN` and you're done.

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

Three places must match the tag. Run:

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

## Known gaps (decide before shipping)

- The formula's `sha256` values are placeholders — `brew install` fails
  until they're filled in after the first release.
- `damon-relay` is now in the tarball, `bin.install`, npm `bin`, and
  `install.js`'s `expected` list — keep all four in sync when binaries
  are added or renamed.
