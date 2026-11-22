#!/usr/bin/env bash
# Regenerate the Homebrew formula for a Damon release and sync it to the
# tap repository (developjik/homebrew-tap). Nothing is hardcoded: every
# sha256 is computed from the published release tarballs and cross-checked
# against the CI-uploaded .sha256 sidecars.
#
#   scripts/update-tap-formula.sh v0.9.0             # compute + push to tap
#   scripts/update-tap-formula.sh v0.9.0 --out damon.rb   # also write file
#   scripts/update-tap-formula.sh v0.9.0 --print     # render to stdout only
#
# Push uses `gh api` (repo contents endpoint), so no tap checkout is
# needed — only a gh login (or GH_TOKEN env) with write access to
# $TAP_REPO. Override the tap with TAP_REPO=owner/repo.
set -euo pipefail

usage() { sed -n '2,13p' "$0" >&2; exit 1; }

tag=""
out=""
do_print=0
do_push=1
for a in "$@"; do
  case "$a" in
    --print) do_print=1; do_push=0 ;;
    --no-push) do_push=0 ;;
    --out) ;; # value consumed below
    -h|--help) usage ;;
    *) if [ -z "$tag" ]; then tag="$a"; elif [ -z "$out" ] && [ "${prev:-}" = "--out" ]; then out="$a"; else usage; fi ;;
  esac
  prev="$a"
done
[ -n "$tag" ] || usage
case "$tag" in v*) ;; *) tag="v$tag" ;; esac
version="${tag#v}"

repo="developjik/damon-agent-core"
tap_repo="${TAP_REPO:-developjik/homebrew-tap}"
base="https://github.com/$repo/releases/download/$tag"
targets="aarch64-apple-darwin x86_64-apple-darwin x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu"

sum_tool() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "==> computing sha256 from $base tarballs" >&2
for target in $targets; do
  # Release legs upload assets in parallel — retry for the upload race
  # and for a stale CDN edge, like the CI formula step does.
  for attempt in 1 2 3 4 5; do
    curl --proto '=https' --tlsv1.2 --retry 20 --retry-delay 10 --retry-all-errors -fsSL \
      "$base/damon-$target.tar.gz" -o "$tmp/$target.tar.gz" && break
    [ "$attempt" = 5 ] && { echo "cannot fetch damon-$target.tar.gz" >&2; exit 1; }
    sleep 10
  done
  computed="$(sum_tool "$tmp/$target.tar.gz")"
  if curl --proto '=https' --tlsv1.2 --retry 5 --retry-all-errors -fsSL \
      "$base/damon-$target.tar.gz.sha256" -o "$tmp/$target.sidecar" 2>/dev/null; then
    published="$(cut -d' ' -f1 < "$tmp/$target.sidecar")"
    [ "$computed" = "$published" ] || {
      echo "sha256 mismatch for $target: computed $computed, sidecar $published" >&2; exit 1;
    }
  else
    echo "    (no .sha256 sidecar for $target — trusting computed value)" >&2
  fi
  echo "    $target $computed" >&2
  eval "sha_$(echo "$target" | tr - _)=\"$computed\""
done

formula="$tmp/damon.rb"
cat > "$formula" <<RUBY
class Damon < Formula
  desc "Local multi-provider agent daemon"
  homepage "https://github.com/$repo"
  version "$version"
  license "MIT OR Apache-2.0"

  on_macos do
    on_arm do
      url "$base/damon-aarch64-apple-darwin.tar.gz"
      sha256 "$sha_aarch64_apple_darwin"
    end
    on_intel do
      url "$base/damon-x86_64-apple-darwin.tar.gz"
      sha256 "$sha_x86_64_apple_darwin"
    end
  end
  on_linux do
    on_intel do
      url "$base/damon-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "$sha_x86_64_unknown_linux_gnu"
    end
    on_arm do
      url "$base/damon-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "$sha_aarch64_unknown_linux_gnu"
    end
  end

  def install
    bin.install "damond"
    bin.install "damon"
    bin.install "damon-telegram"
    bin.install "damon-discord"
    bin.install "damon-slack"
    bin.install "damon-relay"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/damond --version")
    assert_match version.to_s, shell_output("#{bin}/damon --version")
  end
end
RUBY

if [ "$do_print" = 1 ]; then
  cat "$formula"
  exit 0
fi
[ -n "$out" ] && cp "$formula" "$out" && echo "==> wrote $out" >&2

if [ "$do_push" = 1 ]; then
  echo "==> pushing Formula/damon.rb to $tap_repo" >&2
  sha="$(gh api "repos/$tap_repo/contents/Formula/damon.rb" --jq .sha 2>/dev/null || true)"
  payload=( -f message="damon $version" -f content="$(base64 < "$formula" | tr -d '\n')" -f branch=main )
  [ -n "$sha" ] && payload+=( -f sha="$sha" )
  gh api "repos/$tap_repo/contents/Formula/damon.rb" -X PUT "${payload[@]}" --jq '.commit.sha' \
    | sed 's/^/    tap commit /' >&2
fi
