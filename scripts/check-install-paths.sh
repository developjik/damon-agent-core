#!/usr/bin/env bash
# Check that every install path serves the expected version. Run after a
# release (see docs/release.md §7):
#
#   scripts/check-install-paths.sh 0.9.0
#
# Probes: npm registry, crates.io, the Homebrew tap formula, and a real
# `pip install -e python` into a throwaway venv. Exits non-zero if any
# checked path mismatches; paths whose tooling is absent are skipped.
set -uo pipefail

version="${1:-}"
[ -n "$version" ] || { echo "usage: $0 <version>   (e.g. $0 0.9.0)" >&2; exit 1; }
version="${version#v}"
root="$(cd "$(dirname "$0")/.." && pwd)"
tap_repo="${TAP_REPO:-developjik/homebrew-tap}"
fail=0

check() { # name expected actual
  if [ "$2" = "$3" ]; then echo "  ok   $1 = $3"
  else echo "  FAIL $1 = ${3:-<none>} (want $2)"; fail=1; fi
}

echo "== npm (damon-agent)"
if command -v npm >/dev/null 2>&1; then
  check npm "$version" "$(npm view damon-agent version 2>/dev/null)"
else
  echo "  skip (npm not installed)"
fi

echo "== crates.io (damon-core)"
crate_v="$(curl --proto '=https' --tlsv1.2 -fsSL -H 'User-Agent: damon-install-check' \
  "https://crates.io/api/v1/crates/damon-core" 2>/dev/null \
  | grep -o '"max_stable_version":"[^"]*"' | head -1 | cut -d'"' -f4)"
check crates.io "$version" "$crate_v"

echo "== homebrew tap ($tap_repo)"
formula_v="$(gh api "repos/$tap_repo/contents/Formula/damon.rb" --jq .content 2>/dev/null \
  | base64 -d | grep -m1 '^  version ' | cut -d'"' -f2)"
check tap-formula "$version" "$formula_v"
if command -v brew >/dev/null 2>&1; then
  info="$(brew info --json=v2 "developjik/tap/damon" 2>/dev/null)"
  if command -v jq >/dev/null 2>&1; then
    brew_v="$(printf '%s' "$info" | jq -r '.formulae[0].versions.stable // empty' 2>/dev/null)"
  else
    brew_v="$(printf '%s' "$info" | grep -o '"stable": *"[^"]*"' | head -1 | cut -d'"' -f4)"
  fi
  [ -n "$brew_v" ] && check brew-info "$version" "$brew_v" \
    || echo "  skip (tap not added locally — brew info needs 'brew tap developjik/tap')"
fi

echo "== pip install -e python (source)"
if command -v python3 >/dev/null 2>&1; then
  venv="$(mktemp -d)/venv"
  if python3 -m venv "$venv" 2>/dev/null; then
    py_v="$("$venv/bin/pip" install -q -e "$root/python" 2>/dev/null \
      && "$venv/bin/python" -c 'import damon_agent; print(damon_agent.__version__)' 2>/dev/null)"
    check pip-e "$version" "$py_v"
  else
    echo "  skip (python3 -m venv failed)"
  fi
  rm -rf "$(dirname "$venv")"
else
  echo "  skip (python3 not installed)"
fi

echo
if [ "$fail" = 0 ]; then echo "all install paths serve $version"
else echo "SOME PATHS DO NOT SERVE $version" >&2; exit 1; fi
