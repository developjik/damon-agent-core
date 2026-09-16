# Contributing

**English** | [한국어](CONTRIBUTING.ko.md)

## Development

```sh
cargo test                              # full test suite
cargo run --bin damond                  # run the daemon
cargo run --release --example bench     # performance baseline
```

The repo pins a toolchain via `rust-toolchain.toml` (stable + rustfmt +
clippy); rustup picks it up automatically.

## Principles

- Public API stability: once a wire method or config field ships, it must
  not break. Additive changes only; version the protocol if you must
  remove something.
- Secrets never live in code or config files — `env:`, `keychain:`, or
  `!cmd` references only. Literal API keys are rejected at load time.
- Performance regressions are bugs — attach `cargo run --release
  --example bench` output to PRs that touch the streaming path.
- Match existing conventions; don't add a second pattern beside a working
  one.

## DCO

Every commit carries a `Signed-off-by` trailer (`git commit -s`):

```
Signed-off-by: Your Name <you@example.com>
```

This certifies the contribution is yours and submitted under the project
license (MIT/Apache-2.0).
