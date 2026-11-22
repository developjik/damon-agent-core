# Roadmap

**English** | [한국어](roadmap.ko.md)

Triage of the open feature issues (filed 2026-11-01, reviewed against
the tree on 2026-11-22 — see each issue for the per-item evidence
comment). **v0.9.0 ships the current `Unreleased` changelog only; none
of the issues below are in its scope.** Items are grouped by theme in
roughly the order they unblock each other.

## Partially implemented (finish first)

| Issue | Shipped | Remaining | Suggested |
| --- | --- | --- | --- |
| [#29](https://github.com/developjik/damon-agent-core/issues/29) deterministic DAG orchestrator | mission runner (0.7.0): validated flows, zero-LLM scheduling, retries, journaled transitions, crash honesty | content-chained journal, typed data links, re-plan event, determinism test, docs page | v0.10.0 |
| [#15](https://github.com/developjik/damon-agent-core/issues/15) 4-state machine + `session.waitUntil` | `phase` (`idle`/`busy`/`awaiting_input`) on `session.status`, `session.watch`, `capabilities.sessionPhase` | `done` state, `waitUntil` RPC, wire rollup, mission-node peer waits | v0.10.0 |
| [#18](https://github.com/developjik/damon-agent-core/issues/18) worktree fan-out | `worktree.create/list/remove`, PR checkout, progress + cancel | fan-out primitive, hooks, lifecycle in session manager, auto-PR per branch | v0.11.0 |
| [#30](https://github.com/developjik/damon-agent-core/issues/30) two-layer compaction | `compacted_through` + journaled compaction items | `t<N>` stubs, `session.contextQuery`, trigger schedule, cleanup | v0.11.0 |

## Not started — foundations first

These order the rest: most later items build on them.

- [#14](https://github.com/developjik/damon-agent-core/issues/14) SCM_RIGHTS daemon handoff → unblocks [#22](https://github.com/developjik/damon-agent-core/issues/22) PTY holder sidecar
- [#19](https://github.com/developjik/damon-agent-core/issues/19) execution lease pool + continuation ids → bounds [#18](https://github.com/developjik/damon-agent-core/issues/18) fan-out and [#28](https://github.com/developjik/damon-agent-core/issues/28) claim leases
- [#26](https://github.com/developjik/damon-agent-core/issues/26) worktree hardening (hook suppression, confined resolver, GC) → safety floor for fan-out
- [#20](https://github.com/developjik/damon-agent-core/issues/20) verifier gate — self-contained quality win, marked "first to land" in its own issue
- [#33](https://github.com/developjik/damon-agent-core/issues/33) sandbox driver (bwrap/seatbelt/docker)

## Not started — decision needed first

- [#17](https://github.com/developjik/damon-agent-core/issues/17) generic ACP adapter — registry.rs documents that the old ACP catalog was *removed* on purpose; reintroducing it reverses that decision and wants an ADR first.

## Not started — independent features

| Issue | Theme |
| --- | --- |
| [#21](https://github.com/developjik/damon-agent-core/issues/21) | vendor quota/rate-limit scheduler |
| [#23](https://github.com/developjik/damon-agent-core/issues/23) | zstd cold scrollback tier |
| [#24](https://github.com/developjik/damon-agent-core/issues/24) | dual-axis lifecycle/PR poller |
| [#25](https://github.com/developjik/damon-agent-core/issues/25) | two-tier approval supervisor |
| [#27](https://github.com/developjik/damon-agent-core/issues/27) | AST navigation ladder (tree-sitter) |
| [#28](https://github.com/developjik/damon-agent-core/issues/28) | path-ownership matrix + conflict alarms |
| [#31](https://github.com/developjik/damon-agent-core/issues/31) | fuzzy patch engine + AST pre-validation |
| [#32](https://github.com/developjik/damon-agent-core/issues/32) | multi-model cascade/critique router |

Milestone dates are not assigned — sequencing above reflects
dependency order, and each issue's acceptance criteria are the
completion bar.
