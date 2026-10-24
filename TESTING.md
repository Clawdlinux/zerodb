# Testing

Tests protect what users rely on. Every test costs maintenance, so each one has to earn its place.

## What gets a test

A test is worth keeping if a regression in it would break one of these:

- an acknowledged write, durability, or recovery;
- a count or number we report or publish;
- fairness between systems, or the public protocol and API contract;
- a documented error or outcome a caller branches on.

These do not get tests:

- that a removed name or option is gone;
- the test machinery itself (fixtures matching docs, helper behavior);
- housekeeping such as scratch-directory pruning;
- private implementation choices reached by patching internals.

Fix those in code and review. Do not pin them with tests.

## How tests are written

1. **One scenario, many assertions.** Drive a realistic path once, then assert everything it proves. A write, kill, reopen test checks durability, ordering, outcomes and counts in the same test. Do not write one test per property along the same path.
2. **Table-driven cases.** When the same steps run with different inputs, use one body and a table of cases. In Rust use a `for case in CASES` loop with a case name in every assertion message. In Python use `pytest.mark.parametrize`.
3. **Shared helpers.** Setup, fixtures and builders live in one support module. No copied setup between files.
4. **Public API first.** Test through the public API or the wire protocol. Inline unit tests (`#[cfg(test)] mod tests`) are only for private logic that is hard to reach from outside, such as parsers and arithmetic.
5. **Broad invariants belong to simulations.** The deterministic sims and property tests carry invariants across random schedules and faults. Example tests pin specific protocols and edge cases the sims cannot target.
6. **Regression fixes extend existing tests.** A bug fix adds a case row or an assertion to the nearest existing test. Add a new test only when no existing scenario reaches the bug.
7. **Names describe behavior.** Files and tests are named by what they protect. Never by review round, version or ticket (`review_*`, `*_v2`).
8. **Slow tests are opt-in.** Mark them `#[ignore = "reason"]` in Rust or gate them with a marker in Python, and run them in a separate CI job.

## Layout

Rust, per crate:

```text
tests/it/main.rs       one integration test binary, declares the modules
tests/it/support.rs    shared helpers
tests/it/<area>.rs     one module per behavior area
```

One binary per crate keeps link time down. Each file under `tests/` is otherwise its own binary.

Python:

```text
tests/conftest.py      shared fixtures
tests/helpers.py       shared builders and fake workers
tests/test_<area>.py   one file per behavior area
```

## Running

```sh
cargo test -p zerodb-tape --test it recovery::     # 1 behavior module
cargo test --workspace -- --ignored                # the slow checks, in debug
```

The `sim` and `engine_sim` sims stay outside `tests/it` as their own test targets, so CI can call them by name.
Debug builds skip slow filesystem tests and seeded tape schedules. The release CI step runs them.

## Simulations

The sim commands in the [README](README.md#build-and-test) run 2,000 seeds.
Both sims vary disk headroom around the free-space floor and release space before retrying.
Each seed chooses the capacity phase's generation, covering fresh and recovered writers.
Floor rejections must issue no segment writes, including unpublished frames.
The sims inject partial-write and sync `StorageFull` errors alongside crash cuts.
The oracles check rejection state, read-only recovery, and acknowledged durability after reopening.
The engine sim also checks dedup state and verifies rejected-key retries commit exactly once.
Failures print the seed, the capacity plan and the command that reproduces the run.

## Power loss

`tests/crash/power_loss.py` runs the release binary on [LazyFS](https://github.com/dsrhaslab/lazyfs), a FUSE filesystem that holds unsynced writes in memory.
Killing LazyFS drops them, as a power cut does. Each round cuts power at a random moment or at a chosen syscall, such as before a segment `fsync` or after a `MANIFEST` rename.
After each cut it runs `zerodb verify` on the raw directory, reopens, and checks that every acknowledged turn is back at its sequence, sequences are dense, and no batch is partial.

The `deep` workflow runs it nightly with 60 cuts, and on pull requests that touch `zerodb-tape/src` with 20. It needs Linux and FUSE:

```sh
tests/crash/build-lazyfs.sh ~/lazyfs
ZERODB_CRASH_SEED=<seed> python3 tests/crash/power_loss.py --lazyfs ~/lazyfs
```

The build script pins LazyFS and adds `statfs`. Without it FUSE reports 0 free bytes, and zerodb's disk-full floor keeps it read-only.

LazyFS passes metadata calls such as `rename` straight to the disk below, so it cannot catch a missing directory sync. It has no `fallocate`, so segments use the zero-fill fallback there.
The same nightly job runs both sims on 20,000 new seeds each night.

## Before adding a test

Look for an existing scenario that already drives the path. Extend it. If you add a test, check whether two old ones can now merge.
