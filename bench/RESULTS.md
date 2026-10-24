# Latest result

Latest S2 macOS 192k run uses matched preallocation: Engine/bare p99 median **1.150713x**, Tape/bare **1.273602x**.
The earlier **1.662945x** Engine/bare and **1.211374x** Tape/bare medians used an unmatched sparse bare baseline.
Those figures are historical, not current performance results.
See [S2 disk-full overhead](#s2-disk-full-overhead-macos-192k) for historical runs and the corrected preallocation baseline.

Latest S1 macOS 1M run: Engine/bare p99 **1.189216x** [1.085449x, 1.675773x].
Tape/bare median is **1.021696x** [0.996803x, 1.310008x].
Both paired-ratio medians beat M2. Absolute p99 medians are higher, and Engine repetition 1 misses 1.5x.
See [the 1M review run](#s1-review-fixes-and-1m-g1-macos) for eviction evidence and cause analysis.
Linux NVMe and nightly LazyFS remain pending. This trace measures appends, not facts or forget.

Reader load note (2026-10-21): from this commit G1 defaults to `G1_SYMMETRIC=1` (D74).
Bare, Tape and Engine each run 2 concurrent recent-data readers.
Engine reads the 16 most recent turns of one session, not the 16 most recent records overall.
Short sessions return fewer turns. Below about 16k turns most windows hold fewer than 16.
Tape reads the last 16 durable LSNs. Bare reads the same bytes back with positional reads.
Each window line prints `records_per_query`. Compare window latency only at similar values.
Bare reads hit page cache and run many more queries. Check whether bare p99 moves between modes.
All earlier sections in this file used asymmetric load: only Engine ran readers.
Their ratios include reader contention on the engine side only.
Set `G1_SYMMETRIC=0` to reproduce those traces. No symmetric numbers are recorded yet.

# M1 append baseline

Measured on 2026-10-18. These are local development numbers, not G1 results.

## Hardware

- CPU: Apple M4.
- OS: macOS 27.0.1, build 26A434.
- Disk: internal APPLE SSD AP0512Z, 500.28 GB, Apple Fabric, APFS.
- Workspace volume: 460 GiB, about 64 GiB free during measurement.
- Compiler: rustc 1.99.1, commit `58cf8f9c3`.
- Cargo: 1.99.1, commit `5f94df478`.
- Criterion: 0.5.1. Release settings: opt-level 3, thin LTO, 1 codegen unit.

Hardware commands:

```sh
sysctl -n machdep.cpu.brand_string
sw_vers
diskutil info /
system_profiler SPNVMeDataType
df -h .
rustc --version
cargo --version
```

## Method

```sh
cargo bench -p zerodb-tape --bench append -- --noplot
```

The recorded run had no concurrent test workload.
An earlier run overlapped filesystem stress. Its numbers aren't included.
This was a shared development machine, not an isolated performance host.

Each path preallocates 64 MiB segments using `set_len`, followed by file and directory sync.
Initial preparation is outside measurement. Any later roll remains inside measurement.
The bare path calls `write_all` and `sync_data` for each encoded transaction.
Both paths use identical content and encoded byte counts, including salts, headers, and the transaction footer.
Encoded sizes are 524 B for 256 B content, and 4,364 B for 4 KiB content.
The bare frame repeats fixed metadata. Tape generates fresh salts, LSNs, timestamps, and tenant chains.
Tape also includes caller preparation, queueing, acknowledgements, and periodic superblock maintenance.

Criterion uses 20 samples, 1 second of warmup, and a 3-second target measurement.
The mean comes from `target/criterion/**/new/estimates.json`, not the displayed regression slope.
For 16 writers, each Criterion iteration contains 16 appends, 1 per writer.
The amortized mean divides the round mean by 16. It isn't caller latency.
The single-writer Tape measurement uses the same threaded helper with 1 caller.
Thread creation precedes timing. Final joins remain inside the round measurement.

A separate manual loop records 1,024 append latencies per case.
For 16 writers, each caller contributes 64 samples.
The table reports nearest-rank p50 and p99 from these individual caller timings.
Manual latency and Criterion means come from separate loops.

## Numbers

| Content | Path | Writers | Criterion mean, ms/round | Amortized mean, us/op | p50, ms/op | p99, ms/op |
|---|---|---:|---:|---:|---:|---:|
| 256 B | Bare | 1 | 3.982 | 3,982.308 | 3.993 | 4.323 |
| 256 B | Tape Durable | 1 | 4.017 | 4,016.876 | 3.994 | 5.067 |
| 256 B | Tape Durable | 16 | 7.284 | 455.236 | 7.915 | 8.647 |
| 4 KiB | Bare | 1 | 3.515 | 3,515.329 | 3.889 | 5.050 |
| 4 KiB | Tape Durable | 1 | 3.593 | 3,593.468 | 3.946 | 5.043 |
| 4 KiB | Tape Durable | 16 | 8.635 | 539.715 | 6.883 | 8.264 |

On macOS, Rust's `sync_data` uses `F_FULLFSYNC`.
These numbers aren't comparable to Linux `fdatasync` numbers.
G1 requires Linux NVMe. No G1 performance claim follows from this run.
The 1,024-sample p99 estimates remain sensitive to scheduler and storage noise.

## Validation

The benchmark numbers above were recorded at `822cf91`, before the final M1 review.
The reviews change recovery, clean shutdown, and API visibility, not the measured append path.
The benchmark target is rebuilt after review. The timing loops aren't rerun.
The following review validation completed on 2026-10-18.

| Command | Result |
|---|---|
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo check --lib --no-default-features` | Passed. Production builds without simulation access. |
| `cargo test --workspace` | Passed. 500 seeds. Stress ignored in debug. |
| `cargo test --workspace --release` | Passed. 500 seeds and 20,000 appends across 16 threads. |
| `ZERODB_SIM_SEEDS=10000 cargo test -p zerodb-tape --release --features sim --test sim -- --nocapture` | Passed. Seeds 0 through 9,999. |
| `cargo test --doc` | Passed. Private implementation import paths fail to compile as expected. |
| `cargo bench --bench append --no-run` | Passed. Benchmark compiles against the narrowed API. |
| `cargo bench -p zerodb-tape --bench append -- --noplot` | Passed before review at `822cf91`. All 6 cases recorded above. |

The final validation chain exited with status 0.
The required commands were rerun after the short-session fix at `c9d1909`.
Both workspace suites passed their default 500 seeds.
The 10,000-seed release run passed in 75.15 seconds.
Release stress passed 20,000 appends across 16 threads in 10.55 seconds.
Every randomized seed runs 2 or 3 crash generations.
The 10,000-seed run includes 910 deliberate below-superblock corruption cases.
Each required `CorruptionDetected`. All other recovery opens succeeded.
It also includes 104 supplemental large-record scenarios.
The main workload still uses 64 KiB segments for every seed.
The normal and release suites both exercise the oracle's negative tests.
These tests reject partial uncertain transactions and changed uncertain content.

Replay commands:

```sh
ZERODB_SIM_SEED=42 cargo test -p zerodb-tape --release --features sim --test sim
ZERODB_SIM_SEEDS=10000 cargo test -p zerodb-tape --release --features sim --test sim
cargo test --release --test stress
```

## Open cost limit (M2)

Open remains O(tape) for CRC, digest, and chain verification.
Recovery verifies every segment before application replay, even with a current checkpoint.
The replay counter measures records above the checkpoint watermark, not total open work.
A later milestone must add verified-segment markers before we claim checkpoint-bounded open time.
The append measurements above don't measure startup latency.

## Size budget

Production source measures 2,471 lines, including blank lines and comments.
The count excludes `src/sim.rs` and trailing `#[cfg(test)]` modules.
At measurement time, it included the always-exported SimDisk implementation.
Current builds gate SimDisk behind the `sim` feature outside unit tests.
The raw count includes every `src/*.rs` line: 3,198.
That includes 536 simulation-only helper lines and 191 embedded unit-test lines.
Production remains below the M1 budget of 3,000.

```sh
wc -l crates/zerodb-tape/src/*.rs
python3 - <<'PY'
from pathlib import Path
total = 0
for path in Path("crates/zerodb-tape/src").glob("*.rs"):
    if path.name != "sim.rs":
        total += len(path.read_text().split("#[cfg(test)]")[0].splitlines())
print(total)
PY
```

## Fixes and limits

The carry-over regression reproduced a crash after manifest trimming but before orphan unlink.
Open now removes later segment files before recovery and syncs the directory.
An additional seeded regression found incomplete retry durability after cleanup's directory sync failed.
Seed 2 reproduced the orphan returning after a successful retry.
Open now syncs the directory even when no orphan remains visible.
Both regressions failed before their fixes. Each fix has its own signed-off commit.
The carry-over fix is `643ec6d`. The retry regression fix is `f92ca69`.

### Final review fixes

The initial harness missed process restart without power loss after failed sync.
The reviewer's sequence reproduced lost durable acknowledgements at seed 0 before the fix.
Recovery now rewrites the retained suffix above the superblock, then syncs its files and directory.
This re-dirties readable cache bytes left clean after failed writeback.
The regression passes 200 seeds in both stale and advanced superblock cases.
A separate regression covers suffix rewriting across rolls and before a trimmed tail.

The rewrite includes zero padding and the last segment's unused preallocation.
A further regression reproduced failed tail-repair retry losing a later rolled acknowledgement at seed 1.
It passes 32 seeds with both stale and advanced superblocks after rewriting padding.
The rewrite covers data written since the last superblock install, plus anything unsynced at a crash.
Under continuous load, that's at most about 1 second of writes.
Delayed superblock maintenance extends that window.
The physical rewrite also includes roll padding and at most 1 final segment's unused capacity.

All recovery error LSN increments now saturate.
A CRC-valid transaction at `u64::MAX`, followed by an oversized segment, reproduced the overflow before the fix.
Its regression checks both zero and maximum superblock values.

Simulation now includes live reopen after poison, followed by more acknowledgements and a power-loss crash.
Some generations also capture a crash during recovery's rewrites, zero-tail repair, or manifest cleanup.
Corruption samples random byte offsets across all segments through the superblock transaction's footer.
The selected bit spans 0 through 7.
A coverage test checks both segments, offsets beyond the first header, and every bit.
The SimDisk regression confirms live reopen preserves failed cached bytes.
Another sync doesn't heal them. Rewriting identical bytes does.

Production exports storage types from the crate root, including `TapeOptions`.
Directory, recovery, segment, writer, and codec modules are crate-private.
The `sim` feature gates external access to `WriterCore`, `DataDir`, and other deterministic helpers.
Development self-dependency enables that feature for tests and benches.
Default library builds don't expose it.
`deny(missing_docs)` enforces public API documentation.
The root API test and compile-fail documentation tests verify the supported and rejected import paths.

| Commit | Change |
|---|---|
| `4728f9a` | Redirty recovered suffixes. Add live-restart regressions and generations. |
| `e8ec017` | Saturate corruption LSN reporting at `u64::MAX`. |
| `c4a6f08` | Spread corruption across durable prefix bytes and all bits. |
| `c8f1008` | Narrow and document the public API. Gate deterministic access. |
| `5c552db` | Persist cached zero padding. Cover failed repair retry and recovery cuts. |
| `c9d1909` | Checkpoint recovery and clean shutdown. Bound short-session rewrites and skip untouched segment syncs. |

### Short-session re-review

The re-review found short sessions could leave the superblock at 0 indefinitely.
The original regressions failed before the fix.
After 2 sessions, the next open rewrote 2,048 bytes despite only 1,024 bytes written in the previous session.
Corrupting the first short session's acknowledged data also returned success instead of `CorruptionDetected`.
Recovery didn't install its newer durable LSN before returning.

Open now installs any recovered boundary after suffix rewrites, touched-file syncs, and directory sync.
Clean shutdown drains requests, syncs pending Fast records, and forces a checkpoint without the 1-second limit.
Dropping `Tape` joins that shutdown.
Final sync failures and poisoned writers don't advance the boundary.
Unchanged segments fully covered by the superblock aren't synced again.

The counting I/O regression runs 16 short sessions, advancing SimClock by 0.5 seconds between sessions.
Each append fills its 1,024-byte segment, excluding unused-preallocation overhead from the measured bound.
Each open stays within the previous session's write count.
After the first session, no segment data is rewritten or synced during those opens.
A separate 8-session test checks the installed boundary and detects damage to older session data.
Other tests verify recovery sync ordering, final Fast sync before checkpoint, and failed shutdown preserving the old boundary.

The physical rewrite still includes retained padding and at most 1 final segment's unused capacity.
The superblock fix prevents those costs from growing across historical segments.
Drop can't return shutdown errors. The next successful open recovers and checkpoints before accepting writes.

Each review fix is signed off and carries the Copilot co-author trailer.
The simulator models I/O boundaries and torn writes. It doesn't replace real-filesystem crash testing.
The stress test covers real queue contention, not power loss.

# M2 first review (historical)

These measurements describe the first review. The latest merge-review results appear at the end of this file.

Reviewed measurements were recorded on 2026-10-18 on `feat/m2-sim-g1`.
The review base is `a27f307`. Commit identities for each measured stage appear below.
Nothing was pushed.

## Hardware and durability

This run uses the same Apple M4 and internal Apple SSD as M1.
macOS is 27.0.1, build 26A434. The workspace uses APFS, with about 61 GiB free.
The compiler is rustc 1.99.1, commit `58cf8f9c3`.
Cargo is 1.99.1, commit `5f94df478`.
Release settings remain opt-level 3, thin LTO, and 1 codegen unit.
Rust's macOS `sync_data` invokes `F_FULLFSYNC`.
The benchmark uses real filesystem I/O. SimDisk is not used for these measurements.
This is a shared development machine, not an isolated performance host.
Short targeted tests and builds sometimes overlapped the diagnostic stage runs.
The final 1M-turn repetitions run after all test workloads finish.

The requested macOS target is median paired Engine/bare durable-ack p99 at most 1.5x.
**macOS verdict: FAIL. Final median ratio is 1.640x, with a 1.566x through 1.645x spread.**
**Linux NVMe G1 and nightly LazyFS crash validation remain pending.**
This harness is an append-only M2 proxy. It omits cache writes until M3.
It does not yet implement the architecture's complete mixed-cache-write workload.

## Matched trace

```sh
./bench/g1.sh
G1_TURNS=192000 G1_REPS=3 G1_HISTORY=0 ./bench/g1.sh
```

The default trace contains 1,000,000 timed turns per path, with 4 turns per durable batch.
Each measurement runs 3 interleaved repetitions: bare, Tape, Engine, then repeats that order.
The final history experiment runs after all 9 timed phases.
Every summary reports median and min/max across repetitions.
Engine/bare ratios are paired within repetitions before computing their median.
The ratio median is not the quotient of separately selected p99 medians.
All paths use 16 writers. The engine uses 16 tenants and 1024 sessions.
Content sizes cycle through 256 B, 512 B, 1 KiB, 4 KiB, and 8 KiB.
The engine also runs 2 concurrent last-16 window readers.
Initial tenant and session registration uses 1024 untimed turns.
The timed trace excludes tenant creation. Retry keys are disabled for this performance trace.
Default engine checkpoints remain enabled.

All 3 paths use identical content-size ordering, turn payload lengths, and transaction batching.
The bare baseline reuses M1's frame encoder, preallocation, append, and file sync implementation.
Each encoded turn contains 16 metadata bytes and 63 total codec overhead bytes.
The 5 encoded batch lengths are 6768, 10864, 13936, 14448, and 14704 bytes.
These lengths include salt space, record headers, and transaction footers.
The bare path precomputes fixed metadata and frame bytes outside timing, as in M1.
Tape and Engine include caller preparation, hashing, queueing, and replies.
Engine additionally includes session publication, checkpoints, and concurrent window load.

Bare and Tape share the same adaptive group collector starting at C2.
Its default timeout is 200 us. Zero disables waiting. Configuration above 200 us is rejected.
The target is the largest of the previous 64 groups.
Actual group boundaries still depend on scheduling and caller preparation.
All paths report observed group sizes. Engine counts exclude registration warmup.
Segment logical preallocation is 64 MiB on every path.
Physical preallocation was skipped. M1's `set_len` path remains unchanged.
No libc dependency or unsafe FFI was added.
Later segment rolls remain inside timing. Initial segment setup and final shutdown stay outside.
Only the engine runs window readers.
Every session's final sequence is checked after reopen, outside timing.

Caller latencies use nearest-rank p50, p99, and p99.9 across 250,000 durable batch acknowledgements.
These are batch latencies, not amortized per-turn latencies.
Throughput reports turns per second across the complete timed phase.
Window latency samples every 64th completed query, while counting every query.

## Ordered 192k-turn experiments

Each cell is median [minimum, maximum] across 3 interleaved repetitions.
The p99 columns use milliseconds per durable batch.
Every stage uses 192,000 turns per path per repetition. Each path has 48,000 latency samples.
These are diagnostic comparisons on a shared host, not isolated causal measurements.
The baseline was rerun with repetitions. The earlier single-run comparison is withdrawn.

| Stage | Commit | Bare p99, ms | Tape p99, ms | Engine p99, ms | Paired Engine/bare ratio |
|---|---|---|---|---|---|
| Review baseline | `863be52` | 10.008 [9.899, 10.818] | 11.989 [10.527, 13.085] | 17.903 [15.808, 21.953] | 1.655 [1.597, 2.193] |
| C1: wake coalescing | `4795ede` | 9.875 [9.100, 10.110] | 13.448 [10.170, 15.947] | 11.689 [11.608, 16.991] | 1.285 [1.176, 1.681] |
| C2: grouping + entropy handle | `753130c` | 6.882 [5.802, 6.993] | 11.889 [9.997, 14.763] | 12.634 [10.921, 15.411] | 1.882 [1.807, 2.239] |
| C3: buffered saves + install chunks | `83fd3db` | 6.092 [5.949, 7.924] | 10.396 [9.997, 12.197] | 13.821 [12.819, 13.988] | 2.104 [1.765, 2.323] |
| C4: sequence receipts | `0888163` | 6.161 [5.947, 8.374] | 10.219 [9.991, 11.057] | 12.867 [12.119, 12.993] | 2.109 [1.447, 2.164] |
| C5: superblock helper | `29c5f48` | 6.027 [5.952, 6.169] | 10.095 [10.080, 14.066] | 13.293 [12.958, 14.078] | 2.234 [2.101, 2.336] |
| C6: keyless digest + borrowed parser | `183f556` | 6.048 [5.951, 8.616] | 11.014 [9.995, 11.991] | 11.965 [11.941, 12.987] | 2.007 [1.389, 2.147] |
| C2 follow-up: batched entropy read | `120c60d` | 6.025 [6.002, 6.143] | 9.943 [9.877, 9.991] | 12.036 [12.018, 12.067] | 1.998 [1.964, 2.002] |
| C2 follow-up: drain ready tail | `0bc83ed` | 5.988 [5.971, 6.786] | 7.476 [6.868, 7.842] | 9.125 [8.054, 9.774] | 1.345 [1.345, 1.637] |

C1 completed 8 checkpoint saves in each 192k repetition, consistent with the byte threshold.
The regression queues 100 over-threshold publications and requires exactly 1 pending wake.
Another regression services stale wakes and requires exactly 10 saves for 100 byte increments at threshold 10.
The former wake queue could hold 16 redundant requests and save after each one.

C2 changes the bare denominator too. Its p99 falls as group formation improves.
A larger Engine/bare ratio between C1 and C2 is not evidence that Engine itself became slower.
The entropy follow-up replaces 32 reads with 1 in the 32-record regression.
Caller hashing runs after releasing the shared entropy handle.

C3's checkpoint-write regression went from 22 writes to 1 for 20 session pages.
Buffering trades checkpoint-sized memory for fewer I/O calls, outside the resident slot budget.
The review identified install holds that the former snapshot-only counter could not see.
The new State-hold metric includes snapshots, each 32-page install chunk, and final watermark publication.
C3's largest observed State hold across its 3 repetitions was 608.791 us.
Old-file unlink and directory sync no longer hold State.

C4's read-fault regression failed before the fix and now leaves the fault unused after append and retry.
Prepare stages sequence metadata. Sync and publication precede releasing that opaque receipt.
Checkpoint retry entries persist it, while full replay reconstructs it from committed turns.
Older derived checkpoint records without receipts fall back to replay. The tape format is unchanged.

C5's regression previously observed `zerodb-writer` writing periodic superblocks.
It now requires `zerodb-superblock`.
The helper samples only the published durable boundary.
Open still installs synchronously. Shutdown joins the helper before its final synchronous install.
Metadata failures poison later requests, without invalidating earlier durable acknowledgements.

C6 skips retry hashing without a key and computes keyed retry digests on callers.
Its parser validates borrowed slices instead of copying turn content during prepare and publish.
The first turn is decoded once during prepare.
Full tape integrity refresh after sequence mutation remains on the writer.
It cannot reuse the caller's old payload digest without changing or weakening the checked bytes.
No digest or durability check was removed.

### Gap diagnosis before the timeout-drain follow-up

The entropy-only 192k follow-up still misses 1.5x.
Its paired ratio is 1.998 [1.964, 2.002], despite fixing the reviewed paths.
Bare groups average 15.989 through 16.000 requests.
Tape groups average 13.849 through 14.829. Engine groups average 9.930 through 10.492.
Therefore the bounded coalescer improves formation but does not eliminate split groups under engine reader load.
Requests missing the 200 us window still wait another durable sync.
Increasing that bound would violate this review's limit.
The median throughput ratio is 10,161.713 / 16,219.386, or 0.627.
The median group-size ratio is 9.969 / 15.995, or 0.623.
The visible sync-amortization deficit closely matches the throughput deficit.
This does not isolate every pre-enqueue CPU or scheduling contribution.

This exposed another grouping bug.
When scheduling exhausted the wait budget, the collector returned without draining requests already queued.
That ready tail unnecessarily waited for another sync.
The deterministic regression returned `[1]` instead of `[1, 2, 3]` before the fix.
Commit `0bc83ed` adds a final nonblocking drain and records the resulting full group size.
It does not extend the blocking wait beyond 200 us.
Bare and Tape both use the corrected collector.
A partial 1M run was cancelled and discarded after finding this issue.

### Corrected timeout drain

The corrected 192k experiment restores Engine groups to 15.995 requests in all 3 repetitions.
Tape groups range from 15.968 through 15.989. Bare groups range from 15.989 through 16.000.
Engine throughput median is 15,920.939 turns/s, versus bare's 16,078.143 turns/s.
The paired p99 ratio is 1.345 [1.345, 1.637].
That meets the requested median target in this diagnostic run, but 1 repetition still exceeds it.
This is not a substitute for the final 1M-turn verdict.

Bare preframes fixed transactions, while Tape still hashes, frames, maintains chains, and handles segment metadata.
Engine additionally publishes session slots, checkpoints, and competes with window reads.
Moving superblocks off the writer removes writer stalls, not the SSD's physical flush work.
These remaining costs must not be hidden by acknowledging before sync or replying before publication.
The aggregate harness does not isolate their individual CPU and I/O contributions.
The final 1M-turn repetitions below determine the requested local verdict.

## Final 1M-turn repetitions

Measured at `0bc83ed`, including the corrected nonblocking ready-tail drain.
All required validation finished before this run started. No test workloads from this task overlapped it.
The command completed with status 0:

```sh
G1_TURNS=1000000 G1_REPS=3 G1_HISTORY=1 ./bench/g1.sh
```

Every timed path appends 1,000,000 turns in each repetition.
That is 3,000,000 timed turns per path, with 250,000 durable-ack samples per repetition.
Every engine reopen verified the expected final sequence in all 1024 sessions.

| Repetition | Bare p99, ms | Tape p99, ms | Engine p99, ms | Paired Engine/bare ratio |
|---:|---:|---:|---:|---:|
| 1 | 6.009 | 7.110 | 9.856 | 1.640 |
| 2 | 7.589 | 9.926 | 11.885 | 1.566 |
| 3 | 6.085 | 7.800 | 10.011 | 1.645 |
| Median [min, max] | 6.085 [6.009, 7.589] | 7.800 [7.110, 9.926] | 10.011 [9.856, 11.885] | **1.640 [1.566, 1.645]** |

The following cells are median [minimum, maximum] across repetitions, not pooled sample quantiles.

| Path | Turns/s | Ack p50, ms | Ack p99, ms | Ack p99.9, ms |
|---|---|---|---|---|
| Bare | 16,132.7 [15,787.5, 16,613.8] | 3.968 [3.951, 4.035] | 6.085 [6.009, 7.589] | 12.139 [11.956, 13.028] |
| Tape | 15,756.8 [13,197.3, 15,773.9] | 3.994 [3.992, 4.426] | 7.800 [7.110, 9.926] | 20.080 [19.923, 28.147] |
| Engine + readers | 14,493.7 [13,497.0, 14,572.8] | 4.043 [4.036, 4.247] | 10.011 [9.856, 11.885] | 21.072 [21.049, 22.194] |

### Final gap diagnosis

All final repetitions exceed 1.5x. The local target is not met.
The 192k median pass did not carry over to the longer trace.
The corrected collector now forms nearly full groups on every path:

| Repetition | Bare requests/group | Tape requests/group | Engine requests/group | Checkpoint saves | Max State hold, us |
|---:|---:|---:|---:|---:|---:|
| 1 | 15.997 | 15.987 | 15.997 | 45 | 798.792 |
| 2 | 15.999 | 15.985 | 15.998 | 45 | 1406.833 |
| 3 | 16.000 | 15.981 | 15.998 | 45 | 1463.750 |

Thus split groups no longer explain the final gap.
The engine completes 45 checkpoints per run, rather than the review's reported 553 saves.
This matches the byte threshold for the 3.036 GB encoded trace.
All runs end with 5 checkpoint files. Their sampled watermarks are 997,336, 997,332, and 997,320.
State-hold maxima include installation. The snapshot is the maximum observed hold in these runs.
The longest reported hold is 1.464 ms.
The former snapshot-only counter missed install holds reported at 12 ms.

The remaining measured gap is primarily in the tail.
Median p50 is 4.043 ms for Engine and 3.968 ms for bare.
Median p99 is 10.011 ms versus 6.085 ms.
Tape's own median p99 is already 7.800 ms, before engine publication and reader load.
The extra work therefore exists in both the tape layer and the engine layer.

Bare uses preframed bytes. Tape adds framing, integrity work, chains, and manifest installs when segments roll.
Engine also adds session publication, checkpoints, and concurrent windows.
The 45 checkpoints require 180 file/directory sync calls, plus 5 compaction cleanup directory syncs.
Periodic superblocks add synced atomic installs even though their I/O no longer runs on the writer.
On this machine those file syncs use `F_FULLFSYNC`; moving them elsewhere does not remove media work.
The aggregate harness does not separate CPU cost from those competing durability barriers.
It does show that wake amplification, read-after-ack, and half-group formation have been removed.
No durability, integrity, or publication guarantee was relaxed to chase the target.

Further profiling must attribute per-phase CPU, sync time, metadata barriers, and reader contention.
Linux NVMe measurements remain necessary. This macOS failure is not a Linux verdict.

### Concurrent window readers

The readers completed 1,191,295 windows across the 3 engine phases.
Latency samples use stride 64:

| Repetition | Windows | Samples | p50, us | p99, us | p99.9, us |
|---:|---:|---:|---:|---:|---:|
| 1 | 388,984 | 6078 | 326.041 | 966.458 | 3180.833 |
| 2 | 415,072 | 6486 | 327.333 | 1027.834 | 1831.167 |
| 3 | 387,239 | 6052 | 328.542 | 914.584 | 1912.583 |

## Window work versus history

Each cell measures 2000 queries after 100 warmups, returning exactly 16 turns.
Content is fixed at 256 B for this separate history-length experiment.
Since queries start near the middle, except at sequence 1 for the 16-turn history.
Both query forms issue exactly 16 tape reads at every history length.
The harness asserts that count, rather than inferring complexity from elapsed time.

| History turns | Query | Returned | Tape reads/query | p50, us | p99, us | p99.9, us |
|---:|---|---:|---:|---:|---:|---:|
| 16 | Last 16 | 16 | 16 | 237.625 | 365.375 | 503.959 |
| 1024 | Last 16 | 16 | 16 | 232.375 | 364.709 | 476.333 |
| 65,536 | Last 16 | 16 | 16 | 233.917 | 380.250 | 428.375 |
| 16 | Since, limit 16 | 16 | 16 | 242.458 | 351.583 | 432.458 |
| 1024 | Since, limit 16 | 16 | 16 | 233.375 | 350.750 | 420.958 |
| 65,536 | Since, limit 16 | 16 | 16 | 232.792 | 370.041 | 427.000 |

History grows 4096x while record-read work remains flat.
Tail latency still reflects filesystem and scheduler noise.

## Engine simulation and validation

The simulator drives the real Engine API on SimDisk and SimClock.
The `sim` feature's inline option disables background worker threads.
Fixed salts and seeded checkpoint points remove scheduler and entropy variance.
Production defaults retain worker threads and OS entropy.

Each seed runs 120 randomized batches across 48 scopes, with 4 tenants and 2 agents.
It includes 3 append crash generations and cuts during checkpoint installation.
Sync failures occur before persistence and after persistence. Each failure triggers live reopen.
Half the seeds use a 4 KiB hot-page budget.
Each generation corrupts a checkpoint and checks full replay with a warning.
Corruption targets a page-record checksum byte while leaving the file magic intact.
Keyless batches use unique oracle metadata rather than requiring an idempotency key.
After a mid-append cut followed by success, a second post-return image must contain the acknowledgement.
The independent oracle scans recovered tape records before opening the engine.
It verifies dense sequences, exact bytes, stable tenant IDs, and complete transactions.
It also checks last/since windows and original idempotent sequence, transaction, and LSN receipts.
Retries cover uncertain results, live reopen, and crash recovery.
Conflicting retries must write nothing and report `NotCommitted`.
A repeat-run test compares the complete final histories for the same seed.
Every seed also runs 3 grouped generations on the real Engine inline queue.
Each step queues 2 through 4 distinct appends, a same-group retry, and a conflict.
Seeded I/O callbacks call checkpoint save between prepare and publish.
The byte-threshold pending flag is serviced through the same wake check as production.
Grouped sync failures and crashes check complete uncertain transactions and stable retry receipts.
The dedicated group test requires 3 requests, 2 writes, and 1 sync before returning their sequences.
Negative oracle tests reject partial uncertain batches and missing acknowledged records.

The earlier 10,000-seed run covered the pre-review simulator, not these added grouped scenarios.
The reviewed code is validated separately below.
The expanded 2,000-seed engine run passed in 124.49 seconds.
The 2,000-seed M1 run passed in 17.99 seconds.
No storage correctness failures occurred in either run.
CI now runs the default 500 engine seeds in release mode on Linux and macOS.

| Command | Result |
|---|---|
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --workspace` | Passed. Default 500 M1 seeds. Release-only stress ignored. |
| `cargo test --workspace --release` | Passed. Default 500 M1 seeds and release stress. |
| `ZERODB_SIM_SEEDS=2000 cargo test -p zerodb-tape --release --features sim --test sim` | Passed. Seeds 0 through 1,999. |
| `ZERODB_SIM_SEEDS=2000 cargo test -p zerodb-core --release --features sim --test engine_sim` | Passed. Seeds 0 through 1,999. All 4 tests passed. |
| `cargo check -p zerodb-core --lib --no-default-features` | Passed. Exposed a deterministic-only method warning, fixed in `a139813`. |
| `cargo clippy -p zerodb-core --lib --no-default-features -- -D warnings` | Passed after that feature-gating fix. |
| `cargo bench -p zerodb-core --bench g1 --no-run --quiet` | Passed |
| `cargo bench -p zerodb-tape --bench append --no-run --quiet` | Passed |
| `G1_TURNS=192000 G1_REPS=3 G1_HISTORY=0 ./bench/g1.sh` | Passed after each ordered stage, including the entropy follow-up. |
| `G1_TURNS=1000000 G1_REPS=3 G1_HISTORY=1 ./bench/g1.sh` | Passed. All 9 timed phases, reopen checks, and history checks completed. Local performance verdict: FAIL. |

The full validation chain exited with status 0.
It was rerun after the ready-tail correction, before the final benchmark.
Both clippy configurations completed without warnings.

## Review commits

Every commit is signed off and carries the Copilot co-author trailer.

| Commit | Change |
|---|---|
| `863be52` | Three interleaved repetitions and paired ratio summaries. |
| `4795ede` | Coalesced checkpoint pending flag and threshold recheck. |
| `753130c` | Bounded adaptive groups and persistent entropy handle. |
| `83fd3db` | Buffered checkpoint writes, chunked installs, and full State-hold metric. |
| `0888163` | Opaque sequence receipts released after publication. |
| `29c5f48` | Periodic superblock helper with ordered shutdown join. |
| `183f556` | Keyless digest bypass, caller retry hashing, and borrowed payload parsing. |
| `15afc78` | Grouped simulation, in-step checkpoints, post-ack images, keyless batches, and page CRC damage. |
| `120c60d` | One entropy read per caller batch. |
| `e27b43f` | Correct medians for even repetition counts. |
| `cab2a4c` | Documented execution, simulation, and benchmark paths. |
| `a139813` | Gate the deterministic superblock method out of production-only builds. |
| `0bc83ed` | Drain already-ready arrivals when the bounded group wait ends. |

Core has 2343 physical source lines before trailing unit-test modules.
Of those, 2166 are nonblank and noncomment lines.
These counts include feature-gated simulation entry points and inline test instrumentation.
They are not a production-only size measurement. The previous pre-review line count is withdrawn.
The engine simulator contains 769 lines.
Linux NVMe measurements and LazyFS testing remain future work.

# M2 merge review

This section supersedes the first-review verdict above.
The review base is `b2f6873`. Measurements use the same Apple M4, APFS, and macOS 27.0.1 machine.
Rust's file `sync_data` uses `F_FULLFSYNC` here.
Linux NVMe and nightly LazyFS remain pending.
The trace still omits cache writes until M3.

## Merge-blocking correctness fixes

### Bounded checkpoint encoding

`2cc312d` caps the encoded staging buffer at 4 MiB.
Full buffers write at increasing offsets without splitting records.
All data chunks precede the single data-file sync.
Snapshot references, backing-location metadata, and bounded record scratch remain separate allocations.
The bound does not claim constant memory for the entire index snapshot.

The original regression observed a single 5,140,624-byte data write.
`f297302` extends it to 640,000 indexed turns and 10,000 retry entries in a full snapshot.
That encodes an 8,631,174-byte checkpoint using these data writes:

| Offset | Bytes |
|---:|---:|
| 0 | 4,172,146 |
| 4,172,146 | 4,194,071 |
| 8,366,217 | 264,957 |

Peak occupied encoding bytes are 4,194,071, below the 4,194,304-byte cap.
The buffer's reserved capacity is also capped.
The regression reloads all 128 sessions and 10,000 retry entries.
It also reads a live backing location beyond the first buffer and compares its slots.

### Publication between checkpoint snapshot and install

`66c0f23` adds the reverse I/O interleaving missing from the earlier simulator.
The FaultIo callback runs queued `step_sim` work during checkpoint I/O.
That publishes a newer page revision after the old snapshot but before installing its backing.
A subsequent checkpoint and reopen must preserve the newly acknowledged turn.
Seeded workloads exercise both directions: checkpoint inside writer step, and writer step inside checkpoint.

Mutation command, with the revision guard temporarily removed:

```sh
ZERODB_SIM_SEEDS=300 cargo test -p zerodb-core --release --features sim --test engine_sim
```

The dedicated regression failed, returning sequences `[1, 2]` instead of `[1, 2, 3]`.
The randomized test also failed immediately at seed 0. It did not finish 300 seeds.
The guard was restored. All 300 seeds then passed.
The final validation reruns 2,000 seeds with the restored guard.

### Asynchronous poison and checkpoint version

`53f1ec9` injects the metadata worker's poison flag after prepare, while a writer step is running.
The written but unpublished request receives explicit `Unknown`.
The next queued request receives `NotCommitted`.
No records become readable, and publication must not run.
This replaces a dropped reply channel with an explicit error in the deterministic driver.
The public append path already mapped that dropped channel to `Unknown`.

`dae392e` changes checkpoint-file magic to `ZCK2`.
The manifest stays `ZCM1`. The authoritative tape format is unchanged.
A CRC-valid `ZCK1` file now logs a warning and triggers full replay.
Its keyed retry still returns the original sequences.

## Metadata flush policy

### Deferred superblocks

`781d781` requests helper installation after any published segment roll.
Without a roll, the defaults are 30 seconds or 256 MiB of published encoded tape bytes.
Both thresholds are configurable and must be positive.
The byte counter includes roll padding.
Requests run after replies and do not wait for helper completion.
Open and shutdown retain synchronous installs. Shutdown joins the helper before its final install.
Every installed LSN still comes from the already-synced published boundary.

The tradeoff is a larger recovery rewrite and corruption-detection window above the completed superblock.
With default 64 MiB segments, rolls generally request installation before the 256 MiB trigger.
The byte trigger protects configurations with larger segments or a longer time interval.
Trigger accounting can overshoot by one published group.
Helper scheduling and I/O can extend the completed-install lag.
Thus 256 MiB is a request budget, not an unconditional hard repair-size ceiling under stalled I/O.
Blocking acknowledgements to impose that ceiling would contradict the required asynchronous policy.
Repair additionally covers crash-time unsynced bytes and final unused segment preallocation.
The default preallocation is 64 MiB. Clean shutdown avoids this crash-only lag by installing the final boundary.

Unit tests cover the 30-second boundary, byte-triggered installation, and segment-roll installation without clock progress.
Existing stale/advanced-superblock and failed-sync regressions retain their protected-boundary checks.
The M1 randomized driver explicitly configures 1 second to preserve its earlier rapid watermark coverage.
The 30-second production default is tested separately.

### One checkpoint directory sync

`0e273c8` removes the directory sync between checkpoint data and manifest installation.
The sequence remains: write data chunks, sync data, write and sync manifest staging, rename, then sync the directory.
Full compaction still syncs its later old-file cleanup.
This removes 1 directory sync per checkpoint, not the data or manifest file sync.

A new crash test captures after manifest rename, before the final directory sync.
It finds an image where the manifest exists but its referenced checkpoint filename disappeared.
Open must log a warning and replay every authoritative record.
Both acknowledged turns survive.
The checkpoint-only crash matrix now requires either a valid chain or an explicit full-replay warning.
It never permits silently accepting an incomplete derived chain.

## Measured replay budget

The default checkpoint interval remains 64 MiB, not 128 MiB.
`43b9228` adds an alternating clean-reopen probe using the same mixed turn sizes and concurrent reader load.
It runs 3 repetitions per setting, with 208,000 timed turns in each phase:

```sh
G1_TURNS=208000 G1_REPS=3 G1_REPLAY_PROBE=1 ./bench/g1.sh
```

Cells report median [minimum, maximum].

| Checkpoint interval | Clean open, ms | Replayed application records | Sampled pre-close pending bytes, MiB |
|---|---|---|---|
| 64 MiB | 1082.880 [1074.029, 1093.386] | 8904 [8852, 8912] | 25.852 [25.725, 25.865] |
| 128 MiB | 1459.177 [1458.994, 1468.678] | 31,168 [31,164, 31,244] | 90.317 [90.303, 90.555] |

The 128 MiB setting exceeds the existing 64 MiB replay-byte budget in this probe.
Its larger record tail also increases measured clean-open time.
Therefore this change keeps the 64 MiB default.
`G1_CHECKPOINT_MIB=128` remains available for further experiments, without changing production defaults.

Open time includes complete tape verification, checkpoint loading, and application replay.
It does not isolate replay CPU time.
Graceful shutdown installs the final superblock, so this probe does not measure crash-repair rewriting.
Pending bytes are sampled before shutdown. A checkpoint finishing during shutdown can reduce the actual replay tail.

## Final merge-review G1

Measured at `f297302` on 2026-10-18, after all required validation completed.
No test workloads from this task overlapped the final benchmark.
The command completed with status 0:

```sh
G1_TURNS=1000000 G1_REPS=3 G1_HISTORY=1 G1_REPLAY_PROBE=0 G1_CHECKPOINT_MIB=64 ./bench/g1.sh
```

Each repetition runs bare, Tape, then Engine with 2 concurrent window readers.
Every path appends 1,000,000 timed turns per repetition in 4-turn durable batches.
The trace cycles through 256 B, 512 B, 1 KiB, 4 KiB, and 8 KiB content.
Initial registration remains outside timing.
All engine reopens verified the expected final sequence in each of 1024 sessions.

Ratios below are paired within each repetition.
They retain 6 decimal places to expose the first repetition's narrow threshold miss.
The verdict uses unrounded ratios, not the rounded CLI display.

| Repetition | Bare p99, ms | Tape p99, ms | Engine p99, ms | Engine/bare | Tape/bare |
|---:|---:|---:|---:|---:|---:|
| 1 | 6.042041 | 7.007583 | 9.063750 | 1.500114 | 1.159804 |
| 2 | 8.511292 | 9.064708 | 11.212458 | 1.317363 | 1.065021 |
| 3 | 6.904625 | 7.756292 | 9.664083 | 1.399654 | 1.123347 |
| Median [min, max] | 6.904625 [6.042041, 8.511292] | 7.756292 [7.007583, 9.064708] | 9.664083 [9.063750, 11.212458] | **1.399654 [1.317363, 1.500114]** | **1.123347 [1.065021, 1.159804]** |

**macOS median verdict: PASS against 1.5x.**
This is not an all-repetitions pass. Repetition 1 is slightly above 1.5x.
Linux NVMe and nightly LazyFS remain pending.
This append-only proxy does not certify the later mixed-cache-write G1 workload.

The following cells report median [minimum, maximum] across repetitions.
Each path contributes 250,000 durable-ack samples per repetition.

| Path | Turns/s | Ack p50, ms | Ack p99, ms | Ack p99.9, ms |
|---|---|---|---|---|
| Bare | 16,146.8 [14,480.3, 16,453.7] | 3.958 [3.955, 4.107] | 6.905 [6.042, 8.511] | 14.551 [12.066, 17.698] |
| Tape | 15,654.2 [15,101.3, 16,282.0] | 4.001 [3.979, 4.085] | 7.756 [7.008, 9.065] | 20.811 [20.041, 23.832] |
| Engine + readers | 13,823.3 [13,072.4, 14,571.4] | 4.086 [4.042, 4.425] | 9.664 [9.064, 11.212] | 22.040 [21.055, 29.894] |

### Metadata work and memory

Counts are sampled before shutdown, excluding initialization and final shutdown installs.

| Repetition | Tape superblocks | Engine superblocks | Engine checkpoints | Peak encoding buffer, B | Max State hold, us |
|---:|---:|---:|---:|---:|---:|
| 1 | 45 | 45 | 45 | 4,194,084 | 654.917 |
| 2 | 45 | 45 | 45 | 4,194,228 | 1452.292 |
| 3 | 45 | 45 | 45 | 4,194,244 | 1024.167 |

The largest real-trace encoding buffer remains 60 bytes below the 4 MiB cap.
Each run ends with 5 checkpoint files.
The checkpoint sync policy performs 135 normal file/directory sync calls plus 5 compaction cleanup directory syncs.
The previous 45-checkpoint policy required 185 such calls. The new policy requires 140.
There is still one data-file sync and one manifest-file sync per checkpoint.
Only the intermediate directory sync was removed.
The 45 superblock installs follow the 45 published segment rolls with default 64 MiB segments.
They no longer follow the former 1-second cadence.

Group formation remains close to 16 requests:

| Repetition | Bare requests/group | Tape requests/group | Engine requests/group |
|---:|---:|---:|---:|
| 1 | 16.000 | 15.987 | 15.999 |
| 2 | 15.999 | 15.993 | 15.996 |
| 3 | 15.999 | 15.949 | 15.995 |

The first-review Engine/bare median was 1.640x; this run measures 1.399654x.
Absolute Engine p99 changed from 10.011 ms to 9.664 ms.
Bare p99 also changed, from 6.085 ms to 6.905 ms.
Therefore the ratio change must not be presented as an isolated causal speedup.
This remains a shared macOS machine with visible run-to-run variation.

### Clean reopen and windows

Full-trace clean-open time is 6017.377 ms median [5978.479, 6022.293].
Application replay is 3720 records median [3704, 3724].
Full tape verification remains O(tape); these numbers are not checkpoint-bounded startup claims.
They also exclude crash-only rewriting above an older superblock.

Concurrent readers completed 1,242,813 windows:

| Repetition | Windows | Samples | p50, us | p99, us | p99.9, us |
|---:|---:|---:|---:|---:|---:|
| 1 | 418,476 | 6540 | 322.125 | 1014.500 | 1830.042 |
| 2 | 433,129 | 6768 | 325.583 | 1053.167 | 2263.250 |
| 3 | 391,208 | 6114 | 325.167 | 960.625 | 1502.250 |

The final history experiment returns 16 turns, with 2000 queries after 100 warmups per cell.
The harness again asserts exactly 16 tape reads per query.

| History turns | Query | Reads/query | p50, us | p99, us |
|---:|---|---:|---:|---:|
| 16 | Last 16 | 16 | 232.458 | 367.667 |
| 1024 | Last 16 | 16 | 233.042 | 400.417 |
| 65,536 | Last 16 | 16 | 233.250 | 390.542 |
| 16 | Since, limit 16 | 16 | 231.667 | 374.291 |
| 1024 | Since, limit 16 | 16 | 232.458 | 373.083 |
| 65,536 | Since, limit 16 | 16 | 232.500 | 371.875 |

## Merge-review validation and commits

Both 2,000-seed release simulations passed seeds 0 through 1,999.
M1 took 18.08 seconds. The expanded engine suite took 120.34 seconds.
All 7 engine simulation tests passed, including both new checkpoint fallback cases.
The restored revision guard is present. No mutation is included in the commits.

| Command | Result |
|---|---|
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --workspace` | Passed. 500 default M1 seeds; release-only stress ignored. |
| `cargo test --workspace --release` | Passed. 500 default M1 seeds and release stress. |
| `ZERODB_SIM_SEEDS=2000 cargo test -p zerodb-tape --release --features sim --test sim` | Passed |
| `ZERODB_SIM_SEEDS=2000 cargo test -p zerodb-core --release --features sim --test engine_sim` | Passed. All 7 tests. |
| `cargo clippy -p zerodb-core --lib --no-default-features -- -D warnings` | Passed |

# S1 frame v2 (macOS)

Measured on 2026-10-19, after T1 through T4 and the final T5 correctness changes.
Machine: Apple M4, arm64, macOS 27.0.1 build 26A434.
Compiler: `rustc 1.99.1 (58cf8f9c3 2026-10-13)`.
This is a shared machine. No test or simulation from this task overlapped either timed run.

## Storage changes

The fixed header shrank from 104 bytes to 40 bytes. Each record also drops its 16-byte salt.
The footer payload shrank from 44 bytes to 32 bytes.
Its full encoded size is 72 bytes, down from 148 bytes.
A 4-record transaction saves 396 encoded bytes.
CRC32C binds each frame to its manifest segment ID and offset.
Footer CRCs bind exactly the records they close.

The LSN index uses per-segment offset arrays with a paged routing directory.
The default resident budget is 8 MiB. Page charges include cache bookkeeping.
Derived backing files use 8 offset bytes and 16 routing bytes per record, before page padding.
Both files are rebuilt at open. They are not another recovery authority.
Small-budget tests cover eviction, reloads, cross-segment reads and failed index writeback.

## Trace and comparison

Each command runs 3 interleaved repetitions: bare, Tape, then Engine.
Every path writes 192,000 timed turns per repetition, using 4-turn batches and 16 writers.
Each path contributes 48,000 durable-ack samples per repetition.
The Engine also serves 2 window readers across 1024 sessions.
Content sizes cycle through 256 B, 512 B, 1 KiB, 4 KiB and 8 KiB.
Ratios are paired within repetitions. Spreads below are minimum through maximum, not pooled quantiles.

```sh
G1_TURNS=192000 G1_REPS=3 G1_HISTORY=0 G1_REPLAY_PROBE=0 G1_CHECKPOINT_MIB=64 ./bench/g1.sh
```

The command ran twice without code or configuration changes. Both executions returned status 0.
The first execution printed `FAIL` for its 1.5x performance verdict.
The bare path varied sharply, so the same trace was repeated once.
No run was discarded. Both sets of measurements follow.

| Run | Repetition | Bare p99, ms | Tape p99, ms | Engine p99, ms | Engine/bare | Tape/bare |
|---|---:|---:|---:|---:|---:|---:|
| First | 1 | 6.055417 | 7.022292 | 8.020084 | 1.324448 | 1.159671 |
| First | 2 | 5.175958 | 7.073500 | 12.563459 | 2.427272 | 1.366607 |
| First | 3 | 6.473458 | 7.968041 | 11.004625 | 1.699961 | 1.230879 |
| Repeat | 1 | 6.839791 | 6.869625 | 9.054833 | 1.323846 | 1.004362 |
| Repeat | 2 | 5.452459 | 5.854584 | 5.934042 | 1.088324 | 1.073751 |
| Repeat | 3 | 6.252833 | 8.037333 | 8.677084 | 1.387704 | 1.285391 |

| Measurement | Engine/bare median [min, max] | Tape/bare median [min, max] | Against published M2 medians |
|---|---|---|---|
| M2 merge review | 1.399654 [1.317363, 1.500114] | 1.123347 [1.065021, 1.159804] | Reference |
| S1 first run | 1.699961 [1.324448, 2.427272] | 1.230879 [1.159671, 1.366607] | Miss |
| S1 unchanged repeat | 1.323846 [1.088324, 1.387704] | 1.073751 [1.004362, 1.285391] | Pass |

The repeat meets both M2 median targets. Its Engine p99 median is 8.677084 ms.
Its Tape p99 median is 6.869625 ms.
The M2 medians were 9.664083 ms and 7.756292 ms, respectively.
The M2 reference used 1M timed turns per path. S1 uses the plan's requested 192,000 turns.
These trace lengths differ. Neither result proves a causal speedup or a stable hardware-independent bound.
The first S1 miss remains a performance-variance warning.

All 1024 session tails verified after every engine reopen in both runs.
Each engine repetition completed 8 checkpoints and 8 periodic superblock installs.
The repeat's clean-open median was 1148.107 ms [1102.494, 1173.573].
Open still includes a full tape CRC scan. Checkpoints reduce application replay, not that scan.

## Correctness and fault coverage

The final source passed every command below before the T5 commit.
The release workspace run includes the filesystem stress test skipped by the debug run.
The 10,000-seed runs cover seeds 0 through 9,999. CI now sets 2,000 seeds explicitly for both sims.

| Command | Result |
|---|---|
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --workspace` | Passed |
| `cargo test --workspace --release` | Passed |
| `ZERODB_SIM_SEEDS=2000 cargo test -p zerodb-tape --release --features sim --test sim` | Passed. 2,000 seeds. |
| `ZERODB_SIM_SEEDS=2000 cargo test -p zerodb-core --release --features sim --test engine_sim` | Passed. 2,000 seeds. All 7 tests passed. |
| `ZERODB_SIM_SEEDS=10000 cargo test -p zerodb-tape --release --features sim --test sim` | Passed. 10,000 seeds. 105.41 seconds. |
| `ZERODB_SIM_SEEDS=10000 cargo test -p zerodb-core --release --features sim --test engine_sim` | Passed. 10,000 seeds. All 7 tests passed. 877.31 seconds. |

Tape seeds exercise stale footers, corrupt lengths and corruption proofs within and across segments.
They also cover damaged footer kinds, zeroed footers, reply-only sync steps and reordered same-group tails.
Each tape seed injects an operator-cut crash.
Engine seeds add tape corruption and small-index-budget recovery to the existing checkpoint and retry oracle.
Separate regressions check backwards clocks, footer guards and the first bad LSN.
Operator-cut tests check both transaction boundaries and every captured I/O crash point.
An explicit dropped-byte field also covers tails with no decodable dropped LSNs.

D73 rejects interior cuts and names both valid boundaries.
It validates the retained prefix before changing data.
A durable intent bridges zeroed-data sync and superblock lowering.
An interrupted cut resumes before normal recovery checks the durable floor.
The operator report and log identify the cut, dropped range and dropped bytes.
Dropped LSNs may be reused, including acknowledged LSNs. This is the documented exception to I3.

The additional unchanged G1 repeat is the only validation-plan deviation.
Linux NVMe, nightly LazyFS and later fact/forget workloads remain unmeasured here.

# S1 review fixes and 1M G1 (macOS)

Measured on 2026-10-19 after recovery fixes `57ec8ce` and `87506c3`.
This run also includes eviction-counter instrumentation.
Machine: Apple M4, arm64, macOS 27.0.1 build 26A434.
Compiler: `rustc 1.99.1 (58cf8f9c3 2026-10-13)`.
No tests or simulations from this task overlapped the benchmark.

## Review fixes

Corruption proof now uses the first dropped LSN, not information inferred from a damaged footer.
Garbage sectors and valid frames copied over footers cannot hide the next synced transaction.
The tape sim includes both faults.
All footers in a torn group carry the previous group's durable LSN, so they cannot falsely prove that group's damage.

Lookahead skips decoded record runs after checking their footer.
A footer can validate an overlapping suffix after a false start.
Work-count tests cover valid and invalid unproving footers, including 1024-record transactions.
Verification consumes stored record CRC bytes. It does not re-encode records.

Operator-cut tests now cover default-size zero gaps, final preallocation and oversized discarded segments.
The first missing LSN is reported correctly.
Cuts only lower the superblock. They never raise it over retained data that still needs recovery rewriting.
D73 states that rule.
The tape README now documents frame v2 and the paged index.

## Untuned 1M trace

The command matches M2's reference scale and settings:

```sh
G1_TURNS=1000000 G1_REPS=3 G1_HISTORY=1 G1_REPLAY_PROBE=0 G1_CHECKPOINT_MIB=64 ./bench/g1.sh
```

There was 1 run, with 3 interleaved repetitions: bare, Tape, then Engine.
Each path writes 1M timed turns per repetition, using 16 writers and 4-turn durable batches.
Each path contributes 250,000 latency samples per repetition.
The Engine also serves 2 last-16 readers across 1024 sessions.
The default 8 MiB tape-index budget and 64 MiB checkpoint interval were unchanged.
No workload, coalescing or memory settings were tuned.
New counters are printed after timing. They do not add timed metric sampling.
The command returned status 0 and printed a median 1.5x verdict of `PASS`.

| Repetition | Bare p99, ms | Tape p99, ms | Engine p99, ms | Engine/bare | Tape/bare |
|---:|---:|---:|---:|---:|---:|
| 1 | 6.027666 | 7.896292 | 10.101000 | 1.675773 | 1.310008 |
| 2 | 9.126042 | 9.324042 | 10.852833 | 1.189216 | 1.021696 |
| 3 | 8.954167 | 8.925542 | 9.719292 | 1.085449 | 0.996803 |

Ratios are paired within repetitions. Each spread is minimum through maximum, not a pooled quantile.

| Measurement | Engine/bare median [min, max] | Tape/bare median [min, max] |
|---|---|---|
| M2 1M reference | 1.399654 [1.317363, 1.500114] | 1.123347 [1.065021, 1.159804] |
| S1 1M review run | 1.189216 [1.085449, 1.675773] | 1.021696 [0.996803, 1.310008] |

Both S1 median ratios beat M2. This is not an all-repetitions pass.
Engine repetition 1 exceeds 1.5x, and both ratio spreads are wider than M2's.

## Eviction evidence

Counters below are identical across all 3 repetitions.
They are sampled before closing the database, not during reopen.

| Path | Budget, B | Charged resident, B | Evicted pages | Dirty evictions | Derived writeback, B | Page reloads |
|---|---:|---:|---:|---:|---:|---:|
| Tape | 8,388,608 | 8,384,640 | 3876 | 3876 | 15,876,096 | 0 |
| Engine | 8,388,608 | 8,384,640 | 3882 | 3882 | 15,900,672 | 0 |

Each dirty eviction writes 4096 bytes.
The Engine also indexes untimed registration records, which explains its slightly larger page count.
Every repetition therefore exercised dirty eviction at the unchanged default budget.
This recent-window trace did not exercise cold index reloads. Targeted index tests cover those separately.
The counters also verify that failed eviction writebacks are not counted as successful evictions.

## Absolute regression and cause analysis

The paired-ratio improvement is not an absolute latency improvement:

| Path | M2 median p99, ms | S1 median p99, ms | Change |
|---|---:|---:|---:|
| Bare | 6.904625 | 8.954167 | +29.68% |
| Tape | 7.756292 | 8.925542 | +15.07% |
| Engine | 9.664083 | 10.101000 | +4.52% |

Bare p99 varied from 6.027666 ms to 9.126042 ms within this run.
The slower bare denominator contributes to the lower paired ratios.
It does not explain away the higher absolute Tape and Engine p99s.

The bounded index adds about 3,880 dirty writebacks per repetition, absent from the earlier 192k run.
Dirty writeback holds the index mutex, which reads also share.
This adds scale-dependent I/O and a possible source of reader/writer stalls.
There were no cold index reloads, so those cannot explain this run's index I/O cost.
These observations identify added work. They do not isolate its causal share of the latency regression.

Group formation stayed near 16 requests: Tape averaged 15.998 through 16.000, and Engine averaged 15.998.
Batch fragmentation is not supported as an explanation.
Both paths made 43 periodic superblock installs per repetition. The Engine completed 43 checkpoints.
M2 completed 45 of each, so more periodic metadata work is not the explanation either.
Engine median throughput was 13,810 turns/s, close to M2's 13,823.
Tape median throughput fell to 12,766 turns/s from M2's 15,654.
Index writeback cost and shared-machine variation remain plausible contributors. No causal isolation run or tuning was performed.

All 1024 session tails verified after every reopen.
Median clean-open time was 3928.306 ms [3727.210, 4597.007].
The separate history checks returned 16 turns with exactly 16 tape reads at histories of 1024 and 65,536 turns.

## Review validation

Every review-fix commit ran the full command list.
Both sims used seeds 0 through 1,999 before each commit.
The release workspace run includes the real-filesystem stress test skipped by the debug run.

| Command | Result |
|---|---|
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --workspace` | Passed |
| `cargo test --workspace --release` | Passed |
| `ZERODB_SIM_SEEDS=2000 cargo test -p zerodb-tape --release --features sim --test sim` | Passed. 2,000 seeds. |
| `ZERODB_SIM_SEEDS=2000 cargo test -p zerodb-core --release --features sim --test engine_sim` | Passed. 2,000 seeds. All 7 tests passed. |

The prior S1 10,000-seed measurements remain historical. This review pass required 2,000 seeds per sim.
Linux NVMe, nightly LazyFS and later fact/forget workloads remain unmeasured here.
| `cargo bench -p zerodb-core --bench g1 --no-run --quiet` | Passed |
| `G1_TURNS=208000 G1_REPS=3 G1_REPLAY_PROBE=1 ./bench/g1.sh` | Passed. Three alternating probes per checkpoint setting. |
| `G1_TURNS=1000000 G1_REPS=3 G1_HISTORY=1 G1_REPLAY_PROBE=0 G1_CHECKPOINT_MIB=64 ./bench/g1.sh` | Passed. All 9 timed phases, reopen checks, and history checks. Median target PASS. |

All commits are signed off and carry the requested co-author trailer. Nothing was pushed.

| Commit | Change |
|---|---|
| `2cc312d` | Cap checkpoint encoding at 4 MiB, flushing contiguous data chunks. |
| `66c0f23` | Writer publication during checkpoint I/O and mutation-sensitive revision coverage. |
| `53f1ec9` | Explicit Unknown replies after asynchronous mid-step poison. |
| `dae392e` | ZCK2 checkpoint magic with legacy-file full replay. |
| `781d781` | Roll/time/byte superblock requests and recovery-window accounting. |
| `0e273c8` | Remove the pre-manifest directory sync with missing-reference crash coverage. |
| `43b9228` | Replay-budget comparison, metadata counters, and paired Tape/bare reporting. |
| `f297302` | Full-snapshot index/dedup buffer bound and global backing-offset regression. |

# Final checkpoint identity fix

Review base: `67ec44b`.
The requested regression reproduced stale checkpoint acceptance before the fix at seed 37.
It performs a failed save, leaves cleanup removal unsynced, retries the same LSN, and captures a crash after rename.
The stale file ends at a valid record boundary, with a valid header and tenant record but no session page.
The previous name-only manifest accepted that incomplete view as current.

State now reserves monotonic checkpoint attempt IDs before touching a candidate filename.
Failures and filename collisions consume IDs. The counter cannot wrap.
Same-LSN retries use `ckpt-L-1.zdb`, `ckpt-L-2.zdb`, and later unused attempt IDs.
Engine replay fallback preserves the counter instead of resetting it.

Manifest magic is `ZCM2`. Data-file magic remains `ZCK2`.
Every chain entry stores its name, exact byte length, and complete BLAKE3 digest.
Encoding hashes successful buffer writes, including headers and record CRCs.
Loading hashes the existing bounded record-read stream. It does not allocate a whole-file buffer or reread the file.
Length or digest mismatches log a warning and force full replay.
Legacy `ZCM1` manifests are rejected rather than interpreted without seals.
The authoritative tape and durable-ack ordering are unchanged.

Regression coverage:

- 256 seeds exercise failed save, unsynced unlink, same-LSN retry, and post-rename crash.
- Every captured manifest either loads the expected session or requests full replay.
- Failed attempts reserve distinct names, including consecutive `-1` and `-2` suffixes.
- Counter exhaustion returns `ResourceExhausted("checkpoint_attempts")`, without wrapping.
- Both chain positions reject valid-record-boundary truncation.
- Both chain positions reject equal-length replacements with recomputed, valid record CRCs.
- A CRC-valid legacy name-only manifest requests full replay.
- Existing full-compaction, bounded-buffer, cold backing-offset, and checkpoint interleaving tests remain in place.

Performance was not rerun because this request specified correctness validation.
The earlier G1 and reopen timings describe the previous manifest format.

Validation completed before committing the fix:

| Command | Result |
|---|---|
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --workspace --release` | Passed |
| `ZERODB_SIM_SEEDS=2000 cargo test -p zerodb-tape --release --features sim --test sim` | Passed. 18.37 seconds. |
| `ZERODB_SIM_SEEDS=2000 cargo test -p zerodb-core --release --features sim --test engine_sim` | Passed. All 7 tests, 122.45 seconds. |
| `cargo clippy -p zerodb-core --lib --no-default-features -- -D warnings` | Passed |

## S1 index eviction A/B (macOS)

Review round 2 asked whether dirty index-page writeback on the publish path explains the 1M spread.
`G1_LSN_INDEX_MIB` overrides the tape index budget. The default stays 8 MiB.
At 64 MiB the 1M trace never evicts, so the eviction assert only runs at the default budget.

Order: 8 MiB, 64 MiB, 8 MiB, 64 MiB. Same build, same machine, back to back.

```sh
G1_LSN_INDEX_MIB=<8|64> G1_TURNS=1000000 G1_REPS=3 G1_HISTORY=1 G1_REPLAY_PROBE=0 G1_CHECKPOINT_MIB=64 ./bench/g1.sh
```

| Run | Budget | Evictions per rep | Engine/bare p99, per rep | Tape/bare p99, per rep | Bare p99 median, ms | Tape p99 median, ms | Engine p99 median, ms |
|---|---|---:|---|---|---:|---:|---:|
| 1 | 8 MiB | 3,876 tape, 3,882 engine | 1.666, 1.550, 1.045 | 1.298, 0.932, 1.032 | 6.464 | 7.838 | 10.018 |
| 1 | 64 MiB | 0 | 1.513, 1.667, 0.957 | 1.331, 1.406, 0.961 | 7.982 | 9.974 | 10.798 |
| 2 | 8 MiB | 3,876 tape, 3,882 engine | 1.338, 0.866, 1.071 | 1.170, 0.873, 1.040 | 9.385 | 9.120 | 9.042 |
| 2 | 64 MiB | 0 | 0.935, 1.553, 1.016 | 0.864, 1.783, 1.006 | 8.460 | 9.848 | 9.535 |

Findings:
- No eviction does not help. Tape p99 medians are higher at 64 MiB in both runs.
- Repetitions above 1.5x happen with and without eviction. The worst single tape ratio (1.783) had zero evictions.
- Tape throughput overlaps across budgets (12.2K to 23.1K turns/s at 8 MiB, 12.2K to 20.9K at 64 MiB).
- Bare p99 alone moves between 6.0 and 10.4 ms on this machine.

Decision: index writeback stays on the publish path. The spread is macOS `F_FULLFSYNC` variance on this machine.
The 1.5x G1 gate is not judged on macOS. It needs a Linux NVMe run before any G1 claim.

# S2 disk-full overhead (macOS, 192k)

Historical measurements before the baseline fix ran on 2026-10-20 at `4ec356e4ea07af05100dfeb73343d78aba552487`.
This commit includes S2 T1 through T4. T5 changes documentation only.
Machine: Apple M4, arm64, macOS 27.0.1 build 26A434, internal APFS volume.
Compiler: `rustc 1.99.1 (58cf8f9c3 2026-10-13)`.
Available volume space was about 28 GiB before the first run.
This is a shared development machine. No tests or simulations from this task overlapped either timed run.

## Matched command

The command matches [S1's 192k trace](#s1-frame-v2-macos), including its default 4-turn batches and 8 MiB index budget.
Each execution runs 3 interleaved repetitions: bare, Tape, then Engine.
Each path writes 192,000 timed turns per repetition, with 16 writers and 48,000 durable-ack samples.
Engine also runs 2 window readers across 1024 sessions.
The 64 MiB checkpoint threshold, payload sizes, and sync settings are unchanged.
Scratch files stayed under `target/t5-work` through `TMPDIR`.

```sh
G1_TURNS=192000 G1_REPS=3 G1_HISTORY=0 G1_REPLAY_PROBE=0 G1_CHECKPOINT_MIB=64 ./bench/g1.sh
```

Both executions returned status 0. No code, benchmark, or configuration changed between runs.
All 1024 session tails verified after every Engine reopen in both executions.
Each Engine repetition completed 8 checkpoints and 8 periodic superblock installs.
No run was discarded. Nothing was tuned.

## After baseline fix

Measured on 2026-10-20 after `ea30942`, with the bare allocator change in this commit.
Bare now calls `StdIo::allocate`, matching Tape and Engine, including unsupported-filesystem fallback.
Every segment uses the same allocation implementation. No benchmark parameters changed.
Machine and compiler match the historical run above. Available space was about 29 GiB.
Scratch files stayed under `target/review-work` through `TMPDIR`.
No tests or simulations from this task overlapped this timed execution.

The exact matched command above completed 3 interleaved repetitions with status 0.
All 1024 session tails verified after every Engine reopen.
Each Engine repetition completed 8 checkpoints and 8 periodic superblock installs.
No run was discarded. Nothing was tuned.

| Repetition | Bare p99, ms | Tape p99, ms | Engine p99, ms | Engine/bare | Tape/bare |
|---|---:|---:|---:|---:|---:|
| 1 | 13.873750 | 14.091334 | 10.107500 | 0.728534 | 1.015683 |
| 2 | 8.916750 | 13.642166 | 12.424125 | 1.393347 | 1.529948 |
| 3 | 11.348292 | 14.453208 | 13.058625 | 1.150713 | 1.273602 |
| Median | **11.348292** | **14.091334** | **12.424125** | **1.150713** | **1.273602** |

Ratios pair paths within each repetition. Their medians are not ratios of absolute medians.
The macOS result is noise-dominated and shows no measurable overhead signal.
Engine beats bare in repetition 1 at 0.728534x, exposing the storage variance in these ratios.
The 1.5x G1 gate is judged only on Linux NVMe, not macOS.
Linux NVMe and nightly LazyFS remain pending.
Historical S1 and S2 ratios below used a sparse bare baseline and are not matched to this corrected run.
The corrected trace establishes a fair local comparison, not an isolated floor-check cost.
Shared-machine storage variance remains visible in bare's 8.916750-13.873750 ms p99 range.
No production tuning follows from this result.

## Before baseline fix

Bare used sparse `set_len`; Tape and Engine reserved blocks.
These historical comparisons do not satisfy G1's same-preallocation contract.
They cannot isolate disk-floor overhead.

### S1 versus S2

Absolute p99 values are milliseconds per durable batch acknowledgement.
Ratios pair each path with bare within the same repetition.
Median ratios are medians of those paired ratios, not ratios of absolute medians.
The 2 historical S1 executions remain separate.

| Run | Repetition | Bare p99, ms | Tape p99, ms | Engine p99, ms | Engine/bare | Tape/bare |
|---|---:|---:|---:|---:|---:|---:|
| S1 first | 1 | 6.055417 | 7.022292 | 8.020084 | 1.324448 | 1.159671 |
| S1 first | 2 | 5.175958 | 7.073500 | 12.563459 | 2.427272 | 1.366607 |
| S1 first | 3 | 6.473458 | 7.968041 | 11.004625 | 1.699961 | 1.230879 |
| S1 first | Median | **6.055417** | **7.073500** | **11.004625** | **1.699961** | **1.230879** |
| S1 repeat | 1 | 6.839791 | 6.869625 | 9.054833 | 1.323846 | 1.004362 |
| S1 repeat | 2 | 5.452459 | 5.854584 | 5.934042 | 1.088324 | 1.073751 |
| S1 repeat | 3 | 6.252833 | 8.037333 | 8.677084 | 1.387704 | 1.285391 |
| S1 repeat | Median | **6.252833** | **6.869625** | **8.677084** | **1.323846** | **1.073751** |
| S2 first | 1 | 6.063208 | 9.120875 | 9.456291 | 1.559618 | 1.504299 |
| S2 first | 2 | 6.616042 | 7.733500 | 12.381375 | 1.871417 | 1.168901 |
| S2 first | 3 | 11.779875 | 9.770250 | 11.184666 | 0.949472 | 0.829402 |
| S2 first | Median | **6.616042** | **9.120875** | **11.184666** | **1.559618** | **1.168901** |
| S2 repeat | 1 | 6.000625 | 7.269000 | 9.978708 | 1.662945 | 1.211374 |
| S2 repeat | 2 | 6.080250 | 6.812708 | 9.049500 | 1.488343 | 1.120465 |
| S2 repeat | 3 | 6.776750 | 12.091208 | 12.768125 | 1.884107 | 1.784219 |
| S2 repeat | Median | **6.080250** | **7.269000** | **9.978708** | **1.662945** | **1.211374** |

### Historical repeat trigger and verdict (before baseline fix)

The comparison baseline is S1's final unchanged 192k repeat, not its later 1M trace.
Its Tape p99 spread is 2.182749 ms. Its Engine spread is 3.120791 ms.
Spread means maximum minus minimum across that execution's 3 repetitions.
S2's first Tape median rose 2.251250 ms, exceeding its S1 spread by 0.068501 ms.
That triggered 1 full unchanged repeat. The first Engine median rose 2.507582 ms, within its S1 spread.

After repeating, Tape's median increase is 0.399375 ms, or 5.81%.
Engine's median increase is 1.301624 ms, or 15.00%.
Both absolute increases fit S1's repetition spreads and macOS's observed 6-10 ms fsync swings.
S2's first bare p99 ranged from 6.063208 to 11.779875 ms, showing even wider baseline noise.
The repeat's absolute p99 differences are consistent with noise. They don't establish a stable floor-check regression.
This trace doesn't isolate the floor check's cost from preallocation or unrelated storage variance.

The paired ratios still deserve caution.
Engine/bare rose 0.339099x from S1's unchanged-repeat median, exceeding its 0.299380x ratio spread.
Tape/bare rose 0.137623x, within its 0.281029x ratio spread.
S1's first Engine/bare median was already 1.699961x, above either S2 median.
Both S2 executions printed local `FAIL` against 1.5x. This is not a macOS gate failure.
The 1.5x G1 gate is judged on Linux NVMe only. Linux and nightly LazyFS remain pending.
No production change follows from these noisy measurements.

### Historical validation (before baseline fix)

These commands ran after both timed executions.

| Command | Result |
|---|---|
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo test --workspace` | Passed. 145 tests passed; 3 slow debug checks ignored. |
| `G1_TURNS=192000 G1_REPS=3 G1_HISTORY=0 G1_REPLAY_PROBE=0 G1_CHECKPOINT_MIB=64 ./bench/g1.sh` | Passed twice. All 18 timed phases and 6 Engine reopen checks completed. Local performance verdict: FAIL twice. |

The workspace command doesn't enable the core simulation feature. The tape seeded schedule stays ignored in debug.
Both disk-full integration scenarios and the core health test passed.
The seeded release simulations weren't rerun for this documentation-only task.

## S2 index reservation regression on Linux NVMe (2026-10-21)

Same VM and disk as the S1 Linux run: Azure Standard_L8s_v3, Ubuntu 24.04, kernel `7.0.0-1017-azure`, local NVMe, write cache `write through`.
The disk was reformatted with `mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0` to rule out lazy inode init.
Every run used the asymmetric trace: only Engine runs window readers.

Command per commit:

```sh
G1_TURNS=1000000 G1_REPS=3 G1_HISTORY=0 G1_REPLAY_PROBE=0 G1_CHECKPOINT_MIB=64 ./bench/g1.sh
```

Tape durable ack p99 per repetition, 1M turns:

| Commit | What it is | Tape p99, ms | Tape/bare p99 median | Engine/bare p99 median |
|---|---|---|---:|---:|
| `911ffc0` | S1 merge | 0.67, 0.66, 0.65 | 1.411 | 4.012 |
| `4c1e1a4` | S2, before index reservation | 0.65, 0.64, 1.03 | 1.468 | 4.130 |
| `45eae2b` | S2 merge | 1.42, 1.46, 2.31 | 3.394 | 5.260 |
| `ee15bf4` | main before this fix | 1.73, 2.53, 2.18 | 4.839 | 4.314 |
| `1ce8296` | this fix, 2 runs of 3 reps | 0.66, 0.64, 0.65, 0.65, 0.64, 0.65 | 1.464, 1.467 | 2.649, 2.732 |

At 192k turns no commit regressed. Tape p99 stayed near 0.70 ms. The cost grew with index size.

Cause: `e6c9db3` reserved LSN index backing one 4 KiB page at a time.
Each growth called `fallocate(0, len)` and `set_len` on the whole file, about 6000 times per 1M LSNs.
That ran in the writer path, and the size changes rode along in the ext4 journal commit forced by the segment fsync.
`1ce8296` reserves in 1 MiB chunks. A test that primes 131,072 LSNs now makes 3 index allocations instead of 258.

Engine with the fix, 6 repetitions: p50 0.66 to 0.72 ms, p99 1.16 to 1.24 ms, p99.9 2.19 to 2.28 ms.
At S1 the Engine p99 was 1.84 to 2.05 ms and p99.9 was 4.0 to 4.3 ms.

Verdict: the Linux NVMe 1.5x gate still fails. Engine/bare p99 is about 2.7x.
Tape alone is under 1.5x. The remaining gap is in the Engine layer, about 0.55 ms at p99.

## Zero-filled spares and agent concurrency (2026-10-22)

Azure Standard_L8s_v3, centralindia zone 3, Linux 7.0, ext4. G1 at `G1_WRITERS` 1, 4 and 16, batch 4, symmetric readers, medians of 3.
Engine durable ack, `main` before (`68654ab`) and after (`fdf4225`, PR #19) zero-filled spare segments.

Local NVMe, 200k, 400k and 1M turns:

| Writers | p50 µs | p99 µs | p99.9 µs | Turns/s |
|---|---|---|---|---|
| 1 | 157 → 67 | 193 → 193 | 241 → 219 | 25.3k → 48.8k |
| 4 | 231 → 141 | 292 → 280 | 760 → 646 | 68.5k → 105.7k |
| 16 | 585 → 432 | 1,103 → 910 | 2,157 → 1,896 | 104k → 138k |

Attached managed disks, host caching off, 20k, 20k and 80k turns:

| Disk | Writers | Engine p50 µs | Engine p99 µs | Turns/s | Engine/bare p50 |
|---|---|---|---|---|---|
| Premium SSD v2, 128 GiB, 3,000 IOPS | 1 | 1,908 → 1,883 | 3,208 → 3,167 | 1.98k → 2.01k | 1.01 |
| | 4 | 1,995 → 1,898 | 3,114 → 3,066 | 7.7k → 7.9k | 1.03 |
| | 16 | 2,355 → 2,096 | 3,945 → 3,768 | 26.9k → 34.5k | 1.03 |
| Premium SSD P30, 1 TiB | 1 | 4,464 → 4,423 | 6,733 → 6,854 | 870 → 874 | 1.01 |
| | 4 | 4,617 → 4,650 | 6,532 → 7,207 | 3.38k → 3.34k | 1.03 |
| | 16 | 5,329 → 4,950 | 9,248 → 9,279 | 11.6k → 15.0k | 1.05 |

On managed disks an ack costs one device write round trip: about 1.9 ms on Premium SSD v2 and 4.4 ms on P30.
The engine adds 1% to 5% over a bare `pwrite` plus `fdatasync`. Zero-fill does not change 1 or 4 writers there, and adds 28% throughput at 16.
On local NVMe the round trip is about 60 µs, so the journal commit that zero-fill removes was most of the ack.
Ultra Disk was not available in zone 3.

## #52 writer hot path A/B (2026-10-24)

Azure Standard_L8s_v3, local NVMe, ext4. G1 at 16, 64 and 128 writers, 5 reps each.
Systems ran interleaved in one VM session, because of #48. Redis is the control.
Ratio is the median of per-rep throughput ratios to `main` (`8a66c83`), with min to max in brackets.
Writer µs/append is writer thread CPU time per append.

| Writers | System | Turns/s | p99 µs | p99.9 µs | Writer µs/append | Ratio |
|---|---|---|---|---|---|---|
| 16 | main | 48,587 | 508 | 696 | 15.12 | 1.000 |
| | hash once (#55, first version) | 50,471 | 448 | 536 | 14.25 | 1.041 |
| | pwrite (#54) | 48,886 | 510 | 666 | 14.88 | 1.006 |
| | reply wakeups (#53) | 48,435 | 520 | 676 | 12.75 | 1.002 |
| | Redis | 42,612 | 574 | 643 | - | 0.926 |
| 64 | main | 64,135 | 1,782 | 2,899 | 12.06 | 1.000 |
| | hash once | 67,028 | 1,316 | 2,078 | 11.25 | 1.051 (1.037-1.053) |
| | pwrite | 64,791 | 1,774 | 3,034 | 11.84 | 1.011 (1.004-1.013) |
| | reply wakeups | 65,022 | 1,790 | 3,278 | 9.03 | 1.013 (1.004-1.016) |
| | Redis | 73,970 | 1,610 | 1,717 | - | 1.144 |
| 128 | main | 66,550 | 3,563 | 8,702 | 11.16 | 1.000 |
| | hash once | 69,458 | 2,611 | 8,346 | 10.34 | 1.044 (1.027-1.062) |
| | pwrite | 67,178 | 3,606 | 8,441 | 10.97 | 1.010 (1.008-1.014) |
| | reply wakeups | 68,116 | 3,481 | 8,095 | 8.14 | 1.028 (0.990-1.037) |
| | Redis | 79,116 | 3,134 | 3,304 | - | 1.195 |

Writer syscalls at 128 writers, 640k appends, from `perf stat`:

| System | write | lseek | pwrite64 | futex | fdatasync |
|---|---|---|---|---|---|
| main | 641,771 | 641,771 | 0 | 803,469 | 8,284 |
| pwrite | 0 | 0 | 641,771 | unchanged | unchanged |
| reply wakeups | unchanged | unchanged | 0 | 89,631 | 11,186 |
| hash once | unchanged | unchanged | 0 | 775,534 | unchanged |

The hash-once rows above are #55's first version. Review moved its shard bits and kept the hash in each entry. That version, `e3d20e7`, against the same `main` in a later session:

| Writers | Turns/s main → #55 | Ratio | p99 µs | p99.9 µs | Writer µs/append |
|---|---|---|---|---|---|
| 16 | 49,065 → 52,004 | 1.065 (1.052-1.085) | 500 → 436 | 668 → 521 | 14.88 → 13.50 |
| 64 | 65,406 → 70,120 | 1.073 (1.062-1.078) | 1,727 → 1,259 | 3,026 → 1,907 | 11.75 → 10.53 |
| 128 | 68,813 → 74,532 | 1.080 (1.079-1.086) | 3,397 → 2,428 | 8,294 → 8,104 | 10.81 → 9.61 |

Premium SSD v2, 128 GiB, 3,000 IOPS, host caching off, 2,000 appends per writer, same method, with #55 at `e3d20e7`:

| Writers | System | Turns/s | p99 µs | p99.9 µs | Ratio |
|---|---|---|---|---|---|
| 16 | main | 9,588 | 2,590 | 3,168 | 1.000 |
| | hash once | 9,785 | 2,392 | 3,019 | 1.038 (0.969-1.041) |
| | pwrite | 9,629 | 2,522 | 3,277 | 1.009 (0.987-1.034) |
| | reply wakeups | 10,285 | 2,418 | 2,963 | 1.069 (1.047-1.093) |
| | Redis | 5,162 | 5,085 | 5,675 | 0.548 |
| 64 | main | 31,480 | 3,610 | 5,129 | 1.000 |
| | hash once | 32,773 | 3,538 | 5,085 | 1.046 (0.926-1.066) |
| | pwrite | 32,505 | 3,334 | 4,711 | 1.020 (0.892-1.547) |
| | reply wakeups | 32,085 | 3,189 | 5,223 | 1.019 (0.886-1.222) |
| | Redis | 21,403 | 5,864 | 7,797 | 0.674 |
| 128 | main | 44,515 | 5,124 | 12,150 | 1.000 |
| | hash once | 47,633 | 4,661 | 12,844 | 1.075 (1.065-1.080) |
| | pwrite | 44,964 | 4,956 | 9,625 | 1.015 (1.002-1.019) |
| | reply wakeups | 44,990 | 4,650 | 11,732 | 1.018 (1.000-1.023) |
| | Redis | 34,935 | 7,078 | 9,892 | 0.788 |

Hashing once gives the most: 6.5% to 8% on NVMe, with p99 27% to 29% lower at 64 and 128 writers. Review's fixes added about 2 points over the first version.
Reply wakeups cut writer futex calls 9x and writer CPU per append about 27%. NVMe throughput gains only 1% to 3%: groups get smaller, so `fdatasync` calls rise, and the load client shares the 8 vCPUs. On Premium SSD v2 they add 7% at 16 writers.
`pwrite` halves write syscalls for 1% to 2%. So syscalls are not the bottleneck, and coalescing a group into 1 write is deferred.
No change regresses p99 on Premium SSD v2. Its 64-writer runs were noisy for every system.
Redis is still 10% to 21% ahead on NVMe at 64 and 128 writers. On Premium SSD v2 zerodb leads, because each Redis ack pays 2 device round trips.
VM time was about 20 minutes for the first NVMe session and about 18 for the second plus Premium SSD v2, roughly $0.55 in all.
