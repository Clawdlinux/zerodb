# 0002: zerodb decision log

Round 1 review: Skeptic, Constraint Guardian, User Advocate (Claude Opus 5.5). Designer and arbiter: lead agent. Objection ids match the review.

| Id | Decision | Alternatives considered | Why |
|---|---|---|---|
| D1 | Tape is WAL, store and provenance chain at once | Separate WAL plus store; LSM | One commit point. Fewest moving parts. Research: single append file with group commit is the simplest durable path. |
| D2 | Session index by arithmetic over paged offset vectors | B-tree, skip list, learned index | Dense seqs make lookup arithmetic. O(returned) reads. |
| D3 | v0.1 vectors are exact scan, IVF is gated | HNSW, IVF-flat, RaBitQ | Exact recall, no drift, little code. Add IVF only if G2 latency fails. (S13, C10) |
| D4 | Scope cut: no local embedder, FSST, TS SDK, MCP HTTP, custom hash table or timing wheel in v0.1 | Ship everything in v0.1 | Every component must serve G1 or G2. "From scratch" means storage and index code, not std collections. (S13, C10) |
| D5 | Forget is logical at commit, physical only via compaction into husks | In-place zeroing; rewrite CRC in place | Sealed segments never change. No crash state can refuse open. (S1) |
| D6 | Salted content digest `BLAKE3(salt or content)`, salt erased with payload | Unsalted digest; keyed digest with external key store | Forgotten low-entropy content cannot be brute-forced. No extra key store. (C5) |
| D7 | Per-tenant hash chains | One global chain | Tenant verify and export never touch other tenants. (C5) |
| D8 | SUPERBLOCK durable LSN, strict below, truncate above | Strict everywhere; truncate everywhere; sync superblock per commit | Correct under reordered unsynced writes. Bit rot window bounded to about 1 s. No extra sync on the ack path. (S2) |
| D9 | CRC covers length; `len == 0` ends data | CRC over payload only | Corrupt length detected. Preallocated tails handled. (S2) |
| D10 | Pre-created segments with durable dir entries; MANIFEST via rename | Create on roll | No ack depends on an unsynced directory entry. (S3) |
| D11 | Compaction order: new segment, sync, dir sync, checkpoint, manifest, delete | Delete first; swap pointers in memory | Every crash point recovers to old or new state. (S3) |
| D12 | Visibility bounded by durable LSN; one pinned snapshot LSN per query | Read page cache tail; per-index watermarks only | No reader ever sees a write that a crash removes. Consistent fusion. (S4, S9) |
| D13 | `fast` mode only for cache ops, marked `durable: false` | Allow fast turns; keep `group` mode | Cache UX like Redis when asked, never for history or search. `group` merged into `durable`. (S4, U3) |
| D14 | Recovery replays from minimum watermark across all views; checkpoints every 64 MiB or 60 s | Replay active segment only | Bounded open time, no missed records across rolls. (S5) |
| D15 | Index lag cap back-pressures append | Unbounded lag | Bounded delta scans. (S5, C1) |
| D16 | Record id `(tenant, lsn)`; locations resolved through indexes | `(segment, offset)` ids | Ids survive compaction. (S6) |
| D17 | Husks keep header and digest of dropped records | Per-segment anchors | Chain stays verifiable after compaction. (S6) |
| D18 | Tombstones dropped only by a pass covering all older segments | Drop when compacted | Acked deletes never resurrect. (S6) |
| D19 | Writer assigns LSN and seq at write time; txn never spans segments; max batch 4 MiB or 1024 records | Assign at enqueue | Dense seqs, simple recovery. (S7) |
| D20 | Idempotency keys with a bounded dedup table | At-least-once only | Safe retry after `unknown`. (S7, U4) |
| D21 | Fact supersession in LSN order; key scope tenant or tenant/agent; commit time is valid_from | Client time; session-scoped keys | Deterministic history. Cross-session preferences work. (S8, U5) |
| D22 | Forgetting the current fact leaves no current value | Revive previous fact | Never resurrect data the user removed. (S8) |
| D23 | Pub-sub post-commit, at-most-once live, resume by LSN, bounded buffers, `Lagged` on overflow | Durable queues; blocking delivery | Writer never blocks. Crash recovery by replay. (S10) |
| D24 | Wall-clock TTL, documented; SimClock in tests | Monotonic-only TTL | TTL must survive restart. Clock skew risk accepted. (S11) |
| D25 | Seeded single-threaded scheduler drives the core in tests; added fault classes; LazyFS nightly | Real threads under SimDisk | Reproducible failures. Real filesystem coverage. (S12) |
| D26 | Writer thread does framing, CRC, chain, write, sync only; digests on caller threads | Index on writer | Keeps G1 honest on fast-sync hardware. (C1) |
| D27 | G1 on Linux NVMe vs bare `sync_data` baseline with same preallocation | Any hardware | Slow disks hide overhead. (C1) |
| D28 | Geometric session pages with a global hot-page budget | Fixed 4096-slot pages | 128 bytes for a 3-turn session instead of 32 KiB. (C2) |
| D29 | Every in-RAM structure bounded; typed `ResourceExhausted` | Unbounded | No OOM that kills every tenant. (C3) |
| D30 | Forget reports logical and physical completion; recomputes stats; docs list remanence limits | Claim full erasure | Honest. Covers snapshots, old segments, caches. (C4, U6) |
| D31 | Server binds localhost; token maps to tenant and agent | Tenant in request | Blocks cross-tenant reads, including via prompt injection. (C6, U7) |
| D32 | Signed checkpoints carry key id; verify reports unsigned tail; claim stated relative to key holder | Implicit trust | Honest provenance claim. (C7) |
| D33 | Disk reserve, `DiskFull` before reserve, compaction and forget off the writer thread | Fill disk then fail | No compaction deadlock. (C8) |
| D34 | No `libc` in v0.1 | Allow `libc` for fallocate | `set_len` and std `sync_data` suffice. (C9) |
| D35 | Per-milestone LOC budgets | No budget | Keeps code small. (C10) |
| D36 | Exclusive data dir lock; MCP stdio proxies to the running server | Advisory only | Directly fixes the MCP multi-process data loss. (U1) |
| D37 | Visibility stated per channel; response counts records without vectors | Single blanket claim | No hidden gaps. (U2) |
| D38 | Write errors carry `committed`, `not_committed` or `unknown`; poisoned engine serves reads | Generic errors | Clients know when to retry. (U4) |
| D39 | Window shows redacted placeholders; facts with forgotten sources flagged | Skip seqs silently | Predictable output. (U6) |

## Round 2 (review of revision)

| Id | Decision | Why |
|---|---|---|
| D40 | Sync every file written since last sync before ack | Group can span a roll. (N1) |
| D41 | End of data or missing segment below superblock LSN is corruption | No silent loss of synced data. (N2) |
| D42 | Chain over canonical header fields only; husks keep them | Husks verify. (N3) |
| D43 | Cache records unchained, dropped by compaction with no husk; husk ranges collapse below signed checkpoints | Bounded disk under cache churn. (N4) |
| D44 | Publish installs session slot, cache entry, current-fact pointer before ack | Read-your-writes and O(returned). Only BM25 and vectors lag. (N5) |
| D45 | Forget and retention sets read at current durable LSN for `as_of` | No resurrection. (N6) |
| D46 | `fast` writes expose no LSN, id or event until durable | LSNs never reused in any visible way. (N7) |
| D47 | Auth on by default; no-auth needs loopback plus Host and Origin checks | Blocks local and DNS-rebinding access. (N8) |
| D48 | Lag wait max 5 s then `IndexLagExceeded`; dead worker sets `degraded` | No hung appends. (N9) |
| D49 | Idempotency scope `(tenant, session)`, payload digest check, count-based window | No cross-tenant replay. (N10) |
| D50 | Real-thread stress test besides the simulated scheduler | Covers production concurrency code. (N11) |
| D51 | Incremental checkpoints with bounded I/O per interval | Checkpoint cost stays flat. (N12) |
| D52 | Memory bounds table for every structure | Closes C3. |

## Evidence-driven additions (Reddit and X synthesis)

| Id | Decision | Why |
|---|---|---|
| D53 | `trust` label per record; untrusted excluded from recall by default; token caps trust | Persistent-memory poisoning threads. |
| D54 | Per-namespace `retain_for` on turns and facts | Users want memory that expires. |
| D55 | Search hits explain ranks, trust, validity and sources | Opaque memory is hard to debug. |
| D56 | Markdown import and export in M6 | Markdown wikis beat memory frameworks in a 2,176-task test users cite. |
| D57 | Evaluation contract with markdown and SQLite baselines | Benchmarks people can reproduce. |

## Round 3 (arbiter)

| Id | Decision | Why |
|---|---|---|
| D58 | `window` never filters by trust; recall excludes other agents' untrusted writes; own writes always recalled; `trust_change` records promote | Keeps O(returned), valid transcripts and first-use round trip. (B1) |
| D59 | `zerodb init` issues an owner token allowed to write `trusted` | Plug-and-play default. (B1) |
| D60 | Retention per tenant or agent, policy at record LSN, `expired` placeholders | No namespace on turns, no revival. |
| D61 | Goal 4 narrowed to turns and facts | Cache is CRC only. |

Arbiter disposition: APPROVED once D58 to D61 landed, with carry items listed in 0001.

## Rejected

- Dependency invalidation of derived facts (expire facts when sources change): deferred to after v0.1. Facts already flag `source_redacted`. Full invalidation needs a dependency graph, which is not needed for G1 or G2.

## Slim v0.1 (0003)

| Id | Decision | Why |
|---|---|---|
| D62 | Remove hash chains, salted digests, husks and signing | No user evidence asked for tamper evidence. They complicated compaction and forget |
| D63 | Remove Fast durability, Cache kind, pub-sub, trust labels from v0.1 | No evidence. Fast mode broke "acked means durable" |
| D64 | Frame v2: 40-byte header, location-seeded CRC32C | Same integrity class as SQLite and Postgres, without cryptography |
| D65 | Footer binds `first_lsn`, `record_count`, `records_crc`, `synced_lsn` | Atomicity without batch digests. `synced_lsn` proves corruption above the superblock without a counter |
| D66 | Dense record LSNs and txns; footers consume no LSN | Simple recovery checks and paged LSN arrays |
| D67 | Every write carries an `idem_key` | Safe retries over HTTP and MCP, including forget and fact delete |
| D68 | Facts scoped per tenant, prefixes for agent-local keys | Avoids split-brain facts between agents |
| D69 | Forget is "hide, not erase" with an LSN cutoff | Evidence asks to stop recall, not forensic erase. Honest naming |
| D70 | Markdown import deferred | Direct fixes cover the evidence |
| D71 | `rustix =1.1.2` in zerodb-tape, defaults disabled, features `fs` and `std` | Replaces D34. `fs` provides statvfs and safe Linux and macOS fallocate. `std` supports file handles and error conversion. On macOS, fallocate tries contiguous F_PREALLOCATE, then F_ALLOCATEALL, and calls ftruncate. Unsupported preallocation and other platforms use sparse set_len. Equal or smaller macOS lengths skip preallocation |
| D72 | Build in steps S1 to S6, each with two independent reviews and real use | Avoid designing ahead of evidence |
| D73 | Operator truncation accepts 0 or a whole transaction boundary. Interior cuts name both nearest boundaries and fail. | I2 forbids partial transactions. Never round silently. |
| D74 | G1 runs the same reader load on bare, Tape and Engine by default. `G1_SYMMETRIC=0` reproduces earlier engine-only reader traces. | Ratios should not mix reader contention into one side only |
| D75 | S3 serves HTTP on loopback TCP (default `127.0.0.1:7425`) and on a 0600 Unix socket (0660 with an explicit group), with the same token auth. The `Host` check applies to TCP only. Non-loopback TCP is refused until TLS exists. The data directory must be 0700. Only the lock holder installs the socket, from a per-start staging directory, under a `flock` on `<path>.lock`, and a server removes only its own socket. `zerodb mcp` discovers a server only through the Unix socket in `LOCK`, after StrictModes-style ownership checks and an instance id match. `--url http://` takes loopback only. zerodb runs on the host or in a sidecar, never in a disposable sandbox. No container image in S3. New crates: `serde_json =1.0.154` (defaults off, `std`, `Value` only) and `httparse =1.10.1` (defaults off). The zerodb crate uses `rustix =1.1.2` with `event`, `fs`, `net`, `process` and `std`. `process` is new, for `geteuid`. `event` is new, so accept loops poll with a 50 ms timeout and stop without a wake-up connection to a socket path that may be gone. `net` is new, for a nonblocking Unix connect that a full listen backlog cannot hold past the call's deadline, which std's blocking `UnixStream::connect` cannot bound. HTTP framing is std-only. Clients never downgrade an outcome. A call whose attempts never sent a byte is `not_committed`. The retry deadline is the one timeout, and no attempt connects or sends with under 2 s left, the first included. Exit codes: 75 when the lock is held, 70 on poison with `--exit-on-poison` | Guests cannot reach host loopback. Firecracker vsock and runc or gVisor bind mounts all end at a host Unix socket, so one listener covers them. Guest storage can drop flushes or vanish. `hyper` needs an async runtime and `tiny_http` has had no release since 2022 and still needs a client. A `not_committed` after an unclear attempt can hide a durable write. The `LOCK` TCP address and the instance id are public, so only an owner-checked socket earns the token |
| D76 | S4 facts. One fact per transaction, after an optional `TenantCreate`. Facts never share a batch with turns. The fact index is one `BTreeMap` from `(tenant, key)` to LSN pointers with tombstone bits and a per-key dropped bit. `max_facts` caps keys (default 100,000, global, tombstones keep their slots). Checkpoint load and replay enforce it like `max_sessions`, so a lowered cap makes open fail until it is raised. `fact_history` (default 16, 1 to 4,096) bounds history. A key keeps at most `fact_history + 2` pointers: one more to report `truncated`, and one for readers at the previous durable LSN. Checkpoint tags 4 and 5 landed in the same commit that applies facts. Tag 4 records the `fact_history` it was written under and the dropped bit. A larger `fact_history` at open falls back to full replay. A checkpoint with most keys dirty walks the whole map. The current value is the newest revision by LSN, and a tombstone means none. The current LSN is 0 when there is no current value. `if_lsn` reaches prepare through an opaque, unframed `PreparedBatch::application_arg`, and is checked after the retry lookup against the staged overlay and then the published index. A mismatch is `Conflict`, `not_committed`, HTTP 409 `conflict`. Retries reuse the dedup table with a session or fact scope. The digest covers the payload only. Reads pick the newest revision at or below the durable LSN they observed. A list's `next` is the last key the walk finished with, tombstones included. `source_refs` must be empty: prepare and HTTP reject others. HTTP keeps keys out of paths. MCP exposes `fact_set`, `fact_get` and `fact_list`, without `if_lsn`. No downgrade: an S3 binary aborts replay on a `Fact` frame with `CorruptIndex`. No new dependencies or pins | One key per write keeps I6's `(tenant, key)` scope. Replay cannot recompute a digest that covers `if_lsn`, which is never on the tape. A stripped in-band condition would be a hidden payload format. The machine publishes before the tape stores its durable LSN, so a reader must not trust the newest index entry alone. Keys hold `/`, which collides with path suffixes and proxy decoding. A checkpoint that skipped facts would move the watermark past them |
| D77 | No flush pipelining in the tape writer. The writer stays write, sync, publish, reply, and the 200 µs coalescer stays (removed by D80). Pipelining (Aether, Johnson et al. 2010) would write group N+1 while N syncs | Agents are closed-loop with 1 to 4 writers. All of one group waits on its sync, so nothing arrives to overlap. The model gives about 13% at 16 writers and nothing at 1. A pipelined footer would carry a `synced_lsn` one group behind, so no later footer proves the newest acked group was synced, which weakens I1. Greedy release without pipelining raised engine p99 from 1,105 to 1,239 µs. Positional I/O alone was neutral |
| D78 | Zero-filled spare segments. The tape keeps one `spare.zdb`, built as `spare.tmp`: preallocate, then zeros in 256 KiB chunks with a sync after each, then rename and sync the directory. A roll renames it to `seg-<id>.zdb` and syncs the directory before installing the manifest. With no spare ready it preallocates as before. Open fills the first spare. The maintenance thread refills it after each roll, only while two segments of room stay above the floor. There is no option | On ext4, `fallocate` leaves unwritten extents, so every `fdatasync` also commits the journal. On local NVMe it took engine durable-ack p50 from 153 to 66 µs at 1 writer and turns/s from 25.8k to 49.0k. Every percentile stayed at or below main at 1, 4 and 16 writers. Filling at roll time costs about 50 ms per 64 MiB on the writer thread, so the refill runs in the background. One 64 MiB sync stalled tape syncs for about 30 ms, and 1 MiB chunks raised p99 at 1 writer. 256 KiB chunks did neither. Two segments of room keep the active segment and a concurrent roll above the floor. Open fills synchronously so no fill races the first writes. PostgreSQL's `wal_init_zero` does the same. Copy-on-write filesystems gain nothing and pay one extra segment write |
| D79 | By default, sandboxed agents reach memory through their harness. zerodb runs beside the trusted orchestrator, which picks sessions and fact keys. Untrusted code inside a microVM or container gets no token and no socket. The direct-access recipes in `docs/deploy/sandboxes.md` stay, for code trusted with its tenant or given its own. No scoped tokens for now | Every platform checked keeps memory outside the sandbox: AWS AgentCore Memory, Vertex Memory Bank, Foundry, the OpenAI Agents API and the Claude Agent SDK. Compute is ephemeral and memory must outlive it. A token grants a whole tenant, and `memory_recent` takes any session, so untrusted code or a model holding either could read every session and fact (OWASP Agentic Top 10 2026, ASI06 Memory and Context Poisoning). Holding memory in the harness needs no code. Scoped tokens are the path when untrusted code needs direct access |
| D80 | Smart batching. The tape writer groups everything queued when it wakes and never waits for more. The 200 µs coalescer and `TapeOptions::coalescing_window` are gone. `zerodb load --think <ms>` adds a random 0 to 2x pause per write. `/v1/stats` counts `busy_replies` | PostgreSQL (`commit_delay` 0), RocksDB write groups and the LMAX Disruptor all batch without a timer. Agents call a model between writes, so their arrivals are spread out and a wait only adds latency. Azure L8s v3, medians of 3. NVMe, 64 writers, 5 ms think: coalescer to no wait, p50 332 to 129 µs, p99 560 to 563 µs, p999 1,818 to 1,482 µs. 256 writers, 50 ms think: p50 359 to 122 µs, p99 781 to 579 µs. Closed loop: +28% acks/s at 4 writers, +42% at 16. On the slow OS disk, agent-shaped load kept throughput with lower p99 and p999. The one loss is a closed loop with no think on the slow disk: 16 writers split into 2 cohorts that alternate syncs, 6.2k to 4.6k acks/s. Backpressure is unchanged: 1 request in flight per connection, 256 connections, then 503 `busy` |
| D81 | The dedup lookup is 64 `HashMap` shards picked by the key's hash. A resize rehashes 1/64 of the keys | A tenant keeps up to 1M retry keys. Each doubling of one map rehashed every key under the state lock: about 6 ms at 57k keys, about 100 ms near 1M. At 64 writers with 5 ms think that set p99.9 at 6.3 ms. Shards: 0.4 to 0.8 ms, p50 unchanged at 132 µs, 16-writer acks unchanged. A `BTreeMap` also removed the stalls but cost 10% of acks at 16 writers. Pre-sizing would allocate for 1M keys per tenant up front (#43) |
| D82 | No change to thread handoffs for now. The connection thread hands each append to the writer thread and waits for the reply | Waking an idle vCPU costs a hypervisor exit on Azure. Pinning every server thread to one vCPU gives 1 writer a steady p50 of 71 µs, against 85 to 222 µs unpinned, but 33k instead of 55k acks at 16 writers. Spinning 50 µs before blocking: on the writer's wait, 1 writer 11.2k to 13.5k acks/s with no cost at 16, but no p99 gain at 16 writers or with think. On the reply wait too, 64 spinning threads took CPU from the work: 48k at 16 writers. Neither met #34's bar of a better p99 at 16 and 64 writers. Revisit if single-agent latency becomes the target |
| D83 | No GPU offload. Hardware acceleration stays on the CPU: CRC32C instructions (#16), and the disk | The writer spends 8 to 15 µs of CPU per append (#52 A/B). One GPU kernel launch is about 20 µs ([NVIDIA, Nsight Systems](https://developer.nvidia.com/blog/understanding-the-visualization-of-overhead-and-latency-in-nsight-systems/)), and a small host-to-device copy adds 10 to 50 µs, so offload loses on every append. A group of 80 holds about 1,400 SipHash calls (about 30 µs) and 16 KB of CRC32C (about 2 µs). The rest is syscalls, locks and hash-map updates that must stay with the writer. Throughput is bound by `fdatasync` and the single writer thread, which a GPU cannot shorten. A GPU also breaks the no-new-deps rule, and most agent hosts and sandboxes have none. Revisit if search lands (0003 defers it) and needs local embeddings |

D73 recovery details:

- Validate every retained transaction. Report the first bad LSN as `CorruptionDetected`.
- Persist a CRC-protected truncation intent before changing data.
- Truncate, zero and sync data before lowering and syncing the superblock.
- The superblock is lowered, never raised. Normal recovery rewrites and syncs retained data above that floor.
- Replay an unfinished intent at open. A crash between those steps must recover.
- Report and log the cut LSN, dropped LSN range and dropped byte count.
- Dropped LSNs may be reused, even when acknowledged. This operator escape is the documented exception to I3.
- Recover `commit_time` from the retained tape. It stays monotonic from that point.
