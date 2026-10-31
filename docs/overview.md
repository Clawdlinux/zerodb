# AtticDB overview

How AtticDB stores data, how fast it acks, and the steps that built it.
Start with the [top README](../README.md) to install it.

## How it works

- 1 append-only tape is both WAL and store. Indexes and checkpoints are derived and rebuild from it (D1, I8).
- A zero-filled spare segment is kept ready for the next roll. On ext4 an `fdatasync` is then usually 1 device round trip (D78).
- Group commit never waits on a timer. The writer syncs whatever is queued when it wakes (D80).
- Frames carry location-seeded CRC32C and footers bind whole transactions. Recovery cuts only an unproven tail (D64, D65, D8). Damage inside the last synced group has no later proof, so it can still be cut (I1).
- Windows cost O(returned). Paged arrays, held to a memory budget, map seqs to LSNs and LSNs to offsets (D2, D66, I5).
- Every write carries an `idem_key`, so a retry in the dedup window can't commit twice (D67, I6). A client that can't tell whether a write landed reports `unknown`, never `not_committed` (D75).
- Below a free-space floor the engine goes read-only. Writes are rejected before anything is written. Reads keep working (I14).

D-ids are in the [decision log](design/0002-decision-log.md). I-ids are invariants in [0003](design/0003-slim-v0.1.md), which defines the current scope.
CI runs both SimDisk crash simulations on 2,000 seeds. Anyone with write access to the data directory can edit history. Nothing detects it.

## Numbers

Durable appends, `fsync` before every ack, Azure L8s v3. Medians of 7 interleaved reps, 1 client for every server. Full tables: [bench/H2H.md](../bench/H2H.md).

| Local NVMe, 16 writers, closed loop | atticdb | redis | valkey | dicedb |
|---|---|---|---|---|
| Acks/s | 48,611 | 44,572 | 42,928 | 44,693 |
| p99 µs | 502 | 549 | 573 | 547 |

Of the 4 servers, AtticDB has the best closed-loop acks/s and p99 on both disks. On Premium SSD v2 it beats Redis, Valkey and DiceDB in every cell. At 16 writers it acks 9,328/s against 5,086 to 5,151. On local NVMe it's mixed:
- Closed loop, 1 and 4 writers: AtticDB beats Redis, Valkey and DiceDB on every median. It acks 9,799 and 24,516/s, against at most 8,436 and 18,444.
- Closed loop, 16 writers: Redis and DiceDB have a lower p50, 299 and 300 µs against 305. AtticDB's p99.9 and max are the worst of the 4: 674 and 1,210 µs. The others get 604 to 624 and 810 to 947.
- Agent-shaped, 64 writers at 5 ms think and 256 at 50 ms: best p50, 148 and 138 µs. The others get 182 to 184 and 140 to 145. Its p99.9 is best too: 786 and 736 against 964 to 1,053 and 2,103 to 2,758.
- AtticDB's agent-shaped p99 median trails. At 64 writers it's 416 µs against Redis's 382. At 256 it's 558 against DiceDB's 348 and Valkey's 496. [#47](https://github.com/Clawdlinux/atticdb/issues/47) tracks it.
- Those medians sit inside run-to-run noise. At 256 writers AtticDB's p99 ranged 225 to 636 µs and DiceDB's 289 to 1,078. Per rep in the TSVs, AtticDB's 256-writer p99 beat Redis in 7 of 7. It beat Valkey in 4 and DiceDB in 5.
- At 256 writers AtticDB's max is the worst, 8,698 µs against 7,392 to 8,039. Acks/s ties, since think time sets the rate. AtticDB's median is lowest, by 0.5 to 1.6 acks/s.

In-process SQLite wins some cells. At 1 writer on NVMe it acks 19,410/s against AtticDB's 9,799. Its p50 is lower at every NVMe load: 32 µs at 1 writer, 57 and 68 under agent load. On NVMe its p99 is lower at 4 and 16 writers, and its p99.9 at 16. On Premium SSD v2 its closed-loop p50 is lower, and its p99 and p99.9 at 4 and 16.

## Steps

| Step | Scope |
|---|---|
| S1 | Frame v2, guarded recovery, bounded tape index. |
| S2 | Disk-full floor and real preallocation. |
| S3 | Shared server, auth, HTTP, MCP and Python client. |
| S4 | Tenant-scoped fact revisions and conditional updates. |
| S5 | Hide requests and markdown export. |
| S6 | Dogfood and measure the remaining gaps. |
