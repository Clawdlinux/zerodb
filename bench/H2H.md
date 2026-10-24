# Head-to-head: durable appends

Run on 2026-10-24. Raw rows: [h2h-nvme.tsv](h2h-nvme.tsv), [h2h-premium-v2.tsv](h2h-premium-v2.tsv), and the agent-shaped `h2h-*-think5.tsv` and `h2h-*-think50.tsv`. Runbook: [h2h.sh](h2h.sh).

![Durable acks per second and p99 latency, by disk and writer count](h2h.svg)

Redraw with `python3 bench/h2h_chart.py`. The chart shows the closed-loop runs.

## The question

How fast can each system acknowledge a write that survives power loss? Every client has 1 request in flight, and every system acks only after `fsync`.

2 loads:
- **Closed loop.** Each writer sends its next append as soon as the last one is acked. This finds the ceiling.
- **Agent-shaped.** Agents call a model between writes. Each writer sleeps a uniform 0 to 2x the think time before every append. The sleep is seeded by the writer's index, so every system sees the same arrivals.

## Setup

- Azure Standard_L8s_v3 (8 vCPU), Linux 7.0, ext4.
- 2 disks: the VM's local NVMe, and a Premium SSD v2 managed disk. The managed disk has 128 GiB, 3,000 IOPS, 125 MB/s and host caching off.
- Closed loop: 5,000 appends of 256 bytes per writer, at 1, 4 and 16 writers.
- Agent-shaped: 1,000 appends per writer at 64 writers with 5 ms think. 100 per writer at 256 writers with 50 ms think.
- 7 reps, interleaved. Each rep runs all 5 systems before the next rep starts. Every run gets a fresh data directory.
- Tables show medians over the 7 reps. "p99 range" is the lowest to the highest p99 across them.
- Docker and the [monitoring](monitoring/README.md) stack were stopped.

| System | Version | Durability setting | Workload | Client |
|---|---|---|---|---|
| zerodb | `02b43e5` | Always: group commit, ack after `fdatasync` | `POST /v1/append`, 1 session per writer | [h2h_paced.py](h2h_paced.py), HTTP over the Unix socket |
| Redis | 8.10.2 | `appendonly yes`, `appendfsync always` | `RPUSH`, 1 key per writer | [h2h_paced.py](h2h_paced.py), RESP over the Unix socket |
| Valkey | 9.1.2 | Same | Same | Same |
| DiceDB | 1.0.0 (Valkey 8.0.6 fork) | Same | Same | Same |
| SQLite | 3.45.1 | WAL, `synchronous=FULL`, 1 row per transaction | `INSERT` | [h2h_sqlite.py](h2h_sqlite.py), in process, 1 process and 1 connection per writer |

Dragonfly is not timed. It has no append-only file, so it cannot acknowledge a write after `fsync` ([docs](https://www.dragonflydb.io/docs/managing-dragonfly/aof)).

`h2h_paced.py` runs 1 process and 1 raw socket per writer. It speaks RESP to the Redis family and HTTP to zerodb. So every server gets the same client. zerodb's requests cost it slightly more, JSON and an HTTP head against 1 RESP line. Think 0 is the closed loop.

## Premium SSD v2

| Writers | System | Acks/s | p50 µs | p99 µs | p99 range µs | p99.9 µs | max µs |
|---|---|---|---|---|---|---|---|
| 1 | zerodb | 1,167 | 817 | 1,514 | 1,509 to 1,636 | 2,097 | 2,590 |
| 1 | redis | 466 | 2,136 | 3,231 | 3,079 to 3,253 | 4,246 | 11,650 |
| 1 | valkey | 472 | 2,131 | 3,214 | 3,057 to 3,278 | 4,215 | 8,883 |
| 1 | dicedb | 473 | 2,128 | 3,204 | 3,133 to 3,250 | 4,222 | 7,982 |
| 1 | sqlite | 1,143 | 735 | 2,365 | 2,253 to 2,479 | 3,029 | 4,763 |
| 4 | zerodb | 2,619 | 1,473 | 2,366 | 2,246 to 2,404 | 2,875 | 4,026 |
| 4 | redis | 1,022 | 3,972 | 5,568 | 5,436 to 5,646 | 6,945 | 13,518 |
| 4 | valkey | 936 | 4,255 | 5,622 | 5,467 to 5,738 | 7,943 | 15,345 |
| 4 | dicedb | 932 | 4,273 | 5,691 | 5,418 to 5,810 | 8,424 | 16,121 |
| 4 | sqlite | 1,342 | 704 | 1,960 | 1,807 to 1,993 | 2,716 | 9,944,485 |
| 16 | zerodb | 9,328 | 1,618 | 2,734 | 2,394 to 2,937 | 3,323 | 4,173 |
| 16 | redis | 5,151 | 3,292 | 5,195 | 5,016 to 5,582 | 7,465 | 16,279 |
| 16 | valkey | 5,096 | 3,185 | 5,161 | 5,038 to 5,634 | 6,327 | 25,764 |
| 16 | dicedb | 5,086 | 3,513 | 5,063 | 4,988 to 5,785 | 6,072 | 11,252 |
| 16 | sqlite | 1,421 | 704 | 1,696 | 1,639 to 1,839 | 3,098 | 46,787,988 |

Agent-shaped:

| Load | System | Acks/s | p50 µs | p99 µs | p99 range µs | p99.9 µs | max µs |
|---|---|---|---|---|---|---|---|
| 64 writers, 5 ms | zerodb | 9,664 | 1,286 | 2,246 | 2,194 to 2,380 | 2,764 | 6,213 |
| 64 writers, 5 ms | redis | 7,779 | 2,916 | 4,693 | 4,526 to 5,059 | 6,545 | 13,334 |
| 64 writers, 5 ms | valkey | 7,796 | 2,901 | 4,597 | 4,442 to 5,139 | 6,992 | 11,483 |
| 64 writers, 5 ms | dicedb | 7,840 | 2,807 | 4,592 | 4,424 to 4,968 | 6,998 | 11,928 |
| 64 writers, 5 ms | sqlite | 1,105 | 3,860 | 830,031 | 829,880 to 830,422 | 1,730,934 | 3,731,498 |
| 256 writers, 50 ms | zerodb | 4,270 | 1,284 | 2,259 | 2,184 to 2,392 | 2,827 | 5,302 |
| 256 writers, 50 ms | redis | 4,151 | 3,070 | 4,955 | 4,700 to 5,433 | 12,560 | 33,249 |
| 256 writers, 50 ms | valkey | 4,154 | 3,072 | 4,860 | 4,694 to 9,744 | 8,071 | 28,153 |
| 256 writers, 50 ms | dicedb | 4,149 | 3,065 | 4,985 | 4,590 to 5,365 | 9,384 | 27,769 |
| 256 writers, 50 ms | sqlite | 1,038 | 9,405 | 2,130,899 | 1,830,926 to 2,131,252 | 3,732,598 | 6,534,968 |

zerodb beats Redis, Valkey and DiceDB in every cell on this disk. It acks 1.8x to 2.8x as many writes in closed loop. In every run its worst p99 rep beats their best rep. Under agent load its p50 is 1.3 ms against 2.8 to 3.1 ms. In closed loop SQLite has a lower p50 at every writer count. Its p99 and p99.9 are lower at 4 and 16 writers.

## Local NVMe

| Writers | System | Acks/s | p50 µs | p99 µs | p99 range µs | p99.9 µs | max µs |
|---|---|---|---|---|---|---|---|
| 1 | zerodb | 9,799 | 94 | 128 | 113 to 320 | 145 | 300 |
| 1 | redis | 8,110 | 116 | 184 | 171 to 708 | 216 | 354 |
| 1 | valkey | 8,434 | 108 | 172 | 166 to 602 | 212 | 330 |
| 1 | dicedb | 8,436 | 110 | 179 | 166 to 186 | 214 | 344 |
| 1 | sqlite | 19,410 | 32 | 157 | 152 to 170 | 411 | 736 |
| 4 | zerodb | 24,516 | 146 | 214 | 213 to 218 | 251 | 438 |
| 4 | redis | 17,504 | 221 | 304 | 296 to 317 | 348 | 532 |
| 4 | valkey | 18,444 | 202 | 298 | 286 to 324 | 344 | 474 |
| 4 | dicedb | 18,149 | 208 | 310 | 298 to 327 | 361 | 467 |
| 4 | sqlite | 22,162 | 32 | 143 | 138 to 160 | 622 | 729,400 |
| 16 | zerodb | 48,611 | 305 | 502 | 493 to 519 | 674 | 1,210 |
| 16 | redis | 44,572 | 299 | 549 | 543 to 564 | 604 | 932 |
| 16 | valkey | 42,928 | 311 | 573 | 546 to 580 | 624 | 810 |
| 16 | dicedb | 44,693 | 300 | 547 | 537 to 572 | 609 | 947 |
| 16 | sqlite | 24,936 | 32 | 124 | 123 to 128 | 648 | 2,831,276 |

Agent-shaped:

| Load | System | Acks/s | p50 µs | p99 µs | p99 range µs | p99.9 µs | max µs |
|---|---|---|---|---|---|---|---|
| 64 writers, 5 ms | zerodb | 11,687 | 148 | 416 | 273 to 521 | 786 | 4,871 |
| 64 writers, 5 ms | redis | 11,648 | 183 | 382 | 336 to 567 | 964 | 5,317 |
| 64 writers, 5 ms | valkey | 11,641 | 184 | 506 | 341 to 564 | 1,053 | 6,766 |
| 64 writers, 5 ms | dicedb | 11,644 | 182 | 504 | 334 to 547 | 1,029 | 5,292 |
| 64 writers, 5 ms | sqlite | 8,531 | 57 | 33,381 | 33,367 to 33,414 | 103,539 | 428,896 |
| 256 writers, 50 ms | zerodb | 4,351 | 138 | 558 | 225 to 636 | 736 | 8,698 |
| 256 writers, 50 ms | redis | 4,351 | 145 | 612 | 292 to 752 | 2,185 | 7,625 |
| 256 writers, 50 ms | valkey | 4,352 | 142 | 496 | 293 to 1,318 | 2,758 | 8,039 |
| 256 writers, 50 ms | dicedb | 4,352 | 140 | 348 | 289 to 1,078 | 2,103 | 7,392 |
| 256 writers, 50 ms | sqlite | 4,242 | 68 | 18,489 | 18,354 to 18,732 | 103,782 | 329,060 |

In closed loop zerodb beats Redis, Valkey and DiceDB on every median at 1 and 4 writers. At 16 writers it beats them on acks/s and p99 only. It acks 48,611/s against 42,928 to 44,693. Its p99 is 502 µs against 547 to 573. Redis and DiceDB have a lower p50: 299 and 300 against 305 µs. Its p99.9 and max are the worst of the 4 servers: 674 and 1,210 µs. The others get 604 to 624 and 810 to 947.

Under agent load zerodb has the best p50 and p99.9 of the 4 servers. Its p50 is 148 µs at 64 writers against 182 to 184. At 256 writers it's 138 against 140 to 145. Its p99 median trails Redis at 64 writers: 416 against 382 µs. At 256 writers DiceDB and Valkey have lower medians: 348 and 496 against 558. The ranges overlap: zerodb 225 to 636, DiceDB 289 to 1,078. zerodb's max at 256 writers is the worst of the 4: 8,698 µs against 7,392 to 8,039. The think time sets acks/s here, so all 4 tie. At 256 writers zerodb's median is lowest, by 0.5 to 1.6 acks/s.

## Why

- **1 device round trip, not 2.** An append that grows a file makes `fdatasync` also commit the size change to the ext4 journal. Redis-family AOF grows on every write, so each ack costs about 2 round trips. On Premium SSD v2 their 1-writer p50 is 2.1 ms. zerodb writes into zero-filled, preallocated segments (D78). SQLite mostly rewrites its WAL in place. Both pay 1 round trip: 0.8 and 0.7 ms.
- **Group commit.** zerodb syncs once for all queued writers and never waits on a timer (D80). On Premium SSD v2, 16 writers ack 9,328/s. That's 8x its 1-writer rate. The Redis family groups too and grows 11x, from a lower base. But every group still pays 2 round trips, so it tops out at 5,086 to 5,151/s.
- **SQLite serializes writers.** Its writers take 1 lock, and a loser sleeps in the busy handler before it retries. So p50 stays low while throughput barely grows. From 1 to 16 writers it grows 1.3x on NVMe and 1.2x on Premium SSD v2. The max at 16 writers reaches 2.8 s on NVMe and 46.8 s on Premium SSD v2. The percentiles hide it: p99.9 at 16 writers on NVMe is still 648 µs. Under agent load the stalls reach p99. That's 33 ms at 64 writers on NVMe and 2.1 s at 256 writers on Premium SSD v2.
- **SQLite has no socket.** It runs in the writer's process. On NVMe with 1 writer that wins: 19,410 acks/s and a 32 µs p50. zerodb gets 9,799 and 94 µs, including an HTTP round trip over the Unix socket. SQLite also keeps a lower p50 and p99 than zerodb at 4 and 16 writers on NVMe. Its p99.9 is lower at 16 too. Under agent load on NVMe its p50 is lower as well: 57 and 68 µs. It acks fewer writes: 24,936/s against 48,611 at 16 writers.
- **Retry keys no longer stall the writer.** Each doubling of the per-tenant retry-key map used to rehash every key under the state lock. That stalled the writer and pushed up zerodb's p99.9 under agent load. #43 shards the map 64 ways (D81). zerodb's p99.9 under agent load on NVMe is now 786 and 736 µs. The Redis family gets 964 to 2,758 µs.
- **Thread handoffs.** Each append hops from a connection thread to the writer thread and back. Waking an idle vCPU costs a hypervisor exit. Pinning every server thread to 1 vCPU gives 1 writer a steady 71 µs p50 (D82). It also costs acks at 16 writers, so zerodb doesn't pin. D82 saw 85 to 222 µs unpinned. Here zerodb's 1-writer p50 ranged 86 to 214 µs over 7 reps. The slowest was rep 1, slow on acks for Redis and Valkey too.

## Run-to-run noise

Runs on this VM land in a fast or a slow mode at random. Steal time stays 0. The 4 servers often move together. On NVMe at 256 writers with 50 ms think, reps 1 and 2 were fast for all 4. Their p99 was 225 to 301 µs. Rep 3 was slow for all 4: 636 to 765 µs. SQLite, in process, shows no slow reps here. On NVMe its acks and p99 stay within 16% across reps.

Rep 1 at 1 writer on NVMe was slow on acks for zerodb, Redis and Valkey: 4,404, 1,928 and 5,512/s. Their medians are 9,799, 8,110 and 8,434. Rep 1 was the first load of the whole run, so it may be warm-up. In that run each rep used a fixed order, with zerodb first, so any warm-up hit zerodb's slot. `h2h.sh` now shuffles the order every rep.

A per-append trace of zerodb found every stage slower together in slow runs. That includes `fdatasync`, which went from 26 to 65 µs. No single wait in zerodb explains it. [#48](https://github.com/Clawdlinux/zerodb/issues/48) tracks the cause.

With 3 reps, the mode a run landed in decided p99. In 8 adjacent pairs at 256 writers with 50 ms think, zerodb had the lower p99 in 6 ([#47](https://github.com/Clawdlinux/zerodb/issues/47)). So the harness now runs 7 reps, interleaved. Per rep in the TSVs, zerodb's 256-writer p99 beat Redis in 7 of 7 reps. It beat Valkey in 4 and DiceDB in 5. At 64 writers it beat each in 5 of 7.

Agent-shaped p99 medians still move a lot between runs. Read the p99 range next to the median.

The summary now also gives each system's throughput as a ratio to Redis within the same rep, `H2H_CONTROL`. A slow mode hits every system in the rep, so the ratio stays put when the absolute numbers move. Compare systems by that ratio.

## Client

Every server now gets the same client, `h2h_paced.py`. The 2026-10-23 run didn't. The Redis family got memtier_benchmark: 1 thread, 16 connections. zerodb got `zerodb load`: 16 Rust threads. With those clients zerodb had the worst p99 at 16 writers.

With 1 client zerodb's 16-writer NVMe p99 beat Redis, Valkey and DiceDB in 7 of 7 reps. zerodb got slower too, but the Redis family got slower by more:

| 16 writers, NVMe | Acks/s before | Acks/s now | p99 µs before | p99 µs now |
|---|---|---|---|---|
| zerodb | 54,784 | 48,611 | 462 | 502 |
| redis | 52,922 | 44,572 | 407 | 549 |
| valkey | 55,287 | 42,928 | 383 | 573 |
| dicedb | 57,439 | 44,693 | 367 | 547 |

The Python client likely costs more CPU than memtier on the same 8 vCPUs. We haven't measured why the Redis family loses more. The 2 runs are on different days and zerodb revisions (`d40f048`, `02b43e5`), so treat these deltas as rough.

The client shares the server's vCPUs. On an L16s_v3 we pinned the server to vCPUs 0-7 and moved the client between 0-7 and 8-15 (`H2H_SERVER_CPUS`, `H2H_CLIENT_CPUS`). Its own vCPUs raised zerodb by 19% and 28% at 64 and 128 writers, and Redis by 24% and 27%. The zerodb-to-Redis ratio stayed at 0.88 at 128 writers. So the shared client lowers every system's absolute numbers, but it doesn't explain zerodb's gap to Redis. That gap is in the server ([#52](https://github.com/Clawdlinux/zerodb/issues/52)). Details are in [RESULTS.md](RESULTS.md).

## Not covered

PostgreSQL, Ultra Disk, replicated setups, reads, payloads other than 256 bytes, and writes without `fsync`. The harness also runs systems in a fixed order within each rep.
