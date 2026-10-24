#!/usr/bin/env bash
# Head-to-head: durable 256-byte appends, acknowledged only after fsync, 1 request in flight.
# Linux only. Usage: bench/h2h.sh <directory on the disk under test> [label]
# Every server gets the same client, h2h_paced.py: 1 process and socket per writer. With
# H2H_THINK=<ms> each writer sleeps a uniform 0 to 2x that before each write, like an agent.
# Builds pinned Redis, Valkey and DiceDB into $H2H_TOOLS on first run.
# Systems run in a new random order each rep, so none always goes first (#48). The summary
# gives each system's per-rep throughput ratio to $H2H_CONTROL: a slow VM mode hits every
# system in the rep, so the ratio cancels it. H2H_SERVER_CPUS and H2H_CLIENT_CPUS, such as
# 0-7 and 8-15, pin servers and the client to separate vCPUs. SQLite runs in the client.
set -euo pipefail
DISK=$(realpath "${1:?usage: bench/h2h.sh <dir on the disk under test> [label]}")
LABEL=${2:-$(basename "$DISK")}
cd "$(dirname "$0")/.."
REPS=${H2H_REPS:-7}
WRITERS=${H2H_WRITERS:-"1 4 16"}
PER=${H2H_PER_WRITER:-5000}
THINK=${H2H_THINK:-0}
SIZE=256
TOOLS=${H2H_TOOLS:-$HOME/h2h-tools}
CONTROL=${H2H_CONTROL:-redis}
SPIN=${H2H_SERVER_CPUS:+taskset -c $H2H_SERVER_CPUS}
CPIN=${H2H_CLIENT_CPUS:+taskset -c $H2H_CLIENT_CPUS}
OUT=bench/h2h-$LABEL.tsv
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
# With a trap, bash lets the current run stop its own server before exiting.
trap 'exit 143' TERM INT

fetch() {
    [ -d "$TOOLS/$1" ] || git clone -q --depth 1 --branch "$3" "https://github.com/$2" "$TOOLS/$1"
}
mkdir -p "$TOOLS"
fetch redis redis/redis 8.10.2
fetch valkey valkey-io/valkey 9.1.2
fetch dicedb dicedb/dicedb 1.0.0
for name in redis valkey dicedb; do
    [ -x "$TOOLS/$name/src/$name-server" ] || make -C "$TOOLS/$name" -j"$(nproc)" >/dev/null 2>&1
done
cargo build -q --release -p zerodb
ZDB=target/release/zerodb

# Waits for a server's socket. Fails with its log if the server exits first.
wait_socket() {
    until [ -S "$2" ]; do
        if ! kill -0 "$1" 2>/dev/null; then
            echo "server exited before creating $2:" >&2 && cat "$3" >&2
            return 1
        fi
        sleep 0.1
    done
}

# Prints `turns_per_sec p50_us p99_us p999_us max_us` from `key=value` lines on stdin.
pick() {
    awk -F= '{v[$1]=$2} END {print v["turns_per_sec"], v["p50_us"], v["p99_us"], v["p999_us"], v["max_us"]}'
}

# Busy and total jiffies across all CPUs.
cpu() { awk '/^cpu /{t=0; for(i=2;i<=NF;i++) t+=$i; print t-$5-$6, t}' /proc/stat; }

zerodb() {
    local data=$DISK/zerodb
    rm -rf "$data" "$TMP/c.json"
    (umask 077 && "$ZDB" init --config "$TMP/c.json" --tenant h2h >"$TMP/token")
    $SPIN "$ZDB" serve --config "$TMP/c.json" --data-dir "$data" --listen none >"$TMP/serve.log" 2>&1 &
    local pid=$! status=0
    wait_socket $pid "$data/zerodb.sock" "$TMP/serve.log"
    ZERODB_TOKEN=$(cat "$TMP/token") $CPIN python3 bench/h2h_paced.py zerodb "$data/zerodb.sock" \
        "$1" "$PER" $SIZE "$THINK" >"$TMP/load" || status=$?
    kill $pid && wait $pid || true
    # A failed run must not become a row.
    [ $status = 0 ] && pick <"$TMP/load"
}

# Redis-protocol servers: AOF with appendfsync always, so each reply waits for its fsync.
resp() {
    local data=$DISK/$2 sock=$TMP/$2.sock
    rm -rf "$data" && mkdir -p "$data"
    $SPIN "$TOOLS/$2/src/$2-server" --port 0 --unixsocket "$sock" --dir "$data" --save "" \
        --appendonly yes --appendfsync always --logfile "$data/log" &
    local pid=$! status=0
    wait_socket $pid "$sock" "$data/log"
    # The driver fails on any reply but an integer, such as MISCONF after an AOF failure.
    $CPIN python3 bench/h2h_paced.py resp "$sock" "$1" "$PER" $SIZE "$THINK" >"$TMP/load" || status=$?
    kill $pid && wait $pid || true
    [ $status = 0 ] && pick <"$TMP/load"
}

sqlite() {
    rm -f "$DISK"/h2h.db*
    $CPIN python3 bench/h2h_sqlite.py "$DISK/h2h.db" "$1" "$PER" $SIZE "$THINK" | pick
}

{
    echo "# $(uname -sr) $(findmnt -no SOURCE,FSTYPE --target "$DISK"), think ${THINK} ms"
    echo "# zerodb ${H2H_ZERODB_REV:-$(git rev-parse --short HEAD)}, client bench/h2h_paced.py, $REPS reps"
    echo "# $(nproc) vCPUs, server cpus ${H2H_SERVER_CPUS:-any}, client cpus ${H2H_CLIENT_CPUS:-any}"
    for name in redis valkey dicedb; do echo "# $("$TOOLS/$name/src/$name-server" --version)"; done
    echo "# sqlite $(python3 -c 'import sqlite3; print(sqlite3.sqlite_version)')"
    printf 'system\twriters\trep\tturns_per_sec\tp50_us\tp99_us\tp999_us\tmax_us\tbusy_pct\n'
} >"$OUT"
for rep in $(seq "$REPS"); do
    for writers in $WRITERS; do
        for system in $(printf '%s\n' ${H2H_SYSTEMS:-zerodb redis valkey dicedb sqlite} | shuf); do
            read -r b0 t0 < <(cpu)
            case $system in
                zerodb) row=$(zerodb "$writers") ;;
                sqlite) row=$(sqlite "$writers") ;;
                *) row=$(resp "$writers" "$system") ;;
            esac
            # Host CPU busy over the run. Near 100 means client and server compete for CPU.
            read -r b1 t1 < <(cpu)
            row="$row $(awk -v b=$((b1 - b0)) -v t=$((t1 - t0)) 'BEGIN {printf "%.0f", 100 * b / t}')"
            printf '%s\t%s\t%s\t%s\n' "$system" "$writers" "$rep" "${row// /$'\t'}" | tee -a "$OUT"
        done
    done
done

# Medians across reps, and each system's per-rep throughput ratio to the control.
python3 - "$OUT" "$CONTROL" <<'PY'
import statistics, sys, collections
rows = [l.split("\t") for l in open(sys.argv[1]) if l[0] != "#" and not l.startswith("system")]
runs, reps = collections.defaultdict(list), collections.defaultdict(dict)
for system, writers, rep, *values in rows:
    runs[(int(writers), system)].append([float(v) for v in values])
    reps[(int(writers), rep)][system] = float(values[0])
print(f"| Writers | System | Turns/s | p50 µs | p99 µs | p99.9 µs | max µs | busy % | vs {sys.argv[2]} |")
print("|---|---|---|---|---|---|---|---|---|")
for (writers, system), values in sorted(runs.items()):
    tps, p50, p99, p999, top, busy = (statistics.median(column) for column in zip(*values))
    r = [run[system] / run[sys.argv[2]] for (w, _), run in reps.items() if w == writers and {system, sys.argv[2]} <= run.keys()]
    ratio = f"{statistics.median(r):.2f} ({min(r):.2f}-{max(r):.2f})" if r else "-"
    print(f"| {writers} | {system} | {tps:,.0f} | {p50:,.0f} | {p99:,.0f} | {p999:,.0f} | {top:,.0f} | {busy:.0f} | {ratio} |")
PY
