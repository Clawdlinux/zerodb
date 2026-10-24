"""Power-loss test on a real filesystem, through LazyFS.

LazyFS keeps unsynced writes in its own memory, so killing it loses them as a power
cut does. Each round mounts LazyFS, checks every write known to be durable, appends
from several writers, then cuts power: at a chosen syscall, or at a random moment.
After each cut, `zerodb verify` runs on the raw directory LazyFS left behind.

    python3 tests/crash/power_loss.py --lazyfs <lazyfs checkout> [--rounds N] [--seed S]

The seed fixes the cut choices and the written data, not thread timing. On failure the
data directory stays in --work.
"""

import argparse
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "clients", "python"))
from zerodb_client import Client, Error  # noqa: E402

TOKEN = "token-a-0123456789abcdefghijklmnopqrstuv"
WRITERS = 8
SIZES = [200] * 16 + [32 << 10] * 3 + [128 << 10]
# (timing, op, from, to). LazyFS kills itself when an op's path fully matches the regex.
CUTS = [
    ("before", "fsync", ".*/seg-.*", None),
    ("after", "fsync", ".*/seg-.*", None),
    ("before", "write", ".*/seg-.*", None),
    ("before", "rename", ".*/spare.zdb", ".*/seg-.*"),
    ("after", "rename", ".*/MANIFEST.tmp", ".*/MANIFEST"),
    ("after", "rename", ".*/SUPERBLOCK.tmp", ".*/SUPERBLOCK"),
    ("after", "fsync", ".*/ckpt-.*", None),
]


def wait(check, what, timeout=60.0):
    deadline = time.monotonic() + timeout
    while not check():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.02)


class Rig:
    """LazyFS over `root`, mounted at `mnt`, and zerodb serving from `mnt/d`."""

    def __init__(self, args):
        self.args, w = args, args.work
        shutil.rmtree(w, ignore_errors=True)
        self.mnt, self.root, self.fifo, self.sock = f"{w}/mnt", f"{w}/root", f"{w}/f.fifo", f"{w}/s.sock"
        for d in (self.mnt, f"{self.root}/d", f"{w}/scratch"):
            os.makedirs(d, 0o700)
        with open(f"{w}/lazyfs.toml", "w") as f:
            f.write(f'[faults]\nfifo_path="{self.fifo}"\n[cache]\napply_eviction=false\n'
                    '[cache.simple]\ncustom_size="4GB"\nblocks_per_page=1\n'
                    f'[filesystem]\nlog_all_operations=false\nlogfile="{w}/lazyfs.log"\n')
        with os.fdopen(os.open(f"{w}/c.json", os.O_WRONLY | os.O_CREAT, 0o600), "w") as f:
            json.dump({"tenants": [{"name": "a", "token": TOKEN}]}, f)
        self.fs = self.db = None

    def start(self):
        w = self.args.work
        self.fs = subprocess.Popen(
            [f"{self.args.lazyfs}/lazyfs/build/lazyfs", self.mnt, "--config-path", f"{w}/lazyfs.toml",
             "-o", "allow_other", "-o", "modules=subdir", "-o", f"subdir={self.root}", "-f"],
            stdout=subprocess.DEVNULL, stderr=open(f"{w}/lazyfs.out", "ab"))
        wait(lambda: self.fs.poll() is not None or os.path.ismount(self.mnt), "LazyFS mount")
        assert os.path.ismount(self.mnt), f"LazyFS did not mount: {open(f'{w}/lazyfs.out').read()[-800:]}"
        log = open(f"{w}/serve.log", "ab")
        self.db = subprocess.Popen(
            [self.args.bin, "serve", "--config", f"{w}/c.json", "--data-dir", f"{self.mnt}/d",
             "--listen", "none", "--unix-socket", self.sock],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        wait(lambda: self.db.poll() is not None or os.path.exists(self.sock), "zerodb socket")
        assert self.db.poll() is None, f"zerodb exited: {open(f'{w}/serve.log').read()[-500:]}"

    def client(self):
        return Client(f"unix://{self.sock}", TOKEN, retry_deadline=5.0)

    def cut(self, rng):
        """Kills LazyFS, then zerodb. Returns the cut that happened."""
        cut = ("random",)
        if rng.random() < 0.5:
            timing, op, src, dst = rng.choice(CUTS)
            with open(self.fifo, "w") as f:
                f.write(f"lazyfs::crash::timing={timing}::op={op}::from_rgx={src}"
                        + (f"::to_rgx={dst}" if dst else "") + "\n")
            try:
                self.fs.wait(timeout=rng.uniform(0.5, 3.0))
                cut = (timing, op, src)
            except subprocess.TimeoutExpired:
                cut = ("random", f"{timing} {op} {src} did not fire")
        self.stop()
        return cut

    def stop(self):
        for proc in (self.fs, self.db):
            if proc and proc.poll() is None:
                proc.send_signal(signal.SIGKILL)
                proc.wait()
        subprocess.run(["fusermount3", "-uz", self.mnt], check=False, capture_output=True)
        if os.path.exists(self.sock):
            os.remove(self.sock)


def write(client, rng, session, stop, attempted, acked, errors):
    while not stop.is_set():
        batch = f"{session}.{rng.getrandbits(48):x}"
        sizes = [rng.choice(SIZES) for _ in range(rng.randint(1, 4))]
        contents = [f"{batch}/{j}|".ljust(size, "x") for j, size in enumerate(sizes)]
        attempted[batch] = contents
        try:
            receipt = client.append("a", session, [{"role": "user", "content": c} for c in contents])
        except Error as error:
            errors.append(str(error))
            return
        acked.extend(zip(receipt["seqs"], contents))


def check(client, attempted, durable):
    """Durable turns are back at their sequences, sequences are dense, batches are whole."""
    for session, expected in durable.items():
        turns, since = [], 1
        while page := client.window("a", session, since_seq=since, limit=1000, max_bytes=8 << 20)["turns"]:
            turns += [(t["seq"], t["content"]) for t in page]
            since = page[-1]["seq"] + 1
        assert [s for s, _ in turns] == list(range(1, len(turns) + 1)), f"{session}: sequence gap"
        have = dict(turns)
        lost = [seq for seq, content in expected if have.get(seq) != content]
        assert not lost, f"{session}: {len(lost)} durable turns lost or changed, first seqs {lost[:5]}"
        batches = {}
        for seq, content in turns:
            batch, j = content.split("|", 1)[0].split("/")
            assert content in attempted.get(batch, ()), f"{session}: turn {seq} was never written"
            batches.setdefault(batch, []).append(seq)
        for batch, seqs in batches.items():
            whole = list(range(seqs[0], seqs[0] + len(attempted[batch])))
            assert seqs == whole, f"{session}: batch {batch} is partial at {seqs}"
        # Reopen syncs what recovery keeps, so the next cut must keep it too.
        durable[session] = turns


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lazyfs", required=True)
    p.add_argument("--bin", default="target/release/zerodb")
    p.add_argument("--work", default="target/power-loss")
    p.add_argument("--rounds", type=int, default=int(os.environ.get("ZERODB_CRASH_ROUNDS") or 20))
    p.add_argument("--seed", type=int, default=int(os.environ.get("ZERODB_CRASH_SEED") or time.time_ns() % 2**32))
    args = p.parse_args()
    args.lazyfs, args.bin, args.work = map(os.path.abspath, (args.lazyfs, args.bin, args.work))
    rng, rig = random.Random(args.seed), Rig(args)
    attempted, durable, cuts = {}, {f"w{i}": [] for i in range(WRITERS)}, []
    print(f"seed={args.seed} rounds={args.rounds}", flush=True)
    try:
        for round_ in range(args.rounds + 1):
            rig.start()
            check(rig.client(), attempted, durable)
            if round_ == args.rounds:
                break
            stop, acked, errors = threading.Event(), {s: [] for s in durable}, []
            threads = [threading.Thread(target=write, args=(rig.client(), random.Random(rng.getrandbits(64)),
                                                            s, stop, attempted, acked[s], errors)) for s in durable]
            for t in threads:
                t.start()
            time.sleep(rng.uniform(0.2, 1.5))
            cuts.append(rig.cut(rng))
            stop.set()
            for t in threads:
                t.join()
            # A round without acks proves nothing, so it fails.
            assert any(acked.values()), f"no write acknowledged: {errors[:1]} " \
                f"{open(f'{args.work}/serve.log').read()[-600:]}"
            for s, turns in acked.items():
                durable[s] = sorted(set(durable[s]) | set(turns))
            done = subprocess.run([args.bin, "verify", "--data-dir", f"{rig.root}/d", "--scratch",
                                   f"{args.work}/scratch"], capture_output=True, text=True)
            assert done.returncode == 0, f"verify failed: {done.stderr.strip()} {done.stdout.strip()}"
            print(f"round {round_}: cut {cuts[-1]}, acked {sum(map(len, acked.values()))}, "
                  f"durable {sum(map(len, durable.values()))}", flush=True)
    except AssertionError as error:
        sys.exit(f"FAILED seed={args.seed} round={len(cuts)} last cut={cuts[-1:]}: {error}")
    finally:
        rig.stop()
    fired = sum(c[0] != "random" for c in cuts)
    print(f"passed seed={args.seed}: {len(cuts)} power cuts, {fired} at a chosen syscall")


if __name__ == "__main__":
    main()
