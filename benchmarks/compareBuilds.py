#!/usr/bin/env python3
"""A/B benchmark for end-to-end replicated create latency of two aegisfs binaries.

Each arm runs on its own freshly started 5-node, RF=3, process-local cluster, one arm at a time
(never both at once: an idle cluster still burns CPU on gossip and background maintenance, which
would pollute the other arm's timings). Arms are interleaved over several rounds and the order
alternates each round, so slow drift in machine state is spread across both arms.

Every sample is a real `aegisfs create` client process on a 100 MiB (by default) random file,
acknowledged only after all three replicas have committed. The timer wraps only that process.
Raw samples go to a CSV; nothing is edited by hand.

Two views are reported per arm:
  first-N   the first few creates on a fresh cluster. This matches how the original benchmark
            (benchmarks/runMatrix.sh) measured: new cluster, a handful of repetitions.
  sustained every create in the round, as the store fills up. Background work that scales with
            stored data (scrubbing, reconcile passes) shows up here.

usage: compareBuilds.py --a BIN_A --b BIN_B [--size-mib 100] [--rounds 3] [--per-round 10]
                        [--first-n 5] [--warmup 1] [--out raw.csv]
"""
import argparse, csv, os, shutil, signal, statistics, subprocess, sys, tempfile, time

NODES = 5


def start_cluster(binp, base, root):
    shutil.rmtree(root, ignore_errors=True)
    procs = []
    for i in range(NODES):
        d = f"{root}/n{i}"
        os.makedirs(d)
        cmd = [binp, "serve", "--data", d, "--host", "127.0.0.1", "--port", str(base + i),
               "--id", f"n{i}"]
        if i:
            cmd += ["--seeds", f"127.0.0.1:{base}"]
        procs.append(subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    time.sleep(5)
    return procs


def stop_cluster(procs, root):
    for p in procs:
        p.send_signal(signal.SIGKILL)
    for p in procs:
        p.wait()
    shutil.rmtree(root, ignore_errors=True)


def pct(xs, p):
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def summarize(label, xs, size_mib):
    med = statistics.median(xs)
    sd = statistics.stdev(xs) if len(xs) > 1 else 0.0
    print(f"  {label:<10} n={len(xs):<3} median={med:7.1f}  mean={statistics.mean(xs):7.1f}  "
          f"sd={sd:6.1f}  p95={pct(xs, 95):7.1f}  p99={pct(xs, 99):7.1f}  "
          f"min={min(xs):7.1f}  max={max(xs):7.1f}  MB/s(median)={size_mib * 1.048576 / (med / 1000):.0f}")
    return med


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True, help="baseline binary")
    ap.add_argument("--b", required=True, help="candidate binary")
    ap.add_argument("--size-mib", type=int, default=100)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--per-round", type=int, default=10, help="creates per cluster, after warmup")
    ap.add_argument("--first-n", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--base-port", type=int, default=9500)
    ap.add_argument("--out", default="raw.csv")
    a = ap.parse_args()

    tmp = tempfile.mkdtemp(prefix="aegisbench.")
    src = f"{tmp}/in.bin"
    with open(src, "wb") as f:  # incompressible input, identical for both arms
        for _ in range(a.size_mib):
            f.write(os.urandom(1 << 20))

    bins = {"A": a.a, "B": a.b}
    rows = []  # (arm, round, index, ms)
    try:
        for rnd in range(a.rounds):
            order = ["A", "B"] if rnd % 2 == 0 else ["B", "A"]
            for arm in order:
                procs = start_cluster(bins[arm], a.base_port, f"{tmp}/cluster")
                try:
                    for i in range(a.warmup + a.per_round):
                        t0 = time.perf_counter()
                        r = subprocess.run([bins[arm], "--entry", f"127.0.0.1:{a.base_port}",
                                            "create", src, f"bench/{arm}_{rnd}_{i}"],
                                           capture_output=True)
                        dt = (time.perf_counter() - t0) * 1000
                        out = r.stdout.decode()
                        if r.returncode != 0 or "status=OK" not in out:
                            sys.exit(f"create failed (arm {arm} round {rnd} #{i}): {out[:200]}")
                        if i >= a.warmup:
                            rows.append((arm, rnd, i - a.warmup, dt))
                finally:
                    stop_cluster(procs, f"{tmp}/cluster")
                print(f"round {rnd + 1}/{a.rounds} arm {arm} done", file=sys.stderr)

        with open(a.out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["arm", "round", "index", "latency_ms"])
            for arm, rnd, i, ms in rows:
                w.writerow([arm, rnd, i, f"{ms:.2f}"])

        print(f"{a.size_mib} MiB create, RF=3, {NODES} nodes, {a.rounds} rounds x {a.per_round} "
              f"creates/arm/round (+{a.warmup} warm-up each), fresh cluster per round")
        meds = {}
        for view, pick in (("first-%d" % a.first_n, lambda i: i < a.first_n),
                           ("sustained", lambda i: True)):
            print(f"{view}:")
            for arm in "AB":
                xs = [ms for (ar, _, i, ms) in rows if ar == arm and pick(i)]
                meds[(view, arm)] = summarize(f"{arm}", xs, a.size_mib)
            ma, mb = meds[(view, "A")], meds[(view, "B")]
            print(f"  median change A->B: {(mb - ma) / ma * 100:+.1f}%")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
