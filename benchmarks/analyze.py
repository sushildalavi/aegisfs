#!/usr/bin/env python3
"""Summarize AegisFS benchmark raw CSV into per-(mode,op,size) stats.

Usage: analyze.py <raw.csv> <out_dir>
Reads: mode,op,size_kb,rep,latency_ms,bytes_per_s
Writes: <out_dir>/summary.csv and prints the readable table to stdout.
Uses only the Python standard library (no external deps), so it runs anywhere.
"""
import csv
import statistics
import sys
from collections import defaultdict


def pct(values, p):
    if not values:
        return 0
    s = sorted(values)
    k = min(len(s) - 1, int(p * len(s)))
    return s[k]


def main():
    raw, out_dir = sys.argv[1], sys.argv[2]
    rows = defaultdict(list)  # (mode,op,size_kb) -> [latency_ms]
    bps = defaultdict(list)
    with open(raw) as f:
        for r in csv.DictReader(f):
            key = (r["mode"], r["op"], int(r["size_kb"]))
            rows[key].append(float(r["latency_ms"]))
            bps[key].append(float(r["bytes_per_s"]))

    summary = []
    for key in sorted(rows):
        lat = rows[key]
        summary.append({
            "mode": key[0], "op": key[1], "size_kb": key[2], "n": len(lat),
            "mean_ms": round(statistics.mean(lat), 2),
            "p50_ms": round(pct(lat, 0.50), 2),
            "p95_ms": round(pct(lat, 0.95), 2),
            "p99_ms": round(pct(lat, 0.99), 2),
            "mb_per_s": round(statistics.mean(bps[key]) / (1024 * 1024), 2),
        })

    with open(f"{out_dir}/summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0].keys()))
        w.writeheader()
        w.writerows(summary)

    # Markdown: compare sequential vs parallel for create at each size.
    def size_label(kb):
        return f"{kb} KiB" if kb < 1024 else f"{kb // 1024} MiB"

    lines = ["# AegisFS Benchmark Summary\n",
             "Process-local 5-node cluster, RF=3. Latency is end-to-end client wall-clock "
             "(includes TCP + replication to all 3 replicas). Each point is the mean/percentiles "
             "over repetitions. Measured on the dev host; absolute numbers are machine-specific.\n",
             "## Latency by operation, size, and replication mode\n",
             "| op | size | mode | n | mean ms | p50 | p95 | p99 | MB/s |",
             "|----|------|------|---|---------|-----|-----|-----|------|"]
    for s in summary:
        lines.append(f"| {s['op']} | {size_label(s['size_kb'])} | {s['mode']} | {s['n']} | "
                     f"{s['mean_ms']} | {s['p50_ms']} | {s['p95_ms']} | {s['p99_ms']} | {s['mb_per_s']} |")

    # create: sequential vs parallel delta
    lines += ["\n## Create latency: sequential vs parallel fan-out\n",
              "| size | seq mean ms | par mean ms | change |",
              "|------|-------------|-------------|--------|"]
    by = {(s["mode"], s["size_kb"]): s for s in summary if s["op"] == "create"}
    sizes = sorted({s["size_kb"] for s in summary if s["op"] == "create"})
    for kb in sizes:
        seq = by.get(("sequential", kb))
        par = by.get(("parallel", kb))
        if not seq or not par:
            continue
        delta = (par["mean_ms"] - seq["mean_ms"]) / seq["mean_ms"] * 100 if seq["mean_ms"] else 0
        lines.append(f"| {size_label(kb)} | {seq['mean_ms']} | {par['mean_ms']} | {delta:+.1f}% |")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
