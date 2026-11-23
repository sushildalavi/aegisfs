#!/usr/bin/env python3
"""Validate benchmark output before it is used as performance evidence."""
import csv
import pathlib
import sys


def main(path):
    p = pathlib.Path(path)
    with p.open(newline="") as f:
        rows = list(csv.DictReader(f))
    required = {"mode", "op", "size_kb", "rep", "latency_ms", "bytes_per_s"}
    if not rows:
        raise SystemExit("benchmark output has no data rows")
    missing = required - set(rows[0])
    if missing:
        raise SystemExit(f"benchmark output missing columns: {sorted(missing)}")
    seen = set()
    for i, row in enumerate(rows, 2):
        try:
            if float(row["latency_ms"]) <= 0 or float(row["bytes_per_s"]) <= 0:
                raise ValueError
            size = int(row["size_kb"])
            repetition = int(row["rep"])
            if size <= 0 or repetition <= 0:
                raise ValueError
            key = (row["mode"], row["op"], size, repetition)
            if key in seen:
                raise ValueError
            seen.add(key)
        except ValueError:
            raise SystemExit(f"invalid benchmark row {i}: {row}")
    modes = {row["mode"] for row in rows}
    operations = {row["op"] for row in rows}
    if not {"sequential", "parallel"}.issubset(modes):
        raise SystemExit(f"benchmark output must include sequential and parallel modes: {sorted(modes)}")
    if not {"create", "get"}.issubset(operations):
        raise SystemExit(f"benchmark output must include create and get operations: {sorted(operations)}")
    print(f"validated {len(rows)} benchmark rows")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: validate-results.py RESULTS/raw.csv")
    main(sys.argv[1])
