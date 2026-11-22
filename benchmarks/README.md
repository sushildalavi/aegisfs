# AegisFS Benchmarks

Reproducible, honest measurements. See `docs/BENCHMARKS.md` for methodology and results.

## Layout
- `runner/bench.sh` — create/get latency & throughput across sizes, sequential vs parallel fan-out.
- `runner/bench_concurrency.sh` — throughput (ops/sec) at 1/5/10/20 concurrent clients.
- `runner/analyze.py` — stdlib-only analyzer: raw CSV → `summary.{csv,md}` with p50/p95/p99.
- `fixtures/gen_data.py` — deterministic fixture file generator.
- `results/` — committed raw CSV + summaries from real runs.

## Run
```
cmake --build build
benchmarks/runner/bench.sh              # -> results/raw.csv, results/summary.{csv,md}
benchmarks/runner/bench_concurrency.sh  # -> results/concurrency.csv
```

## Notes
- All timings are end-to-end client wall-clock, including TCP + replication to all 3 replicas.
- Numbers are machine-specific (measured over loopback on the dev host). Reproduce locally rather
  than trusting the absolute values; the relative comparisons (seq vs parallel, scaling curve) are
  the point.
- Nothing here is fabricated or borrowed; negative/neutral results are reported as-is.
