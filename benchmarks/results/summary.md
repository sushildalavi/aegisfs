# AegisFS Benchmark Summary

Process-local 5-node cluster, RF=3. Latency is end-to-end client wall-clock (includes TCP + replication to all 3 replicas). Each point is the mean/percentiles over repetitions. Measured on the dev host; absolute numbers are machine-specific.

## Latency by operation, size, and replication mode

| op | size | mode | n | mean ms | p50 | p95 | p99 | MB/s |
|----|------|------|---|---------|-----|-----|-----|------|
| create | 1 KiB | parallel | 5 | 54.8 | 54.0 | 59.0 | 59.0 | 0.02 |
| create | 1 MiB | parallel | 5 | 62.4 | 60.0 | 68.0 | 68.0 | 16.09 |
| create | 10 MiB | parallel | 5 | 127.4 | 128.0 | 130.0 | 130.0 | 78.52 |
| create | 100 MiB | parallel | 5 | 540.8 | 518.0 | 710.0 | 710.0 | 189.33 |
| get | 1 KiB | parallel | 5 | 33.8 | 33.0 | 35.0 | 35.0 | 0.03 |
| get | 1 MiB | parallel | 5 | 35.8 | 36.0 | 37.0 | 37.0 | 27.95 |
| get | 10 MiB | parallel | 5 | 50.2 | 50.0 | 52.0 | 52.0 | 199.31 |
| get | 100 MiB | parallel | 5 | 188.6 | 189.0 | 194.0 | 194.0 | 530.51 |
| create | 1 KiB | sequential | 5 | 53.6 | 53.0 | 64.0 | 64.0 | 0.02 |
| create | 1 MiB | sequential | 5 | 60.4 | 61.0 | 63.0 | 63.0 | 16.57 |
| create | 10 MiB | sequential | 5 | 126.2 | 128.0 | 132.0 | 132.0 | 79.46 |
| create | 100 MiB | sequential | 5 | 647.2 | 659.0 | 761.0 | 761.0 | 156.96 |
| get | 1 KiB | sequential | 5 | 33.2 | 33.0 | 34.0 | 34.0 | 0.03 |
| get | 1 MiB | sequential | 5 | 35.4 | 35.0 | 37.0 | 37.0 | 28.26 |
| get | 10 MiB | sequential | 5 | 50.2 | 50.0 | 52.0 | 52.0 | 199.31 |
| get | 100 MiB | sequential | 5 | 196.8 | 204.0 | 212.0 | 212.0 | 511.3 |

## Create latency: sequential vs parallel fan-out

| size | seq mean ms | par mean ms | change |
|------|-------------|-------------|--------|
| 1 KiB | 53.6 | 54.8 | +2.2% |
| 1 MiB | 60.4 | 62.4 | +3.3% |
| 10 MiB | 126.2 | 127.4 | +1.0% |
| 100 MiB | 647.2 | 540.8 | -16.4% |
