#!/usr/bin/env python3
"""Measure the Cassandra metadata index on this machine and write raw samples as CSV.

One laptop, one Cassandra node (replication factor 1), loopback network: these numbers describe
that setup only. They are not a statement about a real cluster.

    benchIndex.py [--runs 5] [--samples 200] [--out benchmarks/integrations/cassandra]

Needs Cassandra reachable as for the live tests. Uses a fresh throwaway keyspace for every run (so one run's entries do not affect the next run's
reconcile) and drops each one afterwards.
Files written (raw samples, one row per measurement):
  indexLatency.csv   run,op,sample,ms          op = create | update
  lookupLatency.csv  run,op,sample,ms          op = get_by_name | list_prefix_100
  throughput.csv     run,clients,seconds,operations,ops_per_second   (concurrent get_by_name)
  reconcile.csv      run,entries,pass,seconds  pass = first | no_change | one_change
  environment.json   hardware and software versions
"""
import argparse
import csv
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import threading
import time
import uuid
from datetime import timedelta

import cassSupport as s
from aegisfsIntegrations.cassandra import connection, indexer, model as m, schema
from aegisfsIntegrations.cassandra.spool import Spool
from aegisfsIntegrations.cassandra.store import IndexStore


def sysctl(key):
    try:
        return subprocess.run(["sysctl", "-n", key], capture_output=True, text=True).stdout.strip()
    except OSError:
        return ""


def environment(settings, store):
    row = store.backend.session.execute("SELECT release_version FROM system.local").one()
    import cassandra
    docker_image = ""
    try:
        docker_image = subprocess.run(["docker", "inspect", "-f", "{{.Config.Image}}",
                                       os.environ.get("AEGIS_CASS_CONTAINER", "aegis-cass-cassandra-1")],
                                      capture_output=True, text=True).stdout.strip()
    except OSError:
        pass
    return {
        "machine": platform.machine(), "os": platform.platform(),
        "cpu": sysctl("machdep.cpu.brand_string") or platform.processor(),
        "cpu_count": os.cpu_count(), "memory_bytes": int(sysctl("hw.memsize") or 0),
        "python": sys.version.split()[0], "cassandra_driver": cassandra.__version__,
        "cassandra_server": row.release_version, "cassandra_image": docker_image,
        "cassandra_topology": "single node, replication factor 1, in a Docker container, loopback",
        "consistency": {"read": settings.read_consistency, "write": settings.write_consistency,
                        "serial": settings.serial_consistency},
        "bucket_count": store.bucket_count,
        "date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def obs_for(name, version, when):
    return m.Observation(name=name, version=version, observed_at=when, source="bench",
                         file_id="bb%014x" % (abs(hash(name)) % 16**14), stored_size=1000 + version,
                         replicas={"127.0.0.1:1": True, "127.0.0.1:2": True, "127.0.0.1:3": True},
                         replicas_source="listStore")


def timed(fn):
    start = time.perf_counter()
    fn()
    return (time.perf_counter() - start) * 1000.0


def measure(args, run, store, settings, spool, rows):
    prefix = "bench/run%d/" % run
    base = m.utcnow_ms()
    names = ["%sdir%02d/file%05d" % (prefix, i % 20, i) for i in range(args.samples)]
    for i, name in enumerate(names):
        rows["index"].append((run, "create", i, "%.3f" % timed(
            lambda: store.apply(obs_for(name, 1, base + timedelta(milliseconds=i))))))
    later = base + timedelta(seconds=60)
    for i, name in enumerate(names):
        rows["index"].append((run, "update", i, "%.3f" % timed(
            lambda: store.apply(obs_for(name, 2, later + timedelta(milliseconds=i))))))
    rng = random.Random(run)
    for i in range(args.samples):
        name = rng.choice(names)
        rows["lookup"].append((run, "get_by_name", i, "%.3f" % timed(lambda: store.get(name))))
    for i in range(max(20, args.samples // 5)):
        d = "%sdir%02d/" % (prefix, i % 20)
        rows["lookup"].append((run, "list_prefix_100", i, "%.3f" % timed(
            lambda: store.namespace_listing(d, limit=100))))
    for clients in [int(c) for c in args.clients.split(",")]:
        counts = [0] * clients
        stop = time.time() + args.seconds
        start = time.time()

        def work(k):
            local = random.Random(run * 100 + k)
            while time.time() < stop:
                store.get(local.choice(names))
                counts[k] += 1

        threads = [threading.Thread(target=work, args=(k,)) for k in range(clients)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        elapsed = time.time() - start
        total = sum(counts)
        rows["throughput"].append((run, clients, "%.2f" % elapsed, total, "%.1f" % (total / elapsed)))
    aegis = s.FakeAegis()
    for i in range(args.entries):
        aegis.add("%sbulk/d%02d/f%05d" % (prefix, i % 40, i), 1000 + i)
    started = time.time()
    indexer.reconcile(store, spool, aegis, settings)
    rows["reconcile"].append((run, args.entries, "first", "%.3f" % (time.time() - started)))
    started = time.time()
    indexer.reconcile(store, spool, aegis, settings)
    rows["reconcile"].append((run, args.entries, "no_change", "%.3f" % (time.time() - started)))
    aegis.entries[sorted(aegis.entries)[0]].version = 2
    started = time.time()
    indexer.reconcile(store, spool, aegis, settings)
    rows["reconcile"].append((run, args.entries, "one_change", "%.3f" % (time.time() - started)))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--entries", type=int, default=3000, help="inventory size for the reconcile test")
    parser.add_argument("--clients", default="1,4,16")
    parser.add_argument("--seconds", type=float, default=3.0, help="duration of each throughput measurement")
    parser.add_argument("--out", default=os.path.join(s.ROOT, "benchmarks", "integrations", "cassandra"))
    args = parser.parse_args()
    os.makedirs(args.out, exist_ok=True)

    token = uuid.uuid4().hex[:8]
    contact = [os.environ.get("AEGISFS_INT_CASSANDRA_CONTACT_POINTS", "127.0.0.1")]
    port = int(os.environ.get("AEGISFS_INT_CASSANDRA_PORT", "9042"))
    rows = {k: [] for k in ("index", "lookup", "throughput", "reconcile")}
    for run in range(1, args.runs + 1):
        keyspace = "aegis_bench_%s_%d" % (token, run)
        settings = s.settings(contact_points=contact, port=port, keyspace=keyspace, tls=False,
                              allow_insecure=True, workers=8)
        schema.run_migrate(settings)
        store = IndexStore.open(settings)
        spool = Spool(os.path.join("/tmp", "aegis-bench-spool-" + keyspace))
        try:
            if run == 1:
                with open(os.path.join(args.out, "environment.json"), "w") as handle:
                    json.dump(environment(settings, store), handle, indent=2, sort_keys=True)
            measure(args, run, store, settings, spool, rows)
            print("run %d/%d done" % (run, args.runs), file=sys.stderr)
        finally:
            store.close()
            cluster, session = connection.connect(settings, use_keyspace=False)
            session.execute("DROP KEYSPACE IF EXISTS %s" % keyspace)
            cluster.shutdown()

    headers = {"index": ("run", "op", "sample", "ms"), "lookup": ("run", "op", "sample", "ms"),
               "throughput": ("run", "clients", "seconds", "operations", "ops_per_second"),
               "reconcile": ("run", "entries", "pass", "seconds")}
    files = {"index": "indexLatency.csv", "lookup": "lookupLatency.csv", "throughput": "throughput.csv",
             "reconcile": "reconcile.csv"}
    for key, name in files.items():
        with open(os.path.join(args.out, name), "w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers[key])
            writer.writerows(rows[key])
    summarize(rows)


def pct(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(p / 100.0 * (len(ordered) - 1))))]


def summarize(rows):
    print("\nsummary (median / p95 over all samples)")
    for key in ("index", "lookup"):
        for op in sorted({r[1] for r in rows[key]}):
            v = [float(r[3]) for r in rows[key] if r[1] == op]
            print("  %-16s n=%-5d median %.2f ms  p95 %.2f ms" % (op, len(v), statistics.median(v), pct(v, 95)))
    for clients in sorted({r[1] for r in rows["throughput"]}):
        v = [float(r[4]) for r in rows["throughput"] if r[1] == clients]
        print("  get_by_name x%-3d clients: median %.0f ops/s over %d runs" % (clients, statistics.median(v), len(v)))
    for p in ("first", "no_change", "one_change"):
        v = [float(r[3]) for r in rows["reconcile"] if r[2] == p]
        print("  reconcile %-10s median %.2f s over %d runs (%d entries)" % (
            p, statistics.median(v), len(v), [r[1] for r in rows["reconcile"]][0]))


if __name__ == "__main__":
    main()
