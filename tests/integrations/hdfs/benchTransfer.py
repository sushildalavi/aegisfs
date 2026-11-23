"""Measure HDFS export/import throughput, small-file overhead and peak memory.

Run it inside the connector container (runBench.sh does that). Every sample is one real command
line: `hdfs export` or `hdfs import`, run as its own process, timed from outside, with the
process's peak resident memory taken from measureRss.py. For comparison the same files are also
written and read with the native `aegisfs create` / `aegisfs get`. Those do different work with
different guarantees (no second copy, no read-back hash), so the numbers are context, not a
contest.

Output, in --out (default /benchmarks-out when mounted, else ./bench-out):
    transfers.csv   one row per sample (raw)
    summary.csv     median, min, max per scenario
    environment.json   hardware and software versions it ran on
"""
import argparse
import csv
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

import testSupport  # noqa: F401
from aegisfsIntegrations.aegisfsClient import AegisClient
from aegisfsIntegrations.config import Config

HERE = os.path.dirname(os.path.abspath(__file__))
LAUNCHER = next(p for p in (os.path.join(HERE, "..", "..", "..", "extensions", "integrations",
                                         "aegisfs-integrations"),
                            "/opt/aegisfs-integrations/aegisfs-integrations")
                if os.path.exists(p))
FIELDS = ["run_id", "scenario", "operation", "size_bytes", "files", "repeat", "wall_seconds",
          "tool_seconds", "mb_per_s", "peak_rss_kb", "ok"]


def write_file(path, size):
    block = os.urandom(min(size, 1024 * 1024)) if size else b""
    with open(path, "wb") as handle:
        left = size
        while left > 0:
            handle.write(block[:left])
            left -= len(block)


def timed(argv, env=None):
    started = time.monotonic()
    proc = subprocess.run(argv, capture_output=True, text=True, env=env)
    return time.monotonic() - started, proc


def rss_of(stderr):
    for line in stderr.splitlines():
        if line.startswith("PEAK_RSS_KB="):
            return int(line.split("=")[1])
    return ""


def tool(*args):
    return [sys.executable, os.path.join(HERE, "measureRss.py"), "--json"] + list(args)


class Bench:
    def __init__(self, out, run_id, hdfs_dir, prefix):
        self.rows = []
        self.out = out
        self.run_id = run_id
        self.hdfs_dir = hdfs_dir
        self.prefix = prefix
        cfg = Config.load()
        self.aegis = AegisClient.from_config(cfg)
        self.tmp = tempfile.mkdtemp(prefix="bench-")

    def row(self, scenario, operation, size, files, repeat, wall, tool_seconds, rss, ok):
        total = size * files
        self.rows.append({
            "run_id": self.run_id, "scenario": scenario, "operation": operation,
            "size_bytes": size, "files": files, "repeat": repeat,
            "wall_seconds": round(wall, 4), "tool_seconds": tool_seconds,
            "mb_per_s": round(total / 1e6 / wall, 3) if wall > 0 and ok else "",
            "peak_rss_kb": rss, "ok": int(ok)})
        print("%-22s %-14s %10d B x%-4d #%d  %8.3fs  %s" % (
            scenario, operation, size, files, repeat, wall, "ok" if ok else "FAILED"), flush=True)

    def large(self, label, size, repeats):
        src = os.path.join(self.tmp, label)
        write_file(src, size)
        for rep in range(repeats):
            tag = "%s-%d-%s" % (label, rep, uuid.uuid4().hex[:6])
            name, imp_name = self.prefix + tag, self.prefix + tag + "-imp"
            hdfs_path = "%s/%s.dat" % (self.hdfs_dir, tag)
            # native create/get: the baseline
            wall, proc = timed([self.aegis.binary, "--entry", self.aegis.entry, "create", src,
                                name + "-native"])
            self.row(label, "native_create", size, 1, rep, wall, "", "", "status=OK" in proc.stdout)
            dest = os.path.join(self.tmp, "native-get")
            wall, proc = timed([self.aegis.binary, "--entry", self.aegis.entry, "get",
                                name + "-native", dest])
            self.row(label, "native_get", size, 1, rep, wall, "", "", "status=OK" in proc.stdout)
            os.unlink(dest) if os.path.exists(dest) else None
            # the export source
            self.aegis.create(src, name)
            wall, proc = timed(tool("hdfs", "export", name, hdfs_path))
            self.row(label, "hdfs_export", size, 1, rep, wall, self._tool_seconds(proc),
                     rss_of(proc.stderr), proc.returncode == 0)
            wall, proc = timed(tool("hdfs", "import", hdfs_path, imp_name))
            self.row(label, "hdfs_import", size, 1, rep, wall, self._tool_seconds(proc),
                     rss_of(proc.stderr), proc.returncode == 0)
        os.unlink(src)

    @staticmethod
    def _tool_seconds(proc):
        try:
            return json.loads(proc.stdout).get("seconds", "")
        except ValueError:
            return ""

    def small(self, count, size, repeats):
        label = "small-%dB" % size
        for rep in range(repeats):
            tag = "%s-%d-%s" % (label, rep, uuid.uuid4().hex[:6])
            names = []
            for i in range(count):
                src = os.path.join(self.tmp, "s%d" % i)
                write_file(src, size)
                name = "%s%s/f%03d" % (self.prefix, tag, i)
                self.aegis.create(src, name)
                names.append(name)
            # one process per file
            started = time.monotonic()
            ok = True
            for i, name in enumerate(names):
                _, proc = timed(tool("hdfs", "export", name, "%s/%s/f%03d" % (self.hdfs_dir, tag, i)))
                ok = ok and proc.returncode == 0
            self.row(label + "-per-process", "hdfs_export", size, count, rep,
                     time.monotonic() - started, "", "", ok)
            started = time.monotonic()
            ok = True
            for i in range(count):
                _, proc = timed(tool("hdfs", "import", "%s/%s/f%03d" % (self.hdfs_dir, tag, i),
                                     "%s%s-imp/f%03d" % (self.prefix, tag, i)))
                ok = ok and proc.returncode == 0
            self.row(label + "-per-process", "hdfs_import", size, count, rep,
                     time.monotonic() - started, "", "", ok)
            # one process for all files (batch with journal)
            state = os.path.join(self.tmp, "state-" + tag)
            env = dict(os.environ, AEGISFS_INT_HDFS_STATE_DIR=state)
            wall, proc = timed(tool("hdfs", "export-all", "--prefix", "%s%s/" % (self.prefix, tag),
                                    "--to", "%s/%s-batch" % (self.hdfs_dir, tag)), env)
            self.row(label + "-batch", "hdfs_export", size, count, rep, wall, "", rss_of(proc.stderr),
                     proc.returncode == 0)
            wall, proc = timed(tool("hdfs", "import-all", "%s/%s-batch" % (self.hdfs_dir, tag),
                                    "--prefix", "%s%s-batch/" % (self.prefix, tag)), env)
            self.row(label + "-batch", "hdfs_import", size, count, rep, wall, "", rss_of(proc.stderr),
                     proc.returncode == 0)
            # native, one process per file
            started = time.monotonic()
            for i in range(count):
                src = os.path.join(self.tmp, "s%d" % i)
                subprocess.run([self.aegis.binary, "--entry", self.aegis.entry, "get", names[i],
                                src + ".got"], capture_output=True)
            self.row(label + "-per-process", "native_get", size, count, rep,
                     time.monotonic() - started, "", "", True)

    def save(self):
        os.makedirs(self.out, exist_ok=True)
        with open(os.path.join(self.out, "transfers.csv"), "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(self.rows)
        groups = {}
        for r in self.rows:
            if r["ok"]:
                groups.setdefault((r["scenario"], r["operation"], r["size_bytes"], r["files"]),
                                  []).append(r)
        with open(os.path.join(self.out, "summary.csv"), "w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["scenario", "operation", "size_bytes", "files", "samples",
                             "median_seconds", "min_seconds", "max_seconds", "median_mb_per_s",
                             "median_seconds_per_file", "max_peak_rss_kb"])
            for (scenario, op, size, files), rows in sorted(groups.items()):
                walls = [r["wall_seconds"] for r in rows]
                rates = [r["mb_per_s"] for r in rows if r["mb_per_s"] != ""]
                rss = [r["peak_rss_kb"] for r in rows if r["peak_rss_kb"] != ""]
                writer.writerow([scenario, op, size, files, len(rows),
                                 round(statistics.median(walls), 4), min(walls), max(walls),
                                 round(statistics.median(rates), 3) if rates else "",
                                 round(statistics.median(walls) / files, 4),
                                 max(rss) if rss else ""])


def environment():
    def run(*cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=20).stdout.strip()
        except Exception:  # noqa: BLE001
            return ""
    env = {k[len("BENCH_"):].lower(): v for k, v in os.environ.items() if k.startswith("BENCH_")}
    env.update({
        "connector_python": platform.python_version(),
        "connector_platform": platform.platform(),
        "connector_machine": platform.machine(),
        "connector_cpus": os.cpu_count(),
        "aegisfs_binary": run("aegisfs", "help").splitlines()[0] if run("aegisfs", "help") else "",
        "namenode_url": os.environ.get("AEGISFS_INT_HDFS_NAMENODE_URL", ""),
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "notes": ("One laptop. HDFS is 1 NameNode + 2 DataNodes (replication 2) in Docker, the "
                  "apache/hadoop image is linux/amd64 and runs under emulation on Apple "
                  "silicon. AegisFS is 3 nodes in Docker on the same machine. Peak RSS is the "
                  "Python process only."),
    })
    return env


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="/benchmarks-out" if os.path.isdir("/benchmarks-out")
                        else "bench-out")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--large-repeats", type=int, default=3)
    parser.add_argument("--small-count", type=int, default=30)
    parser.add_argument("--sizes-mib", default="1,10,100")
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:8]
    bench = Bench(args.out, run_id, "/aegisfs-test/bench-" + run_id, "hdfs-bench/%s/" % run_id)
    bench.hdfs_dir_made = False
    from aegisfsIntegrations.hdfs import settings as hdfsSettings
    from aegisfsIntegrations.hdfs.webHdfs import WebHdfsClient
    settings, _ = hdfsSettings.load(Config.load())
    WebHdfsClient(settings).mkdirs(bench.hdfs_dir)
    try:
        bench.small(args.small_count, 1024, args.repeats)
        for mib in [int(x) for x in args.sizes_mib.split(",") if x]:
            bench.large("%dMiB" % mib, mib * 1024 * 1024,
                        args.large_repeats if mib >= 100 else args.repeats)
    finally:
        bench.save()
        with open(os.path.join(args.out, "environment.json"), "w") as handle:
            json.dump(environment(), handle, indent=2, sort_keys=True)
    print("wrote results to", args.out)


if __name__ == "__main__":
    main()
