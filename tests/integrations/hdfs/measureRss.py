"""Run one integrations CLI command in this process and report this process's peak memory.

    python measureRss.py hdfs export some/name /aegisfs-test/out.bin

The peak is the resident-set high-water mark of this Python process only (VmHWM on Linux) (the aegisfs client
binary it starts is a separate process and is not included). It is printed to stderr as
PEAK_RSS_KB=<n>. The exit status is the command's.
"""
import resource
import sys

import testSupport  # noqa: F401  (sets up sys.path)
from aegisfsIntegrations import cli


def peak_kb():
    # VmHWM is this process's own high-water mark. ru_maxrss can carry over the size of the
    # parent that forked it, which would overstate the number when the parent is large.
    try:
        with open("/proc/self/status") as handle:
            for line in handle:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except OSError:
        pass
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak // 1024 if sys.platform == "darwin" else peak  # bytes on macOS, KB on Linux


if __name__ == "__main__":
    code = cli.main(sys.argv[1:])
    sys.stderr.write("PEAK_RSS_KB=%d\n" % peak_kb())
    sys.exit(code)
