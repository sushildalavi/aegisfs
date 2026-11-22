#!/usr/bin/env python3
"""Generate deterministic fixture files for AegisFS benchmarks.

Usage: gen_data.py <out_dir> [sizes_kb...]
Creates reproducible pseudo-random files <out_dir>/fixture_<kb>k.bin for each size (default
1, 1024, 10240, 102400 KiB). Deterministic per size so runs are comparable. Stdlib only.
"""
import os
import sys


def write_fixture(path: str, size_bytes: int, seed: int) -> None:
    x = (seed * 2862933555777941757 + 3037000493) & 0xFFFFFFFFFFFFFFFF
    with open(path, "wb") as f:
        written = 0
        buf = bytearray(65536)
        while written < size_bytes:
            chunk = min(len(buf), size_bytes - written)
            for i in range(chunk):
                x ^= (x << 13) & 0xFFFFFFFFFFFFFFFF
                x ^= x >> 7
                x ^= (x << 17) & 0xFFFFFFFFFFFFFFFF
                buf[i] = x & 0xFF
            f.write(bytes(buf[:chunk]))
            written += chunk


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    out = sys.argv[1]
    sizes = [int(s) for s in sys.argv[2:]] or [1, 1024, 10240, 102400]
    os.makedirs(out, exist_ok=True)
    for kb in sizes:
        path = os.path.join(out, f"fixture_{kb}k.bin")
        write_fixture(path, kb * 1024, seed=kb + 1)
        print(f"wrote {path} ({kb} KiB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
