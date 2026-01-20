#!/usr/bin/env python3
import argparse
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate dummy files for watcher test.")
    parser.add_argument("root", help="Directory to write dummy files into.")
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)

    for i in range(args.count):
        path = root / f"coadd-test-{i}.fits"
        path.write_text(f"dummy {i}\n")
        print(f"created {path}")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
