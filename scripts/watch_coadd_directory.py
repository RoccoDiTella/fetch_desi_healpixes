#!/usr/bin/env python3
import argparse
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.trim_coadd_matches import trim_coadd_file  # noqa: E402


def should_process(path: Path, min_age_seconds: int) -> bool:
    age = time.time() - path.stat().st_mtime
    return age >= min_age_seconds


def process_test_mode(path: Path, test_output_dir: Path) -> None:
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    target = test_output_dir / f"{path.stem}_{ts}{path.suffix}"
    target.parent.mkdir(parents=True, exist_ok=True)
    path.rename(target)
    print(f"moved test file: {path} -> {target}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch a directory for coadd files and trim matches.")
    parser.add_argument("root", help="Directory containing coadd-*.fits files.")
    parser.add_argument("csv", help="CSV path with RA/DEC columns.")
    parser.add_argument("--ra-col", default="RA_DESI")
    parser.add_argument("--dec-col", default="DEC_DESI")
    parser.add_argument("--max-arcsec", type=float, default=1.0)
    parser.add_argument("--report-arcsec", type=float, default=3.0)
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--min-age-seconds", type=int, default=30)
    parser.add_argument("--delete-original", action="store_true")
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--test-output-dir", default="watcher_test")
    args = parser.parse_args()

    root = Path(args.root)
    csv_path = Path(args.csv)
    test_output_dir = Path(args.test_output_dir)

    seen = set()
    while True:
        files = sorted(root.glob("coadd-*.fits"))
        for path in files:
            trimmed_path = path.with_suffix(".trimmed.fits")
            if trimmed_path.exists():
                continue
            if path in seen and not args.test_mode:
                continue
            if not should_process(path, args.min_age_seconds):
                continue

            if args.test_mode:
                process_test_mode(path, test_output_dir)
                seen.add(path)
                continue

            trim_coadd_file(
                path,
                csv_path,
                args.ra_col,
                args.dec_col,
                args.max_arcsec,
                args.report_arcsec,
                None,
                args.delete_original,
            )
            seen.add(path)

        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
