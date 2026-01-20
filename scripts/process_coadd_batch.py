#!/usr/bin/env python3
import argparse
from pathlib import Path

from scripts.trim_coadd_matches import trim_coadd_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Process a directory of coadd files.")
    parser.add_argument("root", help="Directory containing coadd-*.fits files.")
    parser.add_argument("csv", help="CSV path with RA/DEC columns.")
    parser.add_argument("--ra-col", default="RA_DESI")
    parser.add_argument("--dec-col", default="DEC_DESI")
    parser.add_argument("--max-arcsec", type=float, default=1.0)
    parser.add_argument("--report-arcsec", type=float, default=3.0)
    parser.add_argument("--delete-original", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    files = sorted(root.glob("coadd-*.fits"))
    if not files:
        print(f"No coadd files found under {root}")
        return

    for path in files:
        trim_coadd_file(
            path,
            Path(args.csv),
            args.ra_col,
            args.dec_col,
            args.max_arcsec,
            args.report_arcsec,
            None,
            args.delete_original,
        )


if __name__ == "__main__":
    main()
