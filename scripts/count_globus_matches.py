#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd
from astropy.io import fits
from astropy.coordinates import SkyCoord
import astropy.units as u


def parse_args():
    parser = argparse.ArgumentParser(
        description="Count CSV matches against local Globus coadd files using RA/DEC proximity."
    )
    parser.add_argument(
        "--csv",
        default="data/DESI_chandra_crossmatch_1arcsec_healpix.csv",
        help="CSV path containing RA_DESI/DEC_DESI.",
    )
    parser.add_argument(
        "--roots",
        nargs="+",
        default=[
            "data/globus",
            "/home/roccoditella/home/roccoditella/astroai/spark/iclr/data/globus",
        ],
        help="One or more directories to search for coadd-*.fits files.",
    )
    parser.add_argument(
        "--max-arcsec",
        type=float,
        default=1.0,
        help="Maximum separation in arcsec for a match (default: 1.0).",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=None,
        help="Limit the number of coadd files processed (for quick tests).",
    )
    parser.add_argument(
        "--csv-limit",
        type=int,
        default=None,
        help="Limit the number of CSV rows (for quick tests).",
    )
    return parser.parse_args()


def load_csv_coords(csv_path: Path, limit: int | None) -> SkyCoord:
    df = pd.read_csv(csv_path, usecols=["RA_DESI", "DEC_DESI"])
    if limit is not None:
        df = df.head(limit)
    return SkyCoord(ra=df["RA_DESI"].values * u.deg, dec=df["DEC_DESI"].values * u.deg)


def load_fibermap_coords(path: Path) -> SkyCoord:
    with fits.open(path) as hdul:
        fm = hdul["FIBERMAP"].data
        cols = fm.columns.names
        if "TARGET_RA" in cols and "TARGET_DEC" in cols:
            ra = fm["TARGET_RA"]
            dec = fm["TARGET_DEC"]
        elif "RA" in cols and "DEC" in cols:
            ra = fm["RA"]
            dec = fm["DEC"]
        else:
            raise RuntimeError(f"No RA/DEC columns found in {path}")
    return SkyCoord(ra=ra * u.deg, dec=dec * u.deg)


def main():
    args = parse_args()
    csv_path = Path(args.csv)
    csv_coords = load_csv_coords(csv_path, args.csv_limit)

    roots = [Path(r) for r in args.roots]
    files = []
    for root in roots:
        files.extend(sorted(root.glob("coadd-*.fits")))
    if args.limit_files is not None:
        files = files[: args.limit_files]

    print(f"CSV rows: {len(csv_coords)}")
    print(f"Coadd files found: {len(files)}")

    total_csv_matches = set()

    for path in files:
        coadd_coords = load_fibermap_coords(path)
        idx_csv, idx_coadd, _, _ = csv_coords.search_around_sky(
            coadd_coords, args.max_arcsec * u.arcsec
        )
        csv_matches = set(idx_csv.tolist())
        total_csv_matches.update(csv_matches)

        print(
            f"{path.name}: targets={len(coadd_coords)} "
            f"csv_matches<= {args.max_arcsec}\" = {len(csv_matches)}"
        )

    print(f"TOTAL unique CSV matches<= {args.max_arcsec}\": {len(total_csv_matches)}")


if __name__ == "__main__":
    main()
