#!/usr/bin/env python3
import argparse
from pathlib import Path

import h5py
import numpy as np
import pandas as pd


def min_sep_arcsec_chunked(ra1, dec1, ra2, dec2, chunk_size=1000):
    ra2r = np.deg2rad(ra2)
    dec2r = np.deg2rad(dec2)
    sin_dec2 = np.sin(dec2r)[None, :]
    cos_dec2 = np.cos(dec2r)[None, :]

    min_sep = np.full(len(ra1), np.inf, dtype=np.float64)
    for start in range(0, len(ra1), chunk_size):
        end = min(start + chunk_size, len(ra1))
        ra1r = np.deg2rad(ra1[start:end])[:, None]
        dec1r = np.deg2rad(dec1[start:end])[:, None]
        sin_dec1 = np.sin(dec1r)
        cos_dec1 = np.cos(dec1r)
        cos_dra = np.cos(ra1r - ra2r[None, :])
        cos_sep = sin_dec1 * sin_dec2 + cos_dec1 * cos_dec2 * cos_dra
        cos_sep = np.clip(cos_sep, -1.0, 1.0)
        sep = np.arccos(cos_sep)
        sep_arcsec = np.rad2deg(sep) * 3600.0
        min_sep[start:end] = sep_arcsec.min(axis=1)
    return min_sep


def parse_args():
    parser = argparse.ArgumentParser(description="Count DESI crossmatches by coordinate proximity.")
    parser.add_argument(
        "--csv",
        default="data/DESI_chandra_crossmatch_1arcsec_healpix.csv",
        help="Path to DESI+Chandra CSV with RA_DESI/DEC_DESI/healpix columns.",
    )
    parser.add_argument(
        "--hdf5-root",
        default="data/desi_crossmatches",
        help="Root directory containing healpix=*/crossmatch_desi.hdf5 files.",
    )
    parser.add_argument(
        "--max-arcsec",
        type=float,
        default=1.0,
        help="Maximum angular separation (arcsec) for a match.",
    )
    parser.add_argument(
        "--healpix",
        type=int,
        default=None,
        help="Optional healpix to limit the scan.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Chunk size for coordinate matching to reduce memory usage.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    csv_path = Path(args.csv)
    hdf5_root = Path(args.hdf5_root)

    csv = pd.read_csv(csv_path, usecols=["RA_DESI", "DEC_DESI", "healpix"])
    if args.healpix is not None:
        csv = csv[csv["healpix"] == args.healpix]

    grouped = {hp: grp for hp, grp in csv.groupby("healpix")}

    paths = sorted(hdf5_root.glob("healpix=*/crossmatch_desi.hdf5"))
    if args.healpix is not None:
        paths = [p for p in paths if p.parent.name == f"healpix={args.healpix}"]

    total_csv = 0
    total_hdf = 0
    total_matches = 0

    for path in paths:
        healpix = int(path.parent.name.split("=")[1])
        csv_hp = grouped.get(healpix)
        if csv_hp is None or len(csv_hp) == 0:
            print(f"healpix={healpix}: CSV rows=0; skipping")
            continue

        try:
            with h5py.File(path, "r") as f:
                ra2 = f["desi/edr_sv3_ra"][:]
                dec2 = f["desi/edr_sv3_dec"][:]
        except OSError as e:
            print(f"healpix={healpix}: failed to open HDF5 ({e}); skipping")
            continue

        ra1 = csv_hp["RA_DESI"].values
        dec1 = csv_hp["DEC_DESI"].values

        min_sep = min_sep_arcsec_chunked(
            ra1, dec1, ra2, dec2, chunk_size=args.chunk_size
        )
        matches = int((min_sep <= args.max_arcsec).sum())

        total_csv += len(csv_hp)
        total_hdf += len(ra2)
        total_matches += matches

        print(
            f"healpix={healpix}: CSV rows={len(csv_hp)} | "
            f"HDF rows={len(ra2)} | matches<= {args.max_arcsec}\" = {matches}"
        )

    print(
        f"TOTAL: CSV rows={total_csv} | HDF rows={total_hdf} | "
        f"matches<= {args.max_arcsec}\" = {total_matches}"
    )


if __name__ == "__main__":
    main()
