#!/usr/bin/env python3
import argparse

import numpy as np
import pandas as pd
import healpy as hp


def desi_pix64_from_radec(ra_deg, dec_deg):
    ra_deg = np.asarray(ra_deg, dtype=float)
    dec_deg = np.asarray(dec_deg, dtype=float)
    theta = np.deg2rad(90.0 - dec_deg)
    phi = np.deg2rad(np.mod(ra_deg, 360.0))
    return hp.ang2pix(64, theta, phi, nest=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add DESI DR1 pix64 (NSIDE=64, NESTED) to a CSV."
    )
    parser.add_argument("csv", help="Input CSV path.")
    parser.add_argument(
        "--ra-col", default="RA_DESI", help="RA column name (default: RA_DESI)."
    )
    parser.add_argument(
        "--dec-col", default="DEC_DESI", help="DEC column name (default: DEC_DESI)."
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output CSV path (default: <input>.with_pix64.csv).",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    pix64 = desi_pix64_from_radec(df[args.ra_col].values, df[args.dec_col].values)
    df["pix64"] = pix64.astype("int64")
    df["pix64_group"] = (df["pix64"] // 100).astype("int64")

    out_path = args.out
    if out_path is None:
        out_path = args.csv.replace(".csv", ".with_pix64.csv")

    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
