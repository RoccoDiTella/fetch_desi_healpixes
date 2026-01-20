#!/usr/bin/env python3
import argparse
import math

import numpy as np
import pandas as pd
import healpy as hp


def guess_nside_from_maxpix(maxpix: int) -> int:
    npix = maxpix + 1
    nside = int(round(math.sqrt(npix / 12.0)))
    if 12 * nside * nside != npix:
        return nside
    return nside


def ang2pix_from_radec(nside: int, ra_deg: np.ndarray, dec_deg: np.ndarray, nest: bool) -> np.ndarray:
    theta = np.deg2rad(90.0 - dec_deg)
    phi = np.deg2rad(np.mod(ra_deg, 360.0))
    return hp.ang2pix(nside, theta, phi, nest=nest)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", help="Path to CSV")
    ap.add_argument("--ra-col", default="RA_DESI")
    ap.add_argument("--dec-col", default="DEC_DESI")
    ap.add_argument("--hp-col", default="healpix")
    ap.add_argument("--max-rows", type=int, default=200000)
    ap.add_argument("--out-pix64", default="desi_dr1_pix64_unique.txt")
    ap.add_argument("--out-pix64-csv", default="desi_dr1_pix64_unique.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.csv, nrows=args.max_rows)

    ra = df[args.ra_col].to_numpy(dtype=float)
    dec = df[args.dec_col].to_numpy(dtype=float)
    hp_in = df[args.hp_col].to_numpy(dtype=np.int64)

    maxpix = int(np.nanmax(hp_in))
    minpix = int(np.nanmin(hp_in))
    nunique = int(pd.Series(hp_in).nunique())

    nside_guess = guess_nside_from_maxpix(maxpix)

    print("=== Input healpix column summary ===")
    print(f"min={minpix} max={maxpix} nunique={nunique}")
    print(
        f"nside_guess_from_maxpix={nside_guess} "
        f"(expected max ~ {12 * nside_guess * nside_guess - 1})"
    )

    candidates = sorted(
        set([nside_guess] + [nside_guess // 2, nside_guess * 2, 8, 16, 32, 64, 128])
    )
    candidates = [n for n in candidates if n >= 1]

    best = None
    print("\n=== Matching (recompute pix from RA/DEC and compare to healpix column) ===")
    for nside in candidates:
        for nest in (True, False):
            pix = ang2pix_from_radec(nside, ra, dec, nest=nest)
            match = (pix == hp_in).mean()
            tag = "NESTED" if nest else "RING"
            print(f"nside={nside:>4} ordering={tag:>6}  match_rate={match:.6f}")
            if best is None or match > best[0]:
                best = (match, nside, nest)

    best_match, best_nside, best_nest = best
    print("\n=== Best inferred scheme for healpix column ===")
    print(
        f"best_match_rate={best_match:.6f}  "
        f"nside={best_nside}  ordering={'NESTED' if best_nest else 'RING'}"
    )

    if best_match < 0.999:
        print("\nWARNING: match_rate < 0.999.")
        print("The healpix column may be a shard/bucket id or computed differently.")

    # DESI DR1 coadds use NSIDE=64, NESTED
    pix64 = ang2pix_from_radec(64, ra, dec, nest=True)
    pix64_unique = np.unique(pix64)

    out_df = pd.DataFrame(
        {
            "pix64": pix64_unique,
            "group": pix64_unique // 100,
        }
    ).sort_values(["group", "pix64"])

    out_df.to_csv(args.out_pix64_csv, index=False)
    with open(args.out_pix64, "w") as f:
        for p in out_df["pix64"].to_numpy():
            f.write(f"{int(p)}\n")

    print("\n=== Output ===")
    print(f"Wrote {len(pix64_unique)} unique DESI DR1 pixels (NSIDE=64 NESTED)")
    print(f"- {args.out_pix64_csv}  (pix64 + group)")
    print(f"- {args.out_pix64}      (pix64 list)")


if __name__ == "__main__":
    main()
