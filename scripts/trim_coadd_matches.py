#!/usr/bin/env python3
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord, search_around_sky
import astropy.units as u
from astropy.io import fits


def load_csv_coords(csv_path: Path, ra_col: str, dec_col: str) -> SkyCoord:
    df = pd.read_csv(csv_path, usecols=[ra_col, dec_col])
    return SkyCoord(ra=df[ra_col].values * u.deg, dec=df[dec_col].values * u.deg)


def load_coadd_coords(hdul: fits.HDUList) -> SkyCoord:
    fm = hdul["FIBERMAP"].data
    cols = fm.columns.names
    if "TARGET_RA" in cols and "TARGET_DEC" in cols:
        ra = fm["TARGET_RA"]
        dec = fm["TARGET_DEC"]
    elif "RA" in cols and "DEC" in cols:
        ra = fm["RA"]
        dec = fm["DEC"]
    else:
        raise RuntimeError("FIBERMAP does not contain RA/DEC columns.")
    return SkyCoord(ra=ra * u.deg, dec=dec * u.deg)


def find_match_indices(csv_coords: SkyCoord, coadd_coords: SkyCoord, max_arcsec: float) -> np.ndarray:
    idx_coadd, idx_csv, _, _ = search_around_sky(coadd_coords, csv_coords, max_arcsec * u.arcsec)
    if len(idx_coadd) == 0:
        return np.array([], dtype=int)
    return np.unique(idx_coadd)


def trim_hdul(hdul: fits.HDUList, keep_idx: np.ndarray) -> fits.HDUList:
    nrows = len(hdul["FIBERMAP"].data)
    keep_mask = np.zeros(nrows, dtype=bool)
    keep_mask[keep_idx] = True

    new_hdus = []
    for hdu in hdul:
        if hdu.data is None:
            new_hdus.append(hdu.copy())
            continue
        data = hdu.data
        if isinstance(hdu, fits.BinTableHDU) and data.shape and data.shape[0] == nrows:
            new_data = data[keep_mask]
            new_hdus.append(fits.BinTableHDU(new_data, header=hdu.header, name=hdu.name))
        elif isinstance(hdu, fits.ImageHDU) and data.shape and data.shape[0] == nrows:
            new_data = data[keep_mask]
            new_hdus.append(fits.ImageHDU(new_data, header=hdu.header, name=hdu.name))
        else:
            new_hdus.append(hdu.copy())
    return fits.HDUList(new_hdus)


def trim_coadd_file(
    coadd_path: Path,
    csv_path: Path,
    ra_col: str,
    dec_col: str,
    max_arcsec: float,
    report_arcsec: float,
    out_path: Path | None,
    delete_original: bool,
) -> None:
    out_path = out_path or coadd_path.with_suffix(".trimmed.fits")

    csv_coords = load_csv_coords(csv_path, ra_col, dec_col)

    with fits.open(coadd_path) as hdul:
        coadd_coords = load_coadd_coords(hdul)
        keep_idx = find_match_indices(csv_coords, coadd_coords, max_arcsec)
        report_idx = find_match_indices(csv_coords, coadd_coords, report_arcsec)
        trimmed = trim_hdul(hdul, keep_idx)

    trimmed.writeto(out_path, overwrite=True)

    nrows = len(coadd_coords)
    print(f"coadd rows: {nrows}")
    print(f"matched rows (<= {max_arcsec}\"): {len(keep_idx)}")
    print(f"matched rows (<= {report_arcsec}\"): {len(report_idx)}")
    print(f"wrote: {out_path}")

    if len(keep_idx) != len(trimmed["FIBERMAP"].data):
        raise RuntimeError("Trimmed FIBERMAP row count does not match kept indices.")

    if delete_original:
        coadd_path.unlink()
        print(f"deleted original: {coadd_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Trim a DESI coadd FITS to matched rows.")
    parser.add_argument("coadd", help="Path to coadd-*.fits")
    parser.add_argument("csv", help="CSV path with RA/DEC columns")
    parser.add_argument("--ra-col", default="RA_DESI")
    parser.add_argument("--dec-col", default="DEC_DESI")
    parser.add_argument("--max-arcsec", type=float, default=1.0)
    parser.add_argument("--report-arcsec", type=float, default=3.0)
    parser.add_argument("--out", default=None, help="Output trimmed FITS path")
    parser.add_argument("--delete-original", action="store_true")
    args = parser.parse_args()

    trim_coadd_file(
        Path(args.coadd),
        Path(args.csv),
        args.ra_col,
        args.dec_col,
        args.max_arcsec,
        args.report_arcsec,
        Path(args.out) if args.out else None,
        args.delete_original,
    )


if __name__ == "__main__":
    main()
