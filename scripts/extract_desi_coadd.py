#!/usr/bin/env python3
import argparse
from pathlib import Path

import h5py
import numpy as np
from astropy.io import fits


def find_hdu_by_name(hdul, name):
    for hdu in hdul:
        if hdu.name == name:
            return hdu
    return None


def load_band_arrays(hdul, band):
    for suffix in ("WAVELENGTH", "WAVE"):
        hdu = find_hdu_by_name(hdul, f"{band}_{suffix}")
        if hdu is not None:
            wave = hdu.data.astype("float32")
            break
    else:
        wave = None

    flux_hdu = find_hdu_by_name(hdul, f"{band}_FLUX")
    ivar_hdu = find_hdu_by_name(hdul, f"{band}_IVAR")
    mask_hdu = find_hdu_by_name(hdul, f"{band}_MASK")

    if flux_hdu is None:
        return None

    flux = flux_hdu.data.astype("float32")
    ivar = ivar_hdu.data.astype("float32") if ivar_hdu is not None else None
    mask = mask_hdu.data.astype("bool") if mask_hdu is not None else None
    return wave, flux, ivar, mask


def extract_fibermap_columns(fibermap):
    if fibermap is None:
        return {}
    cols = fibermap.columns.names
    out = {}
    for key in ("TARGETID", "TARGET_RA", "TARGET_DEC"):
        if key in cols:
            out[key.lower()] = fibermap[key]
    for key in ("RA", "DEC"):
        if key in cols and key.lower() not in out:
            out[key.lower()] = fibermap[key]
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Extract spectra + coordinates/IDs from a DESI coadd file."
    )
    parser.add_argument("--coadd", required=True, help="Path to coadd-*.fits file.")
    parser.add_argument("--out", required=True, help="Output HDF5 path.")
    parser.add_argument(
        "--bands",
        default="B,R,Z",
        help="Comma-separated list of bands to extract (default: B,R,Z).",
    )
    args = parser.parse_args()

    coadd_path = Path(args.coadd)
    out_path = Path(args.out)
    bands = [b.strip().upper() for b in args.bands.split(",") if b.strip()]

    with fits.open(coadd_path) as hdul:
        fibermap_hdu = find_hdu_by_name(hdul, "FIBERMAP")
        fibermap = fibermap_hdu.data if fibermap_hdu is not None else None
        meta = extract_fibermap_columns(fibermap)

        band_data = {}
        for band in bands:
            band_arrays = load_band_arrays(hdul, band)
            if band_arrays is None:
                print(f"Warning: band {band} not found in {coadd_path}")
                continue
            band_data[band] = band_arrays

    if not band_data:
        raise RuntimeError(f"No band data found in {coadd_path}")

    with h5py.File(out_path, "w") as out:
        for key, values in meta.items():
            out.create_dataset(key, data=values)

        for band, (wave, flux, ivar, mask) in band_data.items():
            grp = out.create_group(band.lower())
            if wave is not None:
                grp.create_dataset("wavelength", data=wave)
            grp.create_dataset("flux", data=flux)
            if ivar is not None:
                grp.create_dataset("ivar", data=ivar)
            if mask is not None:
                grp.create_dataset("mask", data=mask)

    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
