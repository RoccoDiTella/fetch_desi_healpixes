## DESI x Chandra spectra extraction (iclr)

### Objective
Extract optical DESI spectra for every row in `data/DESI_chandra_crossmatch_1arcsec_healpix.csv`,
and store them in a per-healpix layout similar to `data/desi_crossmatches/`.

### Current setup (what we already tried)
- We downloaded some `crossmatch_desi.hdf5` files from Hydra:
  `/scratch02/public/sao/jmartine/chandra_spectra/spectra3/healpix=NNNN/crossmatch_desi.hdf5`
  into `data/desi_crossmatches/healpix=NNNN/`.
- These files include DESI spectra, metadata, and Chandra crossmatch info.
- We also experimented with downloading DESI coadd spectra from the public DR1 release via Globus.
  The DR1 coverage is incomplete for our CSV healpix list, so we must target valid healpixes.

### Count crossmatches by DESI coordinates
Counts matches between the CSV and local `crossmatch_desi.hdf5` files using DESI coordinates.

```bash
python3 scripts/count_crossmatches.py
```

Optional flags:
- `--max-arcsec 1.0` to change the match radius
- `--healpix 1703` to limit to a single healpix
- `--csv /path/to/DESI_chandra_crossmatch_1arcsec_healpix.csv`
- `--hdf5-root /path/to/desi_crossmatches`

### Globus: download targeted coadds (healpix/survey/program)
`data_scripts/globus_transfer_full.py` can be constrained to specific healpixes and programs.
You must pass a destination Globus endpoint ID and path.

Example: one healpix (657) from the main survey:
```bash
uv run python3 data_scripts/globus_transfer_full.py <DEST_ENDPOINT_ID> <DEST_PATH> \
  --healpix 657 --surveys main
```

Optional filters:
- `--programs dark bright`
- `--healpix-file path/to/healpix.txt`

Notes:
- DR1 does not include all healpixes in the CSV.
- Use a healpix known to exist in DR1 (e.g., 657, 571, 644).
- Globus is file-based: we download coadd FITS files for a healpix/survey/program and filter locally.
- The transfer submits one coadd per (healpix, survey, program) match from `tilepix.fits`.
- Coadd filenames look like `coadd-<survey>-<program>-<healpix>.fits`.
- Use an absolute destination path. If you pass `home/...` instead of `/home/...`,
  Globus will create a duplicated path like `/home/USER/home/USER/...`.
- If you omit `<DEST_PATH>`, the script defaults to `/home/roccoditella/astroai/spark/iclr/data/globus`.

### Globus interface basics
1. Create/activate a Globus Personal endpoint (GCP) on your machine.
2. Use the endpoint UUID as `DEST_ENDPOINT_ID`.
3. The script authenticates via a browser login and one-time auth code.
4. It queries DR1 `tilepix.fits` to find valid `(healpix, survey, program)` entries, then submits a Globus transfer.

If the `tilepix.fits` download times out, cache it locally:
```bash
curl -L -o data/tilepix.fits \
  https://data.desi.lbl.gov/public/dr1/spectro/redux/iron/healpix/tilepix.fits
```

Then run:
```bash
uv run python3 data_scripts/globus_transfer_full.py <DEST_ENDPOINT_ID> <DEST_PATH> \
  --healpix 657 --surveys main --tilepix-cache data/tilepix.fits
```

If a cached `tilepix.fits` fails to load, re-download it; sometimes the cache is an HTML error page.

### Extract spectra from a coadd FITS file
Once a coadd file is downloaded, extract only spectra + coordinates/IDs:

```bash
python3 scripts/extract_desi_coadd.py \
  --coadd /path/to/coadd-main-dark-657.fits \
  --out /path/to/healpix657_spectra.h5
```

### Notes on identifiers for matching
- Coadd `FIBERMAP` includes `TARGETID`, `TARGET_RA`, `TARGET_DEC`, `BRICKID`, and `BRICK_OBJID`.
- In the sample file `data/globus/coadd-main-backup-2.fits`, many `BRICK_OBJID` entries are `-1`,
  so BRICKID/OBJID matching is incomplete.
- For now, RA/DEC matching is the most reliable. Once `TARGETID` is added to the CSV,
  use it for exact joins.
- The CSV and EDR crossmatch shards use HEALPix **NSIDE=16, NESTED**. A check on
  `data/desi_crossmatches/healpix=657/crossmatch_desi.hdf5` confirms 100% match with
  `healpy.ang2pix(16, ..., nest=True)` and 0% for other tested schemes.
- DR1 coadd file naming uses a different HEALPix scheme, so `healpix=657` in DR1
  does not refer to the same sky region as `healpix=657` in the CSV.
- Ordering matters even at the same NSIDE: NESTED vs RING yields different integer IDs
  unless you explicitly set `nest=True` in `healpy.ang2pix`.
- CSV RA/DEC consistency check: recomputing `ang2pix(16, ..., nest=True)` matches the
  CSV `healpix` column at ~0.99998.

### Targeted download strategy
The full DESI release is too large, so we should iterate in small batches:
1. Choose a healpix (or small set) that exists in DR1 tilepix and has many CSV rows.
2. Download the coadd file(s) for that healpix/survey/program via Globus.
3. Match spectra to CSV rows (RA/DEC now, TARGETID later) and save only matched spectra.
4. Delete the raw coadd file to keep storage small; repeat for the next batch.

### Recommended workflow (smallest download)
1. Compute DR1 pix64 (NSIDE=64, NESTED) from RA/DEC and dedupe the list.
2. Use those pix64 values to drive targeted Globus downloads.
3. Optionally use the p16->p64 child relationship to narrow candidates, then
   select populated children via RA/DEC.

Add pix64 columns to the CSV:
```bash
uv run python3 scripts/add_desi_pix64_to_csv.py data/DESI_chandra_crossmatch_1arcsec_healpix.csv
```

This writes `data/DESI_chandra_crossmatch_1arcsec_healpix.with_pix64.csv` with:
- `pix64` (DESI DR1 pixel, NSIDE=64 NESTED)
- `pix64_group` (directory group = pix64 // 100)

Generate a count table (sorted by frequency):
```bash
uv run python3 - <<'PY'
import pandas as pd
df = pd.read_csv('data/DESI_chandra_crossmatch_1arcsec_healpix.with_pix64.csv', usecols=['pix64'])
counts = df['pix64'].value_counts().reset_index()
counts.columns = ['pix64', 'count']
counts['pix64_group'] = (counts['pix64'] // 100).astype('int64')
counts.sort_values('count', ascending=False).to_csv('data/desi_pix64_counts.csv', index=False)
print('Wrote data/desi_pix64_counts.csv')
PY
```

### DR1 download targeting
- Query Globus by `pix64` (the full pixel id), not by `pix64_group`.
- The `pix64_group` is only used for the directory path.

Example path for `pix64=27258`:
```
/dr1/spectro/redux/iron/healpix/<survey>/<program>/272/27258/coadd-<survey>-<program>-27258.fits
```

### Sanity checks to keep
- Ensure coadd files expose B/R/Z spectra + IVAR + masks in FITS HDUs.
- Confirm CSV healpix consistency (NSIDE=16, NESTED) before using pix64 for DR1.
