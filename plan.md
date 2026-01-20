## Plan: DESI x Chandra spectra extraction

1. Validate coadd contents: confirm B/R/Z spectra, IVAR, masks, and FIBERMAP coords (targets) so the coadds can feed the normalizing-flows dataloaders.
2. Add a healpix consistency test (CSV RA/DEC -> p16 nested) and warn on mismatches.
3. Pick the highest-count pix64 from `data/desi_pix64_counts.csv`, download that coadd via Globus.
4. Process the coadd: match to CSV (RA/DEC), build a crossmatch HDF5 in the same layout as `data/desi_crossmatches`, and add a loader test against `normalizing-flows` expectations.
5. Delete raw coadd FITS files and legacy/incorrect downloads once matches are confirmed.
6. Iterate pix64 one-by-one (download → match → store → delete), then automate batches.
