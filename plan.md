## Plan: DESI x Chandra spectra extraction

1. Validate coadd contents: confirm B/R/Z spectra, IVAR, masks, and FIBERMAP coords (targets) so we can match reliably.
2. Add a healpix consistency test (CSV RA/DEC -> p16 nested) and warn on mismatches.
3. Download coadd files for a pix64 (start with the highest count from `data/desi_pix64_counts.csv`).
4. Crossmatch by RA/DEC (TARGETID later), keep only matched rows, and drop non‑matches; report 3 arcsec counts.
5. Automate: watch the download folder and process each new coadd file as it arrives.
6. Test watcher loop with dummy files (timestamp rename + move) before using real coadds.
7. Compare watcher output vs offline batch processing for the same directory.
8. Iterate pix64 one‑by‑one (download → match → save trimmed → delete raw).

Open issue:
- Watcher crashed when CSV path was truncated; add CSV existence validation and
  clearer error message before running large batches.
