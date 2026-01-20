import unittest

import numpy as np
import pandas as pd

try:
    import healpy as hp
except Exception as exc:  # pragma: no cover - hard dependency for this test
    raise ImportError("healpy is required for healpix consistency tests") from exc


class TestHealpixConsistency(unittest.TestCase):
    def test_csv_healpix_matches_ra_dec(self) -> None:
        csv_path = "data/DESI_chandra_crossmatch_1arcsec_healpix.csv"
        df = pd.read_csv(csv_path, usecols=["RA_DESI", "DEC_DESI", "healpix"])

        ra = df["RA_DESI"].values
        dec = df["DEC_DESI"].values
        theta = np.deg2rad(90.0 - dec)
        phi = np.deg2rad(np.mod(ra, 360.0))

        pix16 = hp.ang2pix(16, theta, phi, nest=True)
        match_rate = (pix16 == df["healpix"].values).mean()

        # Allow a small fraction of mismatches for edge cases or corrupted rows.
        self.assertGreaterEqual(match_rate, 0.999, f"Match rate too low: {match_rate:.6f}")


if __name__ == "__main__":
    unittest.main()
