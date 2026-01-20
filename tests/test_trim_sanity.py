import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.trim_coadd_matches import trim_coadd_file  # noqa: E402


class TestTrimSanity(unittest.TestCase):
    def test_trim_keeps_expected_rows(self) -> None:
        coadd_path = os.environ.get("COADD_PATH")
        csv_path = os.environ.get("CSV_PATH", "data/DESI_chandra_crossmatch_1arcsec_healpix.csv")
        if not coadd_path or not Path(coadd_path).exists():
            self.skipTest("COADD_PATH not set or file missing")

        out_path = Path(coadd_path).with_suffix(".trimmed.test.fits")
        if out_path.exists():
            out_path.unlink()

        trim_coadd_file(
            Path(coadd_path),
            Path(csv_path),
            "RA_DESI",
            "DEC_DESI",
            1.0,
            3.0,
            out_path,
            False,
        )

        self.assertTrue(out_path.exists())


if __name__ == "__main__":
    unittest.main()
