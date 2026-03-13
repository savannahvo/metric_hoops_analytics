"""
collect_data.py
---------------
Downloads the szymonjwiak/nba-traditional dataset fresh from Kaggle every time.
Never uses local copies — always performs a clean download.

Dataset contains:
    Games.csv            — one row per game (results, dates, teams)
    TeamStatistics.csv   — one row per team per game (box score stats)
    PlayerStatistics.csv — one row per player per game

Usage:
    python collect_data.py              # downloads to ml/data/
    python collect_data.py --data-dir /custom/path/

Credentials:
    Local:  ~/.kaggle/kaggle.json
    CI/CD:  KAGGLE_USERNAME + KAGGLE_KEY environment variables
"""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
import zipfile

import pandas as pd

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
DATASET = "eoinamoore/historical-nba-data-and-player-box-scores"
FILES = ["Games.csv", "TeamStatistics.csv", "PlayerStatistics.csv"]


def _configure_kaggle_env() -> None:
    """
    Set up Kaggle credentials from environment variables if present.
    Supports CI/CD environments where ~/.kaggle/kaggle.json is not available.
    """
    username = os.environ.get("KAGGLE_USERNAME")
    key = os.environ.get("KAGGLE_KEY")
    if username and key:
        log.info("Using Kaggle credentials from environment variables.")
        os.environ["KAGGLE_USERNAME"] = username
        os.environ["KAGGLE_KEY"] = key
    else:
        log.info("Using Kaggle credentials from ~/.kaggle/kaggle.json.")


def _download_dataset(dest_dir: str) -> dict[str, str]:
    """
    Download the full szymonjwiak/nba-traditional dataset zip into a temp
    directory, extract the required CSVs, and copy them to dest_dir.

    Returns a mapping of {filename: absolute_path}.
    """
    import kaggle
    kaggle.api.authenticate()

    log.info(f"Downloading dataset '{DATASET}' from Kaggle...")
    kaggle.api.dataset_download_files(
        DATASET,
        path=dest_dir,
        quiet=False,
        force=True,
        unzip=True,
    )

    paths: dict[str, str] = {}
    for fname in FILES:
        fpath = os.path.join(dest_dir, fname)

        # Some Kaggle datasets deliver files inside a sub-directory
        if not os.path.exists(fpath):
            for root, dirs, files in os.walk(dest_dir):
                if fname in files:
                    fpath = os.path.join(root, fname)
                    break

        # Handle .zip variants (older kaggle API versions)
        zpath = fpath + ".zip"
        if os.path.exists(zpath) and not os.path.exists(fpath):
            with zipfile.ZipFile(zpath, "r") as z:
                z.extractall(dest_dir)
            os.remove(zpath)

        if not os.path.exists(fpath):
            log.warning(f"  {fname} was not found after extraction — skipping.")
            continue

        paths[fname] = fpath
        size_mb = os.path.getsize(fpath) / (1024 * 1024)
        log.info(f"  {fname}  ({size_mb:.2f} MB)")

    return paths


def _copy_to_output(src: str, fname: str, out_dir: str) -> str:
    """Copy src CSV to out_dir/fname and return the destination path."""
    dest = os.path.join(out_dir, fname)
    df = pd.read_csv(src, low_memory=False)
    df.to_csv(dest, index=False)
    log.info(f"  Saved {fname} → {dest}  ({len(df):,} rows)")
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download szymonjwiak/nba-traditional from Kaggle (always fresh).",
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help=f"Output directory for downloaded CSVs (default: {DEFAULT_DATA_DIR})",
    )
    args = parser.parse_args()

    out_dir = os.path.abspath(args.data_dir)
    os.makedirs(out_dir, exist_ok=True)
    log.info(f"Output directory: {out_dir}")

    _configure_kaggle_env()

    with tempfile.TemporaryDirectory() as tmp:
        log.info(f"Downloading into temp directory: {tmp}")
        paths = _download_dataset(tmp)

        if not paths:
            log.error(
                "No CSV files were downloaded. "
                "Check your Kaggle credentials and dataset name."
            )
            raise SystemExit(1)

        log.info(f"\nCopying {len(paths)} file(s) to {out_dir} ...")
        for fname, src in paths.items():
            _copy_to_output(src, fname, out_dir)

    log.info("\nDownload complete. File sizes in output directory:")
    for fname in FILES:
        fpath = os.path.join(out_dir, fname)
        if os.path.exists(fpath):
            size_mb = os.path.getsize(fpath) / (1024 * 1024)
            rows = sum(1 for _ in open(fpath)) - 1  # minus header
            log.info(f"  {fname:<30s} {size_mb:>6.2f} MB   {rows:>8,} rows")
        else:
            log.warning(f"  {fname:<30s} NOT FOUND")

    log.info("Done.")


if __name__ == "__main__":
    main()
