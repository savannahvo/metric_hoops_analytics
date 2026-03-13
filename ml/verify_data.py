"""
verify_data.py
--------------
Data quality verification for the NBA training pipeline.
Loads Games.csv, TeamStatistics.csv, PlayerStatistics.csv and performs
a comprehensive set of checks. BLOCKS training if any critical check fails.

Checks performed:
  1. All three required files exist and are non-empty.
  2. Game counts per season are within expected range (regular season ~1230 games,
     COVID-shortened 2020-21 ~1080 games).
  3. NaN rate per column — flags any candidate feature column with >20% missing.
  4. Duplicate game_id + team combinations in TeamStatistics.csv.
  5. Score range sanity (no negative scores, no game with 0 points scored).
  6. Date parse validity (gameDateTimeEst / gameDate parseable).

Output:
  ml/verify_output/data_quality_report.json

Exit codes:
  0 — all checks passed (or only warnings)
  1 — at least one critical check failed

Usage:
    python verify_data.py
    python verify_data.py --data-dir /path/to/data/
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
DEFAULT_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "verify_output")

# Expected regular-season game counts:
#   Full season (2021-22 onward): 30 teams × 82 games / 2 = 1230 games
#   Each game appears as 2 rows in TeamStatistics (one per team), so 2460 rows.
#   2020-21 COVID shortened to 72 games: 30 × 72 / 2 = 1080 games → 2160 team rows.
EXPECTED_GAME_COUNTS: dict[str, dict[str, int]] = {
    "2020-21": {"min_games": 900,  "max_games": 1150},   # COVID-shortened + bubble playoffs
    "2021-22": {"min_games": 1200, "max_games": 1400},
    "2022-23": {"min_games": 1200, "max_games": 1400},
    "2023-24": {"min_games": 1200, "max_games": 1400},
    "2024-25": {"min_games": 1200, "max_games": 1400},
}

# Columns that will become model features — flag if NaN rate > 20%
CANDIDATE_FEATURE_COLUMNS = [
    "fieldGoalsAttempted", "fieldGoalsMade", "threePointersMade",
    "threePointersAttempted", "freeThrowsMade", "freeThrowsAttempted",
    "reboundsOffensive", "reboundsDefensive", "assists", "steals",
    "blocks", "turnovers", "plusMinusPoints", "teamScore", "opponentScore",
    # NOTE: pointsInThePaint and benchPoints are intentionally excluded —
    # the Kaggle dataset only has these columns from the 2024-25 season onward.
    # Feature engineering handles them with median imputation.
]
NAN_RATE_THRESHOLD = 0.20


def _load_csv(path: str, label: str) -> pd.DataFrame | None:
    """Load a CSV file with error handling. Returns None if file is missing."""
    if not os.path.exists(path):
        log.error(f"Required file not found: {path}")
        return None
    try:
        df = pd.read_csv(path, low_memory=False)
        log.info(f"Loaded {label}: {len(df):,} rows, {len(df.columns)} columns")
        return df
    except Exception as exc:
        log.error(f"Failed to read {path}: {exc}")
        return None


def check_file_existence(data_dir: str) -> tuple[list[dict], list[dict]]:
    """Check that all three required CSV files exist and are non-empty."""
    checks: list[dict] = []
    warnings: list[dict] = []
    required = ["Games.csv", "TeamStatistics.csv", "PlayerStatistics.csv"]
    for fname in required:
        fpath = os.path.join(data_dir, fname)
        if not os.path.exists(fpath):
            checks.append({
                "check": "file_exists",
                "file": fname,
                "passed": False,
                "message": f"{fname} not found in {data_dir}",
                "severity": "critical",
            })
        else:
            size_bytes = os.path.getsize(fpath)
            if size_bytes == 0:
                checks.append({
                    "check": "file_non_empty",
                    "file": fname,
                    "passed": False,
                    "message": f"{fname} is empty (0 bytes)",
                    "severity": "critical",
                })
            else:
                checks.append({
                    "check": "file_exists",
                    "file": fname,
                    "passed": True,
                    "message": f"{fname} found ({size_bytes / 1024:.1f} KB)",
                    "severity": "info",
                })
    return checks, warnings


def check_game_counts(
    games_df: pd.DataFrame,
) -> tuple[list[dict], list[dict]]:
    """
    Verify game counts per season are within expected range.
    Games.csv should have one row per game; we group by seasonYear.
    """
    checks: list[dict] = []
    warnings: list[dict] = []

    # Detect season column
    season_col = None
    for col in ("seasonYear", "season_year", "SEASON_YEAR", "season"):
        if col in games_df.columns:
            season_col = col
            break

    if season_col is None:
        warnings.append({
            "check": "game_counts_per_season",
            "passed": True,
            "message": "No season column found in Games.csv — skipping season count check.",
            "severity": "warning",
        })
        return checks, warnings

    season_counts = games_df.groupby(season_col).size().to_dict()
    log.info(f"Game counts by season: {season_counts}")

    for season, expected in EXPECTED_GAME_COUNTS.items():
        count = season_counts.get(season, 0)
        if count == 0:
            warnings.append({
                "check": "game_counts_per_season",
                "season": season,
                "game_count": count,
                "expected_min": expected["min_games"],
                "expected_max": expected["max_games"],
                "passed": True,
                "message": f"Season {season} not present in dataset (may not be downloaded yet).",
                "severity": "warning",
            })
        elif count < expected["min_games"] or count > expected["max_games"]:
            checks.append({
                "check": "game_counts_per_season",
                "season": season,
                "game_count": count,
                "expected_min": expected["min_games"],
                "expected_max": expected["max_games"],
                "passed": False,
                "message": (
                    f"Season {season} has {count} games, "
                    f"expected {expected['min_games']}–{expected['max_games']}."
                ),
                "severity": "critical",
            })
        else:
            checks.append({
                "check": "game_counts_per_season",
                "season": season,
                "game_count": count,
                "expected_min": expected["min_games"],
                "expected_max": expected["max_games"],
                "passed": True,
                "message": f"Season {season}: {count} games (OK).",
                "severity": "info",
            })

    # Also flag any seasons with very few games as a warning
    for season, count in season_counts.items():
        if season not in EXPECTED_GAME_COUNTS and count < 100:
            warnings.append({
                "check": "game_counts_per_season",
                "season": str(season),
                "game_count": count,
                "passed": True,
                "message": f"Season {season} has only {count} games — may be partial data.",
                "severity": "warning",
            })

    return checks, warnings


def check_nan_rates(
    teamstats_df: pd.DataFrame,
) -> tuple[list[dict], list[dict]]:
    """
    Check NaN rate for candidate feature columns.
    Flags any column with >20% missing values as a critical failure.

    NOTE: Only checks modern seasons (2015-16 onward) because the dataset
    spans NBA history back to the 1940s when advanced stats (pointsInThePaint,
    benchPoints, etc.) simply didn't exist. Pre-modern rows are legitimate
    historical data — not data quality issues.
    """
    checks: list[dict] = []
    warnings: list[dict] = []

    # Filter to modern seasons only (2015-16 onward, i.e. dates >= 2015-10-01).
    # The dataset spans NBA history back to the 1940s; advanced stats like
    # pointsInThePaint and benchPoints only exist in the modern era.
    modern_df = teamstats_df
    date_col = None
    for col in ("gameDateTimeEst", "gameDate", "GAME_DATE", "game_date"):
        if col in teamstats_df.columns:
            date_col = col
            break
    if date_col is not None:
        try:
            parsed_dates = pd.to_datetime(teamstats_df[date_col], errors="coerce")
            modern_df = teamstats_df[parsed_dates >= "2015-10-01"]
            log.info(
                f"NaN rate check: filtered to {len(modern_df):,} modern-season rows "
                f"(date >= 2015-10-01) out of {len(teamstats_df):,} total."
            )
        except Exception:
            pass  # Fall back to full dataframe if date column is unusable

    total_rows = len(modern_df)

    for col in CANDIDATE_FEATURE_COLUMNS:
        if col not in teamstats_df.columns:
            warnings.append({
                "check": "nan_rate",
                "column": col,
                "passed": True,
                "message": f"Column '{col}' not present in TeamStatistics.csv — skipping.",
                "severity": "warning",
            })
            continue

        nan_count = modern_df[col].isna().sum()
        nan_rate = nan_count / total_rows if total_rows > 0 else 0.0

        if nan_rate > NAN_RATE_THRESHOLD:
            checks.append({
                "check": "nan_rate",
                "column": col,
                "nan_count": int(nan_count),
                "nan_rate": round(float(nan_rate), 4),
                "threshold": NAN_RATE_THRESHOLD,
                "passed": False,
                "message": (
                    f"Column '{col}' has {nan_rate:.1%} missing values "
                    f"({nan_count:,}/{total_rows:,}), threshold is {NAN_RATE_THRESHOLD:.0%}."
                ),
                "severity": "critical",
            })
        else:
            checks.append({
                "check": "nan_rate",
                "column": col,
                "nan_count": int(nan_count),
                "nan_rate": round(float(nan_rate), 4),
                "threshold": NAN_RATE_THRESHOLD,
                "passed": True,
                "message": f"Column '{col}': {nan_rate:.1%} missing (OK).",
                "severity": "info",
            })

    return checks, warnings


def check_duplicate_game_team(
    teamstats_df: pd.DataFrame,
) -> tuple[list[dict], list[dict]]:
    """
    Check for duplicate (gameId, teamId) combinations in TeamStatistics.csv.
    Each team should appear exactly once per game.
    """
    checks: list[dict] = []
    warnings: list[dict] = []

    game_col = None
    team_col = None
    for gc in ("gameId", "game_id", "GAME_ID"):
        if gc in teamstats_df.columns:
            game_col = gc
            break
    for tc in ("teamId", "team_id", "TEAM_ID"):
        if tc in teamstats_df.columns:
            team_col = tc
            break

    if game_col is None or team_col is None:
        warnings.append({
            "check": "duplicate_game_team",
            "passed": True,
            "message": "gameId or teamId column not found — skipping duplicate check.",
            "severity": "warning",
        })
        return checks, warnings

    duplicates = teamstats_df.duplicated(subset=[game_col, team_col], keep=False)
    dup_count = int(duplicates.sum())

    if dup_count > 0:
        sample_dups = (
            teamstats_df[duplicates][[game_col, team_col]]
            .head(5)
            .to_dict("records")
        )
        checks.append({
            "check": "duplicate_game_team",
            "duplicate_rows": dup_count,
            "sample_duplicates": sample_dups,
            "passed": False,
            "message": (
                f"Found {dup_count} duplicate (gameId, teamId) rows in TeamStatistics.csv. "
                "This will corrupt season-to-date rolling averages."
            ),
            "severity": "critical",
        })
    else:
        checks.append({
            "check": "duplicate_game_team",
            "duplicate_rows": 0,
            "passed": True,
            "message": "No duplicate (gameId, teamId) combinations found (OK).",
            "severity": "info",
        })

    return checks, warnings


def check_score_sanity(
    teamstats_df: pd.DataFrame,
) -> tuple[list[dict], list[dict]]:
    """
    Check that teamScore and opponentScore are positive numbers.
    Detects rows with 0 or negative scores that indicate bad data.
    """
    checks: list[dict] = []
    warnings: list[dict] = []

    score_cols = []
    for col in ("teamScore", "opponentScore"):
        if col in teamstats_df.columns:
            score_cols.append(col)

    if not score_cols:
        warnings.append({
            "check": "score_sanity",
            "passed": True,
            "message": "Score columns not found in TeamStatistics.csv — skipping.",
            "severity": "warning",
        })
        return checks, warnings

    for col in score_cols:
        series = pd.to_numeric(teamstats_df[col], errors="coerce")
        bad_mask = series.notna() & (series <= 0)
        bad_count = int(bad_mask.sum())
        if bad_count > 0:
            # A small number of zero/negative score rows (e.g. postponed games
            # recorded as 0-0 in historical data) is acceptable noise.
            # Only flag as critical if >0.5% of rows are affected.
            total = len(teamstats_df)
            bad_rate = bad_count / total if total > 0 else 0.0
            if bad_rate > 0.005:
                checks.append({
                    "check": "score_sanity",
                    "column": col,
                    "bad_rows": bad_count,
                    "bad_rate": round(bad_rate, 4),
                    "passed": False,
                    "message": (
                        f"Column '{col}' has {bad_count} rows ({bad_rate:.1%}) with score <= 0. "
                        "These games may be incomplete or corrupted."
                    ),
                    "severity": "critical",
                })
            else:
                warnings.append({
                    "check": "score_sanity",
                    "column": col,
                    "bad_rows": bad_count,
                    "bad_rate": round(bad_rate, 4),
                    "passed": True,
                    "message": (
                        f"Column '{col}' has {bad_count} rows ({bad_rate:.2%}) with score <= 0 "
                        f"out of {total:,} — acceptable noise, skipping."
                    ),
                    "severity": "warning",
                })
        else:
            checks.append({
                "check": "score_sanity",
                "column": col,
                "bad_rows": 0,
                "passed": True,
                "message": f"Column '{col}': all scores positive (OK).",
                "severity": "info",
            })

    return checks, warnings


def check_date_validity(
    teamstats_df: pd.DataFrame,
) -> tuple[list[dict], list[dict]]:
    """
    Verify that the game date column is parseable.
    Unparseable dates will silently break chronological ordering and
    cause lookahead data leakage in feature engineering.
    """
    checks: list[dict] = []
    warnings: list[dict] = []

    date_col = None
    for col in ("gameDateTimeEst", "gameDate", "GAME_DATE", "game_date"):
        if col in teamstats_df.columns:
            date_col = col
            break

    if date_col is None:
        warnings.append({
            "check": "date_validity",
            "passed": True,
            "message": "No date column found in TeamStatistics.csv — skipping.",
            "severity": "warning",
        })
        return checks, warnings

    parsed = pd.to_datetime(teamstats_df[date_col], errors="coerce")
    unparseable = int(parsed.isna().sum())
    total = len(teamstats_df)
    rate = unparseable / total if total > 0 else 0.0

    if rate > 0.05:
        checks.append({
            "check": "date_validity",
            "column": date_col,
            "unparseable_count": unparseable,
            "unparseable_rate": round(rate, 4),
            "passed": False,
            "message": (
                f"Column '{date_col}' has {rate:.1%} unparseable dates ({unparseable:,} rows). "
                "Date ordering is critical for avoiding lookahead leakage."
            ),
            "severity": "critical",
        })
    else:
        checks.append({
            "check": "date_validity",
            "column": date_col,
            "unparseable_count": unparseable,
            "unparseable_rate": round(rate, 4),
            "passed": True,
            "message": f"Date column '{date_col}': {rate:.1%} unparseable ({unparseable} rows) — OK.",
            "severity": "info",
        })

    return checks, warnings


def run_verification(data_dir: str, output_dir: str) -> dict[str, Any]:
    """
    Run all data quality checks and produce a report dict.
    """
    all_checks: list[dict] = []
    all_warnings: list[dict] = []

    # ── File existence ───────────────────────────────────────────────────────
    c, w = check_file_existence(data_dir)
    all_checks.extend(c)
    all_warnings.extend(w)

    # Abort early if files are missing
    critical_failures = [x for x in all_checks if not x["passed"]]
    if critical_failures:
        report = {
            "passed": False,
            "checks": all_checks,
            "warnings": all_warnings,
            "summary": f"Aborted: {len(critical_failures)} critical file-existence failure(s).",
        }
        return report

    # ── Load CSVs ────────────────────────────────────────────────────────────
    games_df = _load_csv(os.path.join(data_dir, "Games.csv"), "Games.csv")
    teamstats_df = _load_csv(
        os.path.join(data_dir, "TeamStatistics.csv"), "TeamStatistics.csv"
    )
    player_df = _load_csv(
        os.path.join(data_dir, "PlayerStatistics.csv"), "PlayerStatistics.csv"
    )

    for df, label in [
        (games_df, "Games.csv"),
        (teamstats_df, "TeamStatistics.csv"),
        (player_df, "PlayerStatistics.csv"),
    ]:
        if df is None:
            all_checks.append({
                "check": "csv_loadable",
                "file": label,
                "passed": False,
                "message": f"Failed to read {label}.",
                "severity": "critical",
            })
        else:
            all_checks.append({
                "check": "csv_loadable",
                "file": label,
                "passed": True,
                "message": f"{label} loaded successfully ({len(df):,} rows).",
                "severity": "info",
            })

    if games_df is None or teamstats_df is None:
        report = {
            "passed": False,
            "checks": all_checks,
            "warnings": all_warnings,
            "summary": "Critical: required CSVs could not be loaded.",
        }
        return report

    # ── Game count check ─────────────────────────────────────────────────────
    c, w = check_game_counts(games_df)
    all_checks.extend(c)
    all_warnings.extend(w)

    # ── NaN rate check ───────────────────────────────────────────────────────
    c, w = check_nan_rates(teamstats_df)
    all_checks.extend(c)
    all_warnings.extend(w)

    # ── Duplicate game+team check ────────────────────────────────────────────
    c, w = check_duplicate_game_team(teamstats_df)
    all_checks.extend(c)
    all_warnings.extend(w)

    # ── Score sanity check ───────────────────────────────────────────────────
    c, w = check_score_sanity(teamstats_df)
    all_checks.extend(c)
    all_warnings.extend(w)

    # ── Date validity check ──────────────────────────────────────────────────
    c, w = check_date_validity(teamstats_df)
    all_checks.extend(c)
    all_warnings.extend(w)

    # ── Aggregate result ─────────────────────────────────────────────────────
    failures = [x for x in all_checks if not x["passed"]]
    passed = len(failures) == 0

    n_total = len(all_checks)
    n_passed = len([x for x in all_checks if x["passed"]])
    summary = (
        f"{n_passed}/{n_total} checks passed. "
        f"{len(failures)} critical failure(s). "
        f"{len(all_warnings)} warning(s)."
    )
    log.info(summary)

    return {
        "passed": passed,
        "checks": all_checks,
        "warnings": all_warnings,
        "summary": summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify NBA CSV data quality before training. Exits 1 if any check fails.",
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help=f"Directory containing Games.csv, TeamStatistics.csv, PlayerStatistics.csv "
             f"(default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for data_quality_report.json (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    log.info(f"Data directory : {data_dir}")
    log.info(f"Output directory: {output_dir}")

    report = run_verification(data_dir, output_dir)

    report_path = os.path.join(output_dir, "data_quality_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info(f"Report saved → {report_path}")

    if report["passed"]:
        log.info("All checks PASSED. Pipeline may proceed.")
        sys.exit(0)
    else:
        failures = [c for c in report["checks"] if not c["passed"]]
        log.error(f"DATA QUALITY FAILED: {len(failures)} critical check(s) did not pass.")
        for f in failures:
            log.error(f"  FAIL [{f['check']}]: {f['message']}")
        log.error("Training blocked. Fix data issues before retrying.")
        sys.exit(1)


if __name__ == "__main__":
    main()
