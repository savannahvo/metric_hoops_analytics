"""
odds_loader.py
--------------
Load and merge odds data from two sources:
  - Historical 2020-25: pre-built CSV
  - 2025-26: OddsData.sqlite (after backfill runs)

Key functions:
  load_odds(hist_csv_path, sqlite_db_path) -> pd.DataFrame
  merge_odds(training_df, odds_df, impute_medians) -> pd.DataFrame
  compute_training_medians(df) -> dict
"""

import logging
import sqlite3

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# Maps team names used in odds sources to Kaggle integer team IDs
TEAM_NAME_TO_ID: dict[str, int] = {
    "Atlanta Hawks": 1610612737,
    "Boston Celtics": 1610612738,
    "Cleveland Cavaliers": 1610612739,
    "New Orleans Pelicans": 1610612740,
    "New Orleans Hornets": 1610612740,
    "Chicago Bulls": 1610612741,
    "Dallas Mavericks": 1610612742,
    "Denver Nuggets": 1610612743,
    "Golden State Warriors": 1610612744,
    "Houston Rockets": 1610612745,
    "LA Clippers": 1610612746,
    "Los Angeles Clippers": 1610612746,
    "Los Angeles Lakers": 1610612747,
    "Miami Heat": 1610612748,
    "Milwaukee Bucks": 1610612749,
    "Minnesota Timberwolves": 1610612750,
    "Brooklyn Nets": 1610612751,
    "New Jersey Nets": 1610612751,
    "New York Knicks": 1610612752,
    "Orlando Magic": 1610612753,
    "Indiana Pacers": 1610612754,
    "Philadelphia 76ers": 1610612755,
    "Phoenix Suns": 1610612756,
    "Portland Trail Blazers": 1610612757,
    "Sacramento Kings": 1610612758,
    "San Antonio Spurs": 1610612759,
    "Oklahoma City Thunder": 1610612760,
    "Toronto Raptors": 1610612761,
    "Utah Jazz": 1610612762,
    "Memphis Grizzlies": 1610612763,
    "Washington Wizards": 1610612764,
    "Detroit Pistons": 1610612765,
    "Charlotte Hornets": 1610612766,
    "Charlotte Bobcats": 1610612766,
}

ODDS_COLS = ["SPREAD_DIFF", "ML_PROB_DIFF", "OVER_UNDER"]


def _ml_to_prob(ml_home: float, ml_away: float) -> float:
    """Convert moneyline pair to vig-removed home win probability."""
    def raw_prob(line: float) -> float:
        if line < 0:
            return abs(line) / (abs(line) + 100)
        else:
            return 100 / (line + 100)

    p_home = raw_prob(ml_home)
    p_away = raw_prob(ml_away)
    total = p_home + p_away
    if total == 0:
        return 0.5
    return p_home / total


def _map_team(name: str) -> int:
    """Map a team name string to its integer ID. Raises ValueError on miss."""
    tid = TEAM_NAME_TO_ID.get(name)
    if tid is None:
        raise ValueError(
            f"Unrecognised team name: '{name}'. Add it to TEAM_NAME_TO_ID in odds_loader.py."
        )
    return tid


def _load_historical_csv(path: str) -> pd.DataFrame:
    """Load historical odds CSV (2020-21 through 2024-25)."""
    df = pd.read_csv(path, parse_dates=["GAME_DATE"])
    df["GAME_DATE"] = df["GAME_DATE"].dt.strftime("%Y-%m-%d")

    # Map team names to IDs
    df["HOME_TEAM_ID"] = df["HOME_TEAM"].apply(_map_team)
    df["AWAY_TEAM_ID"] = df["AWAY_TEAM"].apply(_map_team)

    # Vig-removed moneyline probability (centered at 0 to match scheduler.py line 334)
    df["ML_PROB_DIFF"] = df.apply(
        lambda r: _ml_to_prob(r["ML_HOME"], r["ML_AWAY"]) - 0.5, axis=1
    )

    # SPREAD_DIFF: use as-is (positive = home favoured)
    df = df.rename(columns={"SPREAD": "SPREAD_DIFF", "OU": "OVER_UNDER"})

    # Handle missing OU column (historical CSV doesn't have it)
    if "OVER_UNDER" not in df.columns:
        df["OVER_UNDER"] = np.nan

    return df[["GAME_DATE", "HOME_TEAM_ID", "AWAY_TEAM_ID", "SPREAD_DIFF", "ML_PROB_DIFF", "OVER_UNDER"]]


def _load_sqlite_2025_26(db_path: str) -> pd.DataFrame:
    """Load 2025-26 odds from OddsData.sqlite → odds_2025-26 table."""
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql('SELECT * FROM "odds_2025-26"', conn)
    finally:
        conn.close()

    df = df.rename(columns={"Date": "GAME_DATE"})
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"]).dt.strftime("%Y-%m-%d")

    df["HOME_TEAM_ID"] = df["Home"].apply(_map_team)
    df["AWAY_TEAM_ID"] = df["Away"].apply(_map_team)

    df["ML_PROB_DIFF"] = df.apply(
        lambda r: _ml_to_prob(r["ML_Home"], r["ML_Away"]) - 0.5, axis=1
    )
    df = df.rename(columns={"Spread": "SPREAD_DIFF", "OU": "OVER_UNDER"})

    return df[["GAME_DATE", "HOME_TEAM_ID", "AWAY_TEAM_ID", "SPREAD_DIFF", "ML_PROB_DIFF", "OVER_UNDER"]]


def load_odds(hist_csv_path: str, sqlite_db_path: str) -> pd.DataFrame:
    """
    Load odds from both sources and union them.

    Returns a DataFrame with columns:
        GAME_DATE, HOME_TEAM_ID, AWAY_TEAM_ID, SPREAD_DIFF, ML_PROB_DIFF, OVER_UNDER
    """
    frames = []

    try:
        hist_df = _load_historical_csv(hist_csv_path)
        log.info("Historical odds loaded: %d rows", len(hist_df))
        frames.append(hist_df)
    except Exception as exc:
        log.warning("Could not load historical odds CSV (%s): %s", hist_csv_path, exc)

    try:
        live_df = _load_sqlite_2025_26(sqlite_db_path)
        log.info("2025-26 odds loaded: %d rows", len(live_df))
        frames.append(live_df)
    except Exception as exc:
        log.warning("Could not load 2025-26 odds from SQLite (%s): %s", sqlite_db_path, exc)

    if not frames:
        raise RuntimeError("No odds data loaded from either source.")

    odds_df = pd.concat(frames, ignore_index=True)
    # Deduplicate: keep first occurrence (historical preferred over SQLite for overlap dates)
    odds_df = odds_df.drop_duplicates(subset=["GAME_DATE", "HOME_TEAM_ID", "AWAY_TEAM_ID"], keep="first")
    log.info("Total odds rows after union+dedup: %d", len(odds_df))
    return odds_df


def compute_training_medians(df: pd.DataFrame) -> dict:
    """
    Compute median of odds columns from 2020-23 training rows ONLY.
    Call this before any imputation so the medians are uncontaminated.
    """
    train_mask = df["SEASON"].isin(["2020-21", "2021-22", "2022-23"])
    train_df = df[train_mask]
    medians = {}
    for col in ODDS_COLS:
        if col in train_df.columns:
            val = train_df[col].dropna().median()
            medians[col] = float(val) if not np.isnan(val) else 0.0
        else:
            medians[col] = 0.0
    log.info("Training medians (2020-23): %s", medians)
    return medians


def merge_odds(
    training_df: pd.DataFrame,
    odds_df: pd.DataFrame,
    impute_medians: dict,
) -> pd.DataFrame:
    """
    Left-join odds onto training_df by (GAME_DATE, HOME_TEAM_ID, AWAY_TEAM_ID).
    NaN odds rows are imputed from impute_medians.

    Returns an enriched copy of training_df with SPREAD_DIFF, ML_PROB_DIFF, OVER_UNDER
    columns overwritten (or added) from real odds wherever available.
    """
    df = training_df.copy()

    # Drop stale placeholder odds columns before merge
    for col in ODDS_COLS:
        if col in df.columns:
            df = df.drop(columns=[col])

    merged = df.merge(
        odds_df[["GAME_DATE", "HOME_TEAM_ID", "AWAY_TEAM_ID"] + ODDS_COLS],
        on=["GAME_DATE", "HOME_TEAM_ID", "AWAY_TEAM_ID"],
        how="left",
    )

    matched = merged[ODDS_COLS[0]].notna().sum()
    total = len(merged)
    match_rate = matched / total if total > 0 else 0.0
    log.info("Odds match rate: %d/%d (%.1f%%)", matched, total, match_rate * 100)

    # Impute NaNs from training medians
    for col in ODDS_COLS:
        n_nan = merged[col].isna().sum()
        if n_nan > 0:
            merged[col] = merged[col].fillna(impute_medians.get(col, 0.0))
            log.info("Imputed %d NaN values in %s", n_nan, col)

    return merged, match_rate
