"""
feature_engineering.py
-----------------------
Builds training_data.csv from raw Kaggle CSVs.

All features are home-minus-away differentials.  Season-to-date stats are
computed strictly from games BEFORE the current game date to prevent lookahead
data leakage.

Kaggle dataset: szymonjwiak/nba-traditional
Required files (in --data-dir):
    Games.csv
    TeamStatistics.csv
    PlayerStatistics.csv

Output:
    training_data.csv  (one row per game, home-team perspective)

Usage:
    python feature_engineering.py
    python feature_engineering.py --data-dir /path/to/data/ --output-dir /path/to/out/
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import numpy as np
import pandas as pd

from feature_schema import FEATURES, FEATURES_NO_ODDS

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
DEFAULT_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "data")

# ELO parameters
ELO_K = 20
ELO_DEFAULT = 1500.0

# Season range to process
SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]

# League-average home score (used for score imputation at inference time)
AVG_HOME_PTS = 113.5


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_div(num: float, denom: float, default: float = 0.0) -> float:
    """Safe division returning default when denominator is zero."""
    if denom == 0 or np.isnan(denom):
        return default
    return float(num) / float(denom)


def _elo_expected(elo_home: float, elo_away: float) -> float:
    """Expected win probability for home team given Elo ratings."""
    return 1.0 / (1.0 + 10 ** ((elo_away - elo_home) / 400.0))


def _elo_update(elo: float, actual: float, expected: float, k: float = ELO_K) -> float:
    """Return updated Elo rating after one game."""
    return elo + k * (actual - expected)


def _normalize_date_col(df: pd.DataFrame) -> pd.DataFrame:
    """Parse gameDateTimeEst (eoinamoore dataset) into a tz-naive GAME_DATE column."""
    if "gameDateTimeEst" not in df.columns:
        raise KeyError("gameDateTimeEst column not found — is this the eoinamoore dataset?")
    df["GAME_DATE"] = pd.to_datetime(df["gameDateTimeEst"], errors="coerce", utc=True)
    df["GAME_DATE"] = df["GAME_DATE"].dt.tz_localize(None)
    return df


def _is_playoff(game_type: str) -> bool:
    """Return True if the game type string indicates a playoff game."""
    if not isinstance(game_type, str):
        return False
    gt = game_type.lower()
    return "playoff" in gt or "play-in" in gt or "play in" in gt


# ── Load and normalise raw data ───────────────────────────────────────────────

def load_games(data_dir: str) -> pd.DataFrame:
    """Load and normalise Games.csv."""
    path = os.path.join(data_dir, "Games.csv")
    df = pd.read_csv(path, low_memory=False)
    df = _normalize_date_col(df)
    log.info(f"Games.csv loaded: {len(df):,} rows")

    # eoinamoore column names → internal names
    df.rename(columns={
        "gameId":       "GAME_ID",
        "hometeamId":   "HOME_TEAM_ID",
        "awayteamId":   "AWAY_TEAM_ID",
        "hometeamName": "HOME_TEAM_NAME",
        "awayteamName": "AWAY_TEAM_NAME",
        "homeScore":    "HOME_SCORE",
        "awayScore":    "AWAY_SCORE",
        "gameType":     "GAME_TYPE",
    }, inplace=True)

    # Derive SEASON from GAME_DATE (eoinamoore has no season column)
    def _date_to_season(dt):
        if pd.isna(dt):
            return None
        if dt.month >= 10:
            return f"{dt.year}-{str(dt.year + 1)[2:]}"
        return f"{dt.year - 1}-{str(dt.year)[2:]}"
    df["SEASON"] = df["GAME_DATE"].apply(_date_to_season)

    return df


def load_team_stats(data_dir: str) -> pd.DataFrame:
    """Load and normalise TeamStatistics.csv."""
    path = os.path.join(data_dir, "TeamStatistics.csv")
    df = pd.read_csv(path, low_memory=False)
    df = _normalize_date_col(df)
    log.info(f"TeamStatistics.csv loaded: {len(df):,} rows")

    # eoinamoore column names → internal names
    df.rename(columns={
        "gameId":               "GAME_ID",
        "teamId":               "TEAM_ID",
        "teamName":             "TEAM_NAME",
        "home":                 "IS_HOME",
        "win":                  "WIN",
        "teamScore":            "PTS",
        "opponentScore":        "OPP_PTS",
        "fieldGoalsAttempted":  "FGA",
        "fieldGoalsMade":       "FGM",
        "threePointersMade":    "FG3M",
        "threePointersAttempted": "FG3A",
        "freeThrowsMade":       "FTM",
        "freeThrowsAttempted":  "FTA",
        "reboundsOffensive":    "OREB",
        "reboundsDefensive":    "DREB",
        "assists":              "AST",
        "steals":               "STL",
        "blocks":               "BLK",
        "turnovers":            "TOV",
        "plusMinusPoints":      "PLUS_MINUS",
    }, inplace=True)

    # Boolean normalisation
    for col in ("WIN", "IS_HOME"):
        if col in df.columns:
            df[col] = df[col].map(
                lambda x: True if str(x).lower() in ("true", "1", "yes") else False
            )

    return df


def load_player_stats(data_dir: str) -> pd.DataFrame:
    """Load and normalise PlayerStatistics.csv."""
    path = os.path.join(data_dir, "PlayerStatistics.csv")
    df = pd.read_csv(path, low_memory=False)
    log.info(f"PlayerStatistics.csv loaded: {len(df):,} rows")

    # eoinamoore column names → internal names
    # Note: PlayerStatistics.csv has no teamId column
    df.rename(columns={
        "gameId":          "GAME_ID",
        "personId":        "PLAYER_ID",
        "points":          "PTS",
        "assists":         "AST",
        "reboundsTotal":   "REB",
        "plusMinusPoints": "PLUS_MINUS",
        "numMinutes":      "MIN",
    }, inplace=True)

    return df


# ── Per-team season-to-date profiles ─────────────────────────────────────────

def compute_team_profile(games_before: pd.DataFrame) -> dict:
    """
    Compute season-to-date aggregate stats from a team's game log up to (but
    not including) the current game.  All stats are per-game averages.
    """
    n = len(games_before)
    if n == 0:
        return {}

    def avg(col: str, default: float = 0.0) -> float:
        if col not in games_before.columns:
            return default
        vals = pd.to_numeric(games_before[col], errors="coerce").dropna()
        return float(vals.mean()) if len(vals) > 0 else default

    fga = avg("FGA", 85.0)
    fgm = avg("FGM", 40.0)
    fg3m = avg("FG3M", 12.0)
    fg3a = avg("FG3A", 30.0)
    fta = avg("FTA", 20.0)
    ftm = avg("FTM", 16.0)
    oreb = avg("OREB", 10.0)
    dreb = avg("DREB", 30.0)
    tov = avg("TOV", 13.0)
    pts = avg("PTS", 110.0)
    opp_pts = avg("OPP_PTS", 110.0)
    bench_pts = avg("BENCH_PTS", 35.0)
    paint_pts = avg("PAINT_PTS", 45.0)

    # Four Factors
    poss = max(fga + 0.44 * fta - oreb + tov, 1.0)
    off_rtg = (pts / poss) * 100.0
    def_rtg = (opp_pts / poss) * 100.0
    efg_pct = _safe_div(fgm + 0.5 * fg3m, fga)
    ts_pct = _safe_div(pts, 2.0 * (fga + 0.44 * fta))
    oreb_pct = _safe_div(oreb, oreb + dreb)
    dreb_pct = _safe_div(dreb, oreb + dreb)
    tov_rate = _safe_div(tov, poss) * 100.0
    ft_rate = _safe_div(fta, fga)
    fg3_rate = _safe_div(fg3a, fga)

    # Win %
    if "WIN" in games_before.columns:
        win_col = pd.to_numeric(games_before["WIN"].map({True: 1, False: 0, 1: 1, 0: 0}),
                                errors="coerce").fillna(0)
        win_pct = float(win_col.mean())
    else:
        win_pct = 0.5

    # Plus-minus per game
    pm = avg("PLUS_MINUS", 0.0)

    return {
        "GAMES_PLAYED": n,
        "PTS": pts,
        "OPP_PTS": opp_pts,
        "OFF_RTG": off_rtg,
        "DEF_RTG": def_rtg,
        "NET_RTG": off_rtg - def_rtg,
        "EFG_PCT": efg_pct,
        "TS_PCT": ts_pct,
        "OREB_PCT": oreb_pct,
        "DREB_PCT": dreb_pct,
        "TOV_RATE": tov_rate,
        "FT_RATE": ft_rate,
        "FG3_RATE": fg3_rate,
        "BENCH_PTS": bench_pts,
        "PAINT_PTS": paint_pts,
        "WIN_PCT": win_pct,
        "PLUS_MINUS": pm,
        "FGA": fga,
        "FTA": fta,
        "OREB": oreb,
        "TOV": tov,
        "POSS": poss,
    }


def compute_rolling_profile(games_before: pd.DataFrame, window: int = 10) -> dict:
    """
    Compute rolling last-N game stats from a team's recent game log.
    Returns dict with ROLL10_* keys.
    """
    recent = games_before.tail(window)
    if len(recent) == 0:
        return {
            "ROLL10_WIN_PCT": 0.5,
            "ROLL10_NET_RTG": 0.0,
            "ROLL10_PTS": 110.0,
            "ROLL10_EFG": 0.50,
            "ROLL10_TOV": 13.0,
        }

    def avg(col: str, default: float = 0.0) -> float:
        if col not in recent.columns:
            return default
        vals = pd.to_numeric(recent[col], errors="coerce").dropna()
        return float(vals.mean()) if len(vals) > 0 else default

    if "WIN" in recent.columns:
        win_col = pd.to_numeric(recent["WIN"].map({True: 1, False: 0, 1: 1, 0: 0}),
                                errors="coerce").fillna(0)
        roll_win_pct = float(win_col.mean())
    else:
        roll_win_pct = 0.5

    fga = avg("FGA", 85.0)
    fgm = avg("FGM", 40.0)
    fg3m = avg("FG3M", 12.0)
    fta = avg("FTA", 20.0)
    oreb = avg("OREB", 10.0)
    tov = avg("TOV", 13.0)
    pts = avg("PTS", 110.0)
    opp_pts = avg("OPP_PTS", 110.0)

    poss = max(fga + 0.44 * fta - oreb + tov, 1.0)
    off_rtg = (pts / poss) * 100.0
    def_rtg = (opp_pts / poss) * 100.0
    net_rtg = off_rtg - def_rtg
    efg = _safe_div(fgm + 0.5 * fg3m, fga)

    return {
        "ROLL10_WIN_PCT": roll_win_pct,
        "ROLL10_NET_RTG": net_rtg,
        "ROLL10_PTS": pts,
        "ROLL10_EFG": efg,
        "ROLL10_TOV": tov,
    }


def compute_schedule_features(
    game_log: pd.DataFrame,
    game_date: pd.Timestamp,
) -> dict:
    """
    Compute schedule/fatigue features for a team at a given game_date.
    Uses game_log of all games BEFORE game_date.
    """
    if game_log.empty or "GAME_DATE" not in game_log.columns:
        return {"DAYS_REST": 3.0, "IS_B2B": 0, "GAMES_LAST_7": 0}

    past = game_log[game_log["GAME_DATE"] < game_date].sort_values("GAME_DATE")

    days_rest = 3.0  # default: well-rested
    is_b2b = 0
    games_last_7 = 0

    if len(past) > 0:
        last_game_date = past["GAME_DATE"].iloc[-1]
        delta = (game_date - last_game_date).days
        days_rest = float(min(delta, 7))
        is_b2b = 1 if delta <= 1 else 0

    cutoff_7 = game_date - pd.Timedelta(days=7)
    games_last_7 = int(((past["GAME_DATE"] >= cutoff_7)).sum())

    return {
        "DAYS_REST": days_rest,
        "IS_B2B": is_b2b,
        "GAMES_LAST_7": games_last_7,
    }


def compute_streak(game_log: pd.DataFrame) -> int:
    """
    Compute current win/loss streak at the time of the current game.
    Positive = win streak, negative = losing streak.
    """
    if len(game_log) == 0:
        return 0

    sorted_log = game_log.sort_values("GAME_DATE", ascending=False)
    if "WIN" not in sorted_log.columns:
        return 0

    wins = sorted_log["WIN"].map({True: 1, False: 0, 1: 1, 0: 0}).fillna(0).astype(int)
    streak = 0
    first = int(wins.iloc[0])

    for w in wins:
        if int(w) == first:
            streak += (1 if first == 1 else -1)
        else:
            break

    return streak


def compute_player_features(
    player_df: pd.DataFrame,
    team_id,
    games_before: pd.DataFrame,
) -> dict:
    """
    Compute player-level features for historical training (no live injury data).
    Returns TOP3_PPG, TOP5_PM, STAR_AVAILABLE.
    """
    defaults = {"TOP3_PPG": 0.0, "TOP5_PM": 0.0, "STAR_AVAILABLE": 1.0}

    if player_df is None or player_df.empty:
        return defaults
    if len(games_before) == 0:
        return defaults

    # Get game IDs from games before this game
    if "GAME_ID" not in games_before.columns:
        return defaults

    game_ids = set(games_before["GAME_ID"].astype(str).tolist())
    # player_df is now a pre-indexed per-team slice (already filtered by team_id)
    # so we only need to filter by game_id here.
    if "GAME_ID" not in player_df.columns:
        return defaults
    team_players = player_df[
        player_df["GAME_ID"].astype(str).isin(game_ids)
    ].copy()

    if team_players.empty:
        return defaults

    # Per-player season averages
    if "PTS" not in team_players.columns:
        return defaults

    team_players["PTS"] = pd.to_numeric(team_players["PTS"], errors="coerce").fillna(0)

    player_avgs = team_players.groupby("PLAYER_ID")["PTS"].mean().sort_values(ascending=False)
    top3_ppg = float(player_avgs.head(3).sum())

    # Plus-minus for top-5 players by minutes
    if "PLUS_MINUS" in team_players.columns and "MIN" in team_players.columns:
        team_players["MIN"] = pd.to_numeric(team_players["MIN"], errors="coerce").fillna(0)
        team_players["PLUS_MINUS"] = pd.to_numeric(team_players["PLUS_MINUS"], errors="coerce").fillna(0)
        player_min = team_players.groupby("PLAYER_ID")["MIN"].mean().sort_values(ascending=False)
        top5_ids = player_min.head(5).index
        top5_pm = float(team_players[team_players["PLAYER_ID"].isin(top5_ids)].groupby("PLAYER_ID")["PLUS_MINUS"].mean().sum())
    else:
        top5_pm = 0.0

    # Star available: is top scorer playing? (historically always 1 unless injured)
    star_available = 1.0

    return {
        "TOP3_PPG": top3_ppg,
        "TOP5_PM": top5_pm,
        "STAR_AVAILABLE": star_available,
    }


# ── ELO tracker ───────────────────────────────────────────────────────────────

class EloTracker:
    """Tracks Elo ratings for all teams, updated after each settled game."""

    def __init__(self, k: float = ELO_K, default: float = ELO_DEFAULT):
        self.k = k
        self.default = default
        self.ratings: dict = {}

    def get(self, team_id) -> float:
        return self.ratings.get(str(team_id), self.default)

    def update(self, home_id, away_id, home_win: bool) -> None:
        """Update Elo ratings for both teams after a game result."""
        home_elo = self.get(home_id)
        away_elo = self.get(away_id)
        expected = _elo_expected(home_elo, away_elo)
        actual = 1.0 if home_win else 0.0
        self.ratings[str(home_id)] = _elo_update(home_elo, actual, expected, self.k)
        self.ratings[str(away_id)] = _elo_update(away_elo, 1.0 - actual, 1.0 - expected, self.k)


# ── Main feature builder ──────────────────────────────────────────────────────

def build_training_data(
    data_dir: str,
    output_dir: str,
) -> pd.DataFrame:
    """
    Core pipeline: load CSVs → compute features → write training_data.csv.

    For each game (sorted by date ascending):
      1. Look up home/away team season-to-date stats from TeamStatistics using
         only rows with GAME_DATE < current game date (strict lookahead prevention).
      2. Compute rolling last-10 stats.
      3. Compute schedule/rest features.
      4. Compute Elo differential (snapshot before game, update after).
      5. Compute player features (TOP3_PPG, TOP5_PM).
      6. Assemble differential feature vector and WIN label.

    Returns the full training DataFrame.
    """
    # Load raw data
    games_df = load_games(data_dir)
    team_df = load_team_stats(data_dir)
    player_df = load_player_stats(data_dir)

    # Enrich player_df with TEAM_ID by joining on (GAME_ID, home indicator).
    # PlayerStatistics.csv lacks teamId; TeamStatistics.csv has it.
    if "TEAM_ID" not in player_df.columns and "GAME_ID" in team_df.columns:
        home_col_player = None
        for c in ("home", "IS_HOME", "HOME"):
            if c in player_df.columns:
                home_col_player = c
                break
        home_col_team = "IS_HOME" if "IS_HOME" in team_df.columns else None
        if home_col_player and home_col_team:
            # Build lookup: (GAME_ID, IS_HOME bool) → TEAM_ID
            tid_lookup = team_df[["GAME_ID", home_col_team, "TEAM_ID"]].copy()
            tid_lookup = tid_lookup.rename(columns={home_col_team: "_home_key"})
            tid_lookup["_home_key"] = tid_lookup["_home_key"].map(
                lambda x: True if str(x).lower() in ("true", "1", "yes") else False
            )
            player_df["_home_key"] = player_df[home_col_player].map(
                lambda x: True if str(x).lower() in ("true", "1", "yes") else False
            )
            player_df = player_df.merge(
                tid_lookup.drop_duplicates(subset=["GAME_ID", "_home_key"]),
                left_on=["GAME_ID", "_home_key"],
                right_on=["GAME_ID", "_home_key"],
                how="left",
            )
            player_df = player_df.drop(columns=["_home_key"])
            log.info(
                f"Enriched player_df with TEAM_ID: "
                f"{player_df['TEAM_ID'].notna().sum():,}/{len(player_df):,} rows matched."
            )

    # Filter to target seasons
    if "SEASON" in games_df.columns:
        games_df = games_df[games_df["SEASON"].isin(SEASONS)].copy()
        log.info(f"Games after season filter: {len(games_df):,}")

    # Sort games chronologically
    games_df = games_df.sort_values("GAME_DATE").reset_index(drop=True)

    # Sort team stats chronologically per team
    team_df = team_df.sort_values(["TEAM_ID", "GAME_DATE"]).reset_index(drop=True)

    # Build per-team game log index for fast lookup
    team_game_logs: dict = {}
    for team_id, grp in team_df.groupby("TEAM_ID"):
        team_game_logs[str(team_id)] = grp.copy()

    # Build per-team player log index for fast player feature lookup.
    # Scanning 1.66M player rows per game is too slow; pre-index by TEAM_ID.
    player_game_logs: dict = {}
    if "TEAM_ID" in player_df.columns:
        for team_id, grp in player_df.groupby("TEAM_ID"):
            player_game_logs[str(team_id)] = grp.copy()
        log.info(f"Built player game log index for {len(player_game_logs)} teams.")

    elo = EloTracker()
    rows = []
    skipped = 0

    log.info(f"Building features for {len(games_df):,} games...")

    for idx, game in games_df.iterrows():
        game_date = game["GAME_DATE"]

        # Skip rows with invalid dates
        if pd.isna(game_date):
            skipped += 1
            continue

        home_id = str(game.get("HOME_TEAM_ID", ""))
        away_id = str(game.get("AWAY_TEAM_ID", ""))
        game_id = str(game.get("GAME_ID", idx))
        season = str(game.get("SEASON", ""))
        game_type = str(game.get("GAME_TYPE", "Regular Season"))

        home_score = pd.to_numeric(game.get("HOME_SCORE", np.nan), errors="coerce")
        away_score = pd.to_numeric(game.get("AWAY_SCORE", np.nan), errors="coerce")

        if pd.isna(home_score) or pd.isna(away_score):
            skipped += 1
            continue

        home_win = int(home_score > away_score)

        # ── Retrieve pre-game team logs (strict lookahead prevention) ──────
        def get_log_before(team_id: str) -> pd.DataFrame:
            log_df = team_game_logs.get(team_id, pd.DataFrame())
            if log_df.empty:
                return log_df
            return log_df[log_df["GAME_DATE"] < game_date].copy()

        home_log = get_log_before(home_id)
        away_log = get_log_before(away_id)

        # ── Season-to-date profiles ────────────────────────────────────────
        hp = compute_team_profile(home_log)
        ap = compute_team_profile(away_log)

        # ── Rolling last-10 ───────────────────────────────────────────────
        hr = compute_rolling_profile(home_log, window=10)
        ar = compute_rolling_profile(away_log, window=10)

        # ── Schedule features ─────────────────────────────────────────────
        hs = compute_schedule_features(home_log, game_date)
        as_ = compute_schedule_features(away_log, game_date)

        # ── Streaks ───────────────────────────────────────────────────────
        home_streak = compute_streak(home_log)
        away_streak = compute_streak(away_log)

        # ── Elo (pre-game snapshot, then update) ──────────────────────────
        home_elo = elo.get(home_id)
        away_elo = elo.get(away_id)
        elo_diff = home_elo - away_elo

        # ── Player features ───────────────────────────────────────────────
        home_player_log = player_game_logs.get(home_id, pd.DataFrame())
        away_player_log = player_game_logs.get(away_id, pd.DataFrame())
        hpf = compute_player_features(home_player_log, home_id, home_log)
        apf = compute_player_features(away_player_log, away_id, away_log)

        # ── Season progress ───────────────────────────────────────────────
        home_games = hp.get("GAMES_PLAYED", 0)
        season_progress = min(home_games / 82.0, 1.0)

        # ── Assemble differential features ───────────────────────────────
        def diff(key: str, home_dict: dict, away_dict: dict, default: float = 0.0) -> float:
            h = home_dict.get(key, default)
            a = away_dict.get(key, default)
            try:
                return round(float(h) - float(a), 6)
            except (TypeError, ValueError):
                return 0.0

        row = {
            # Metadata (not features)
            "GAME_ID": game_id,
            "GAME_DATE": game_date.date().isoformat() if not pd.isna(game_date) else None,
            "SEASON": season,
            "HOME_TEAM_ID": home_id,
            "AWAY_TEAM_ID": away_id,
            "HOME_SCORE": float(home_score),
            "AWAY_SCORE": float(away_score),

            # Target
            "WIN": home_win,

            # Team efficiency diffs
            "OFF_RTG_DIFF":     diff("OFF_RTG",   hp, ap),
            "DEF_RTG_DIFF":     diff("DEF_RTG",   hp, ap),
            "EFG_PCT_DIFF":     diff("EFG_PCT",   hp, ap),
            "TS_PCT_DIFF":      diff("TS_PCT",    hp, ap),
            "OREB_PCT_DIFF":    diff("OREB_PCT",  hp, ap),
            "DREB_PCT_DIFF":    diff("DREB_PCT",  hp, ap),
            "TOV_RATE_DIFF":    diff("TOV_RATE",  hp, ap),
            "FT_RATE_DIFF":     diff("FT_RATE",   hp, ap),
            "FG3_RATE_DIFF":    diff("FG3_RATE",  hp, ap),
            "BENCH_PTS_DIFF":   diff("BENCH_PTS", hp, ap),
            "PAINT_PTS_DIFF":   diff("PAINT_PTS", hp, ap),

            # Rolling form diffs
            "ROLL10_WIN_PCT_DIFF": diff("ROLL10_WIN_PCT", hr, ar),
            "ROLL10_NET_RTG_DIFF": diff("ROLL10_NET_RTG", hr, ar),
            "ROLL10_PTS_DIFF":     diff("ROLL10_PTS",     hr, ar),
            "ROLL10_EFG_DIFF":     diff("ROLL10_EFG",     hr, ar),
            "ROLL10_TOV_DIFF":     diff("ROLL10_TOV",     hr, ar),

            # Schedule / fatigue diffs
            "DAYS_REST_DIFF":    round(hs["DAYS_REST"] - as_["DAYS_REST"], 2),
            "IS_B2B_DIFF":       float(hs["IS_B2B"] - as_["IS_B2B"]),
            "GAMES_LAST_7_DIFF": float(hs["GAMES_LAST_7"] - as_["GAMES_LAST_7"]),

            # Player features diffs
            "INJURY_IMPACT_DIFF":  0.0,   # not available in historical data
            "STAR_AVAILABLE_DIFF": diff("STAR_AVAILABLE", hpf, apf),
            "TOP3_PPG_DIFF":       diff("TOP3_PPG",       hpf, apf),
            "TOP5_PM_DIFF":        diff("TOP5_PM",        hpf, apf),

            # Context
            "HOME_COURT":      1.0,
            "ELO_DIFF":        round(elo_diff, 2),
            "WIN_PCT_DIFF":    diff("WIN_PCT", hp, ap),
            "STREAK_DIFF":     float(home_streak - away_streak),
            "SEASON_PROGRESS": round(season_progress, 4),
            "IS_PLAYOFF":      float(int(_is_playoff(game_type))),

            # Odds — NaN-imputed to 0.0 for historical training
            "SPREAD_DIFF":  0.0,
            "ML_PROB_DIFF": 0.0,
            "OVER_UNDER":   0.0,
        }

        rows.append(row)

        # Update Elo after game result is known
        elo.update(home_id, away_id, bool(home_win))

        if (idx + 1) % 1000 == 0:
            log.info(f"  Processed {idx + 1:,} / {len(games_df):,} games ...")

    if skipped > 0:
        log.warning(f"Skipped {skipped} games with missing dates or scores.")

    df = pd.DataFrame(rows)
    df = df.sort_values("GAME_DATE").reset_index(drop=True)

    # Fill any residual NaNs in feature columns with 0
    for col in FEATURES:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)

    log.info(f"\nTraining data built: {len(df):,} rows, {len(df.columns)} columns")

    # Summary stats
    if "WIN" in df.columns:
        home_win_rate = df["WIN"].mean()
        log.info(f"  Home win rate : {home_win_rate:.3f}")
    if "SEASON" in df.columns:
        season_counts = df.groupby("SEASON").size()
        log.info(f"  Games per season:\n{season_counts.to_string()}")

    # Print feature completeness
    for col in FEATURES:
        if col in df.columns:
            nan_rate = df[col].isna().mean()
            if nan_rate > 0.01:
                log.warning(f"  Feature {col}: {nan_rate:.1%} NaN")

    # Save output
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "training_data.csv")
    df.to_csv(out_path, index=False)
    log.info(f"\nSaved training_data.csv → {out_path}")

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build training_data.csv from raw Kaggle NBA CSVs.",
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
        help=f"Directory to write training_data.csv (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    output_dir = os.path.abspath(args.output_dir)

    log.info(f"Data directory  : {data_dir}")
    log.info(f"Output directory: {output_dir}")

    df = build_training_data(data_dir, output_dir)

    print(f"\nDone. {len(df):,} games processed.")
    print(f"Output: {os.path.join(output_dir, 'training_data.csv')}")


if __name__ == "__main__":
    main()
