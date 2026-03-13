"""
daily_ingest.py
---------------
1. Fresh download from Kaggle (eoinamoore/historical-nba-data-and-player-box-scores)
2. Parse Games.csv, TeamStatistics.csv, PlayerStatistics.csv
3. Upsert into Neon PostgreSQL:
   - games table
   - team_game_stats table
   - player_game_stats table (2020-21+ only, pruned to 2 seasons for 512MB Neon limit)

Usage:
    python daily_ingest.py                    # ingest yesterday's data
    python daily_ingest.py --backfill-days 90 # load last 90 days
    python daily_ingest.py --full             # load all seasons (use with caution - Neon 512MB limit)
"""

import argparse
import os
import shutil
import sys
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
KAGGLE_DATASET = "eoinamoore/historical-nba-data-and-player-box-scores"
DOWNLOAD_DIR = Path("/tmp/kaggle_nba")
PLAYER_STATS_MIN_SEASON = 2020  # 2020-21 and later

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_env() -> str:
    """Load DATABASE_URL from .env file or environment."""
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL not set. Add it to .env or the environment.")
        sys.exit(1)
    return db_url


def download_dataset() -> Path:
    """Download fresh copy of the Kaggle dataset to DOWNLOAD_DIR."""
    if DOWNLOAD_DIR.exists():
        shutil.rmtree(DOWNLOAD_DIR)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Authenticating with Kaggle API …")
    try:
        from kaggle import KaggleApi

        api = KaggleApi()
        api.authenticate()
        log.info("Downloading dataset '%s' …", KAGGLE_DATASET)
        api.dataset_download_files(
            KAGGLE_DATASET,
            path=str(DOWNLOAD_DIR),
            unzip=True,
            quiet=False,
        )
    except Exception as exc:
        log.error("Kaggle download failed: %s", exc)
        sys.exit(1)

    log.info("Dataset saved to %s", DOWNLOAD_DIR)
    return DOWNLOAD_DIR


def cleanup() -> None:
    if DOWNLOAD_DIR.exists():
        shutil.rmtree(DOWNLOAD_DIR)
        log.info("Cleaned up %s", DOWNLOAD_DIR)


def find_csv(directory: Path, stem_pattern: str) -> Path | None:
    """Case-insensitive search for a CSV file matching a stem pattern.
    Returns the shortest-named match to prefer base files over advanced variants."""
    pattern_lower = stem_pattern.lower()
    matches = [p for p in directory.rglob("*.csv") if pattern_lower in p.stem.lower()]
    if not matches:
        return None
    return min(matches, key=lambda p: len(p.stem))


def safe_int(val, default=None):
    try:
        if pd.isna(val):
            return default
        return int(val)
    except (ValueError, TypeError):
        return default


def safe_float(val, default=None):
    try:
        if pd.isna(val):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def coerce_date(val):
    """Parse various date representations to a Python date object."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Games table
# ---------------------------------------------------------------------------

GAMES_UPSERT = """
INSERT INTO games (
    game_id, game_date, season, home_team_id, away_team_id,
    home_team_name, away_team_name, home_score, away_score,
    winner, game_type
)
VALUES %s
ON CONFLICT (game_id) DO UPDATE SET
    game_date       = EXCLUDED.game_date,
    season          = EXCLUDED.season,
    home_team_id    = EXCLUDED.home_team_id,
    away_team_id    = EXCLUDED.away_team_id,
    home_team_name  = EXCLUDED.home_team_name,
    away_team_name  = EXCLUDED.away_team_name,
    home_score      = EXCLUDED.home_score,
    away_score      = EXCLUDED.away_score,
    winner          = EXCLUDED.winner,
    game_type       = EXCLUDED.game_type;
"""


def _col(df: pd.DataFrame, *candidates: str) -> str | None:
    """Return the first column name from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def load_games(directory: Path, cutoff_date: date | None) -> pd.DataFrame:
    csv_path = find_csv(directory, "game")
    if csv_path is None:
        log.warning("Games.csv not found in %s — skipping games table", directory)
        return pd.DataFrame()

    log.info("Reading %s …", csv_path)
    df = pd.read_csv(csv_path, low_memory=False)
    log.info("  Raw rows: %d, columns: %s", len(df), list(df.columns))

    # Normalise column names to lowercase stripped versions for matching
    df.columns = [c.strip() for c in df.columns]

    # ---- game_id ----
    game_id_col = _col(df, "gameId", "game_id", "GAME_ID", "GameId")
    if game_id_col is None:
        log.error("Cannot find gameId column in Games.csv. Columns: %s", list(df.columns))
        return pd.DataFrame()
    df["_game_id"] = df[game_id_col].astype(str).str.strip()

    # ---- game_date ----
    date_col = _col(df, "gameDateTimeEst", "gameDate", "game_date", "GAME_DATE", "GameDate")
    if date_col:
        df["_game_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    else:
        df["_game_date"] = None

    # ---- season ----
    # eoinamoore dataset has no seasonYear column; derive from game date.
    # NBA season X starts in October of year X (e.g. 2024-25 → season year 2024).
    season_col = _col(df, "seasonYear", "season", "SEASON", "Season")
    if season_col:
        df["_season"] = df[season_col].apply(safe_int)
    elif "_game_date" in df.columns:
        def _date_to_season_year(d):
            if d is None:
                return None
            return d.year if d.month >= 10 else d.year - 1
        df["_season"] = df["_game_date"].apply(_date_to_season_year)
    else:
        df["_season"] = None

    # ---- team ids ----
    # eoinamoore dataset uses lowercase 't': hometeamId, awayteamId
    home_tid_col = _col(df, "hometeamId", "homeTeamId", "home_team_id", "HOME_TEAM_ID", "HomeTeamId")
    away_tid_col = _col(df, "awayteamId", "awayTeamId", "away_team_id", "VISITOR_TEAM_ID", "AwayTeamId")
    df["_home_team_id"] = df[home_tid_col].apply(safe_int) if home_tid_col else None
    df["_away_team_id"] = df[away_tid_col].apply(safe_int) if away_tid_col else None

    # ---- team names ----
    home_name_col = _col(df, "hometeamName", "homeTeamName", "home_team_name", "HOME_TEAM_NAME", "HomeTeamName")
    away_name_col = _col(df, "awayteamName", "awayTeamName", "away_team_name", "VISITOR_TEAM_NAME", "AwayTeamName")
    df["_home_team_name"] = df[home_name_col].astype(str) if home_name_col else ""
    df["_away_team_name"] = df[away_name_col].astype(str) if away_name_col else ""

    # ---- scores ----
    home_score_col = _col(df, "homeScore", "home_score", "HOME_TEAM_SCORE", "PTS_home", "HomeScore")
    away_score_col = _col(df, "awayScore", "away_score", "VISITOR_TEAM_SCORE", "PTS_away", "AwayScore")
    df["_home_score"] = df[home_score_col].apply(safe_int) if home_score_col else None
    df["_away_score"] = df[away_score_col].apply(safe_int) if away_score_col else None

    # ---- winner ----
    df["_winner"] = df.apply(
        lambda r: (
            "home"
            if (r["_home_score"] is not None and r["_away_score"] is not None and r["_home_score"] > r["_away_score"])
            else ("away" if (r["_home_score"] is not None and r["_away_score"] is not None) else None)
        ),
        axis=1,
    )

    # ---- game_type ----
    gtype_col = _col(df, "gameType", "game_type", "GAME_TYPE", "GameType", "seasonType")
    df["_game_type"] = df[gtype_col].astype(str) if gtype_col else "Regular Season"

    # ---- filter by cutoff ----
    if cutoff_date is not None:
        before = len(df)
        df = df[df["_game_date"] >= cutoff_date]
        log.info("  Date filter (>= %s): %d → %d rows", cutoff_date, before, len(df))

    return df


def upsert_games(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = [
        (
            row["_game_id"],
            row["_game_date"],
            row["_season"],
            row["_home_team_id"],
            row["_away_team_id"],
            row["_home_team_name"] or None,
            row["_away_team_name"] or None,
            row["_home_score"],
            row["_away_score"],
            row["_winner"],
            row["_game_type"],
        )
        for _, row in df.iterrows()
    ]
    with conn.cursor() as cur:
        execute_values(cur, GAMES_UPSERT, rows)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# team_game_stats table
# ---------------------------------------------------------------------------

TEAM_STATS_UPSERT = """
INSERT INTO team_game_stats (
    game_id, team_id, team_name,
    pts, reb, reb_off, reb_def,
    ast, stl, blk, tov, fouls,
    fg_pct, fg3_pct, ft_pct,
    pts_paint, pts_fast_break, pts_bench,
    plus_minus,
    season_wins, season_losses
)
VALUES %s
ON CONFLICT (game_id, team_id) DO UPDATE SET
    team_name       = EXCLUDED.team_name,
    pts             = EXCLUDED.pts,
    reb             = EXCLUDED.reb,
    reb_off         = EXCLUDED.reb_off,
    reb_def         = EXCLUDED.reb_def,
    ast             = EXCLUDED.ast,
    stl             = EXCLUDED.stl,
    blk             = EXCLUDED.blk,
    tov             = EXCLUDED.tov,
    fouls           = EXCLUDED.fouls,
    fg_pct          = EXCLUDED.fg_pct,
    fg3_pct         = EXCLUDED.fg3_pct,
    ft_pct          = EXCLUDED.ft_pct,
    pts_paint       = EXCLUDED.pts_paint,
    pts_fast_break  = EXCLUDED.pts_fast_break,
    pts_bench       = EXCLUDED.pts_bench,
    plus_minus      = EXCLUDED.plus_minus,
    season_wins     = EXCLUDED.season_wins,
    season_losses   = EXCLUDED.season_losses;
"""


def load_team_stats(directory: Path, cutoff_date: date | None, games_df: pd.DataFrame) -> pd.DataFrame:
    csv_path = find_csv(directory, "teamstat")
    if csv_path is None:
        csv_path = find_csv(directory, "team_stat")
    if csv_path is None:
        csv_path = find_csv(directory, "teamgame")
    if csv_path is None:
        log.warning("TeamStatistics.csv not found — skipping team_game_stats")
        return pd.DataFrame()

    log.info("Reading %s …", csv_path)
    df = pd.read_csv(csv_path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    log.info("  Raw rows: %d, columns: %s", len(df), list(df.columns))

    # Build a game_id → game_date lookup from games_df
    date_lookup: dict[str, date] = {}
    if not games_df.empty and "_game_id" in games_df.columns and "_game_date" in games_df.columns:
        date_lookup = dict(zip(games_df["_game_id"].astype(str), games_df["_game_date"]))

    def _c(*candidates):
        return _col(df, *candidates)

    game_id_col = _c("gameId", "game_id", "GAME_ID", "GameId")
    if game_id_col is None:
        log.error("Cannot find gameId in TeamStatistics. Skipping.")
        return pd.DataFrame()

    df["_game_id"] = df[game_id_col].astype(str).str.strip()

    team_id_col = _c("teamId", "team_id", "TEAM_ID", "TeamId")
    df["_team_id"] = df[team_id_col].apply(safe_int) if team_id_col else None

    team_name_col = _c("teamName", "team_name", "TEAM_NAME", "TeamName", "teamCity")
    df["_team_name"] = df[team_name_col].astype(str) if team_name_col else ""

    def g(col_name, *extras, fn=safe_int):
        c = _c(col_name, *extras)
        return df[c].apply(fn) if c else pd.Series([None] * len(df))

    # eoinamoore dataset uses full names: teamScore, reboundsOffensive, plusMinusPoints, etc.
    df["_pts"]            = g("teamScore", "PTS", "pts", "points")
    df["_reb"]            = g("reboundsTotal", "REB", "reb", "rebounds")
    df["_reb_off"]        = g("reboundsOffensive", "OREB", "oreb")
    df["_reb_def"]        = g("reboundsDefensive", "DREB", "dreb")
    df["_ast"]            = g("assists", "AST", "ast")
    df["_stl"]            = g("steals", "STL", "stl")
    df["_blk"]            = g("blocks", "BLK", "blk")
    df["_tov"]            = g("turnovers", "TOV", "tov")
    df["_fouls"]          = g("foulsPersonal", "fouls", "personalFouls", "PF", "pf")
    df["_fg_pct"]         = g("fieldGoalsPercentage", "FG_PCT", "fg_pct", fn=safe_float)
    df["_fg3_pct"]        = g("threePointersPercentage", "FG3_PCT", "fg3_pct", fn=safe_float)
    df["_ft_pct"]         = g("freeThrowsPercentage", "FT_PCT", "ft_pct", fn=safe_float)
    df["_pts_paint"]      = g("pointsInThePaint", "PTS_PAINT", "pts_paint")
    df["_pts_fast_break"] = g("fastBreakPoints", "PTS_FB", "pts_fast_break")
    df["_pts_bench"]      = g("benchPoints", "PTS_BENCH", "pts_bench")
    df["_plus_minus"]     = g("plusMinusPoints", "PLUS_MINUS", "plus_minus", fn=safe_float)

    # ---- season wins / losses (running tally per team per season) ----
    # Attach season from games_df
    season_lookup: dict[str, int] = {}
    if not games_df.empty and "_game_id" in games_df.columns and "_season" in games_df.columns:
        season_lookup = {
            str(gid): s
            for gid, s in zip(games_df["_game_id"], games_df["_season"])
            if s is not None
        }
    df["_season"] = df["_game_id"].map(season_lookup)

    # Game date lookup
    df["_game_date"] = df["_game_id"].map(date_lookup)

    # Filter by cutoff
    if cutoff_date is not None:
        before = len(df)
        df = df[df["_game_date"] >= cutoff_date]
        log.info("  Date filter team_stats (>= %s): %d → %d rows", cutoff_date, before, len(df))

    # Compute running wins/losses within this dataset.
    # Must compute _is_win on df BEFORE creating df_sorted so the column is present.
    wins_col = _c("win", "WIN", "teamWins", "isHomeWin", "wl")
    df["_is_win"] = None
    if wins_col:
        df["_is_win"] = df[wins_col].apply(lambda x: bool(safe_int(x, 0)))

    # Also use dataset's pre-computed season wins/losses if available
    sw_col = _c("seasonWins", "season_wins", "SEASON_WINS")
    sl_col = _c("seasonLosses", "season_losses", "SEASON_LOSSES")
    if sw_col and sl_col:
        df["_season_wins"]   = df[sw_col].apply(safe_int)
        df["_season_losses"] = df[sl_col].apply(safe_int)
    else:
        df["_season_wins"]  = None
        df["_season_losses"] = None
        if wins_col:
            df_sorted = df.sort_values("_game_date", na_position="last").copy()
            df_sorted["_cumwin"]    = df_sorted.groupby(["_team_id", "_season"])["_is_win"].cumsum()
            df_sorted["_cumgames"]  = df_sorted.groupby(["_team_id", "_season"]).cumcount() + 1
            df_sorted["_season_wins"]   = df_sorted["_cumwin"].apply(safe_int)
            df_sorted["_season_losses"] = (df_sorted["_cumgames"] - df_sorted["_cumwin"]).apply(safe_int)
            df = df_sorted

    return df


def upsert_team_stats(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = []
    for _, row in df.iterrows():
        rows.append((
            str(row["_game_id"]),
            row.get("_team_id"),
            row.get("_team_name") or None,
            row.get("_pts"),
            row.get("_reb"),
            row.get("_reb_off"),
            row.get("_reb_def"),
            row.get("_ast"),
            row.get("_stl"),
            row.get("_blk"),
            row.get("_tov"),
            row.get("_fouls"),
            row.get("_fg_pct"),
            row.get("_fg3_pct"),
            row.get("_ft_pct"),
            row.get("_pts_paint"),
            row.get("_pts_fast_break"),
            row.get("_pts_bench"),
            row.get("_plus_minus"),
            row.get("_season_wins"),
            row.get("_season_losses"),
        ))
    with conn.cursor() as cur:
        execute_values(cur, TEAM_STATS_UPSERT, rows)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# player_game_stats table
# ---------------------------------------------------------------------------

PLAYER_STATS_UPSERT = """
INSERT INTO player_game_stats (
    game_id, player_id, player_name, team_id,
    pts, reb, ast, stl, blk, tov,
    fg_pct, fg3_pct, ft_pct,
    plus_minus, minutes
)
VALUES %s
ON CONFLICT (game_id, player_id) DO UPDATE SET
    player_name = EXCLUDED.player_name,
    team_id     = EXCLUDED.team_id,
    pts         = EXCLUDED.pts,
    reb         = EXCLUDED.reb,
    ast         = EXCLUDED.ast,
    stl         = EXCLUDED.stl,
    blk         = EXCLUDED.blk,
    tov         = EXCLUDED.tov,
    fg_pct      = EXCLUDED.fg_pct,
    fg3_pct     = EXCLUDED.fg3_pct,
    ft_pct      = EXCLUDED.ft_pct,
    plus_minus  = EXCLUDED.plus_minus,
    minutes     = EXCLUDED.minutes;
"""


def parse_minutes(val) -> float | None:
    """Convert 'MM:SS' or numeric minutes to float."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip()
    if ":" in s:
        try:
            parts = s.split(":")
            return float(parts[0]) + float(parts[1]) / 60.0
        except Exception:
            return None
    return safe_float(val)


def load_player_stats(
    directory: Path,
    cutoff_date: date | None,
    games_df: pd.DataFrame,
    full: bool = False,
) -> pd.DataFrame:
    csv_path = find_csv(directory, "playerstat")
    if csv_path is None:
        csv_path = find_csv(directory, "player_stat")
    if csv_path is None:
        csv_path = find_csv(directory, "playergame")
    if csv_path is None:
        log.warning("PlayerStatistics.csv not found — skipping player_game_stats")
        return pd.DataFrame()

    log.info("Reading %s …", csv_path)
    df = pd.read_csv(csv_path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    log.info("  Raw rows: %d, columns: %s", len(df), list(df.columns))

    def _c(*candidates):
        return _col(df, *candidates)

    game_id_col = _c("gameId", "game_id", "GAME_ID", "GameId")
    if game_id_col is None:
        log.error("Cannot find gameId in PlayerStatistics. Skipping.")
        return pd.DataFrame()
    df["_game_id"] = df[game_id_col].astype(str).str.strip()

    # Build season lookup from games_df
    season_lookup: dict[str, int] = {}
    date_lookup: dict[str, date] = {}
    if not games_df.empty:
        if "_game_id" in games_df.columns and "_season" in games_df.columns:
            season_lookup = {
                str(gid): s
                for gid, s in zip(games_df["_game_id"], games_df["_season"])
                if s is not None
            }
        if "_game_id" in games_df.columns and "_game_date" in games_df.columns:
            date_lookup = dict(zip(games_df["_game_id"].astype(str), games_df["_game_date"]))

    df["_season"] = df["_game_id"].map(season_lookup)
    df["_game_date"] = df["_game_id"].map(date_lookup)

    # Filter to 2020-21+ unless --full
    if not full:
        before = len(df)
        df = df[df["_season"] >= PLAYER_STATS_MIN_SEASON]
        log.info(
            "  Season filter (>= %d): %d → %d rows", PLAYER_STATS_MIN_SEASON, before, len(df)
        )

    if cutoff_date is not None:
        before = len(df)
        df = df[df["_game_date"] >= cutoff_date]
        log.info("  Date filter player_stats (>= %s): %d → %d rows", cutoff_date, before, len(df))

    # Remaining column mapping
    player_id_col = _c("personId", "playerId", "player_id", "PLAYER_ID", "PlayerId")
    team_id_col   = _c("teamId", "team_id", "TEAM_ID", "TeamId")

    df["_player_id"] = df[player_id_col].apply(safe_int) if player_id_col else None
    df["_team_id"]   = df[team_id_col].apply(safe_int) if team_id_col else None

    # eoinamoore dataset stores firstName + lastName separately
    pn_col = _c("playerName", "player_name", "PLAYER_NAME", "PlayerName", "name")
    fn_col = _c("firstName", "first_name")
    ln_col = _c("lastName", "last_name")
    if pn_col:
        df["_player_name"] = df[pn_col].astype(str)
    elif fn_col and ln_col:
        df["_player_name"] = (
            df[fn_col].astype(str).str.strip() + " " + df[ln_col].astype(str).str.strip()
        )
    else:
        df["_player_name"] = ""

    def g(col_name, *extras, fn=safe_int):
        c = _c(col_name, *extras)
        return df[c].apply(fn) if c else pd.Series([None] * len(df))

    # eoinamoore dataset uses full column names
    df["_pts"]        = g("points", "PTS", "pts")
    df["_reb"]        = g("reboundsTotal", "REB", "reb", "rebounds")
    df["_ast"]        = g("assists", "AST", "ast")
    df["_stl"]        = g("steals", "STL", "stl")
    df["_blk"]        = g("blocks", "BLK", "blk")
    df["_tov"]        = g("turnovers", "TOV", "tov")
    df["_fg_pct"]     = g("fieldGoalsPercentage", "FG_PCT", "fg_pct", fn=safe_float)
    df["_fg3_pct"]    = g("threePointersPercentage", "FG3_PCT", "fg3_pct", fn=safe_float)
    df["_ft_pct"]     = g("freeThrowsPercentage", "FT_PCT", "ft_pct", fn=safe_float)
    df["_plus_minus"] = g("plusMinusPoints", "PLUS_MINUS", "plus_minus", fn=safe_float)

    min_col = _c("numMinutes", "MIN", "min", "minutes")
    df["_min"] = df[min_col].apply(parse_minutes) if min_col else None

    return df


def upsert_player_stats(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = []
    for _, row in df.iterrows():
        rows.append((
            str(row["_game_id"]),
            row.get("_player_id"),
            row.get("_player_name") or None,
            row.get("_team_id"),
            row.get("_pts"),
            row.get("_reb"),
            row.get("_ast"),
            row.get("_stl"),
            row.get("_blk"),
            row.get("_tov"),
            row.get("_fg_pct"),
            row.get("_fg3_pct"),
            row.get("_ft_pct"),
            row.get("_plus_minus"),
            row.get("_min"),
        ))
    with conn.cursor() as cur:
        execute_values(cur, PLAYER_STATS_UPSERT, rows)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest NBA data from Kaggle → Neon PostgreSQL")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--backfill-days",
        type=int,
        metavar="N",
        help="Load only the last N days of data",
    )
    group.add_argument(
        "--full",
        action="store_true",
        help="Load all seasons (caution: Neon 512MB limit)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Determine date cutoff
    cutoff_date: date | None = None
    if args.full:
        log.info("Mode: FULL load — all seasons")
    elif args.backfill_days:
        cutoff_date = date.today() - timedelta(days=args.backfill_days)
        log.info("Mode: backfill last %d days (cutoff: %s)", args.backfill_days, cutoff_date)
    else:
        cutoff_date = date.today() - timedelta(days=1)
        log.info("Mode: yesterday's data (cutoff: %s)", cutoff_date)

    # Load env & connect
    db_url = load_env()
    log.info("Connecting to Neon …")
    try:
        conn = psycopg2.connect(db_url)
    except Exception as exc:
        log.error("DB connection failed: %s", exc)
        sys.exit(1)

    try:
        # Download dataset
        data_dir = download_dataset()

        # --- Games ---
        games_df = load_games(data_dir, cutoff_date)
        n_games = upsert_games(conn, games_df)
        log.info("games upserted: %d", n_games)

        # --- Team stats ---
        team_df = load_team_stats(data_dir, cutoff_date, games_df)
        n_team = upsert_team_stats(conn, team_df)
        log.info("team_game_stats rows upserted: %d", n_team)

        # --- Player stats ---
        player_df = load_player_stats(data_dir, cutoff_date, games_df, full=args.full)
        n_player = upsert_player_stats(conn, player_df)
        log.info("player_game_stats rows upserted: %d", n_player)

        print(f"\n=== Ingest complete ===")
        print(f"  {n_games} games upserted")
        print(f"  {n_team} team stats rows upserted")
        print(f"  {n_player} player stats rows upserted")

    finally:
        conn.close()
        cleanup()


if __name__ == "__main__":
    main()
