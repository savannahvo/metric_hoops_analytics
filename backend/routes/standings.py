"""
routes/standings.py
-------------------
Standings computed from the Neon team_game_stats table.
Returns East and West conferences sorted by win percentage,
with playoff/play-in positioning.
"""

import logging
from datetime import datetime, timezone
from collections import defaultdict
from fastapi import APIRouter

from utils.db import execute_query
from utils.cache import cached
from utils.nba_assets import get_team_colors, get_team_logo_url, TEAM_TRICODES

log    = logging.getLogger(__name__)
router = APIRouter()

# ── Conference membership (team_id → "East" | "West") ─────────────────────────
_CONFERENCE: dict[int, str] = {
    # East
    1610612737: "East",   # Atlanta Hawks
    1610612738: "East",   # Boston Celtics
    1610612751: "East",   # Brooklyn Nets
    1610612766: "East",   # Charlotte Hornets
    1610612741: "East",   # Chicago Bulls
    1610612739: "East",   # Cleveland Cavaliers
    1610612765: "East",   # Detroit Pistons
    1610612754: "East",   # Indiana Pacers
    1610612748: "East",   # Miami Heat
    1610612749: "East",   # Milwaukee Bucks
    1610612752: "East",   # New York Knicks
    1610612753: "East",   # Orlando Magic
    1610612755: "East",   # Philadelphia 76ers
    1610612761: "East",   # Toronto Raptors
    1610612764: "East",   # Washington Wizards
    # West
    1610612743: "West",   # Denver Nuggets
    1610612744: "West",   # Golden State Warriors
    1610612745: "West",   # Houston Rockets
    1610612746: "West",   # LA Clippers
    1610612747: "West",   # Los Angeles Lakers
    1610612763: "West",   # Memphis Grizzlies
    1610612750: "West",   # Minnesota Timberwolves
    1610612740: "West",   # New Orleans Pelicans
    1610612760: "West",   # Oklahoma City Thunder
    1610612756: "West",   # Phoenix Suns
    1610612757: "West",   # Portland Trail Blazers
    1610612758: "West",   # Sacramento Kings
    1610612759: "West",   # San Antonio Spurs
    1610612762: "West",   # Utah Jazz
    1610612742: "West",   # Dallas Mavericks
}


def _compute_standings(rows: list[dict]) -> dict:
    """
    Aggregate raw team_game_stats rows into standings records.
    Returns {"East": [...], "West": [...]}.
    """
    teams: dict[int, dict] = {}

    for r in rows:
        tid = int(r.get("team_id", 0))
        if not tid:
            continue

        if tid not in teams:
            teams[tid] = {
                "team_id":    tid,
                "team_name":  r.get("team_name", ""),
                "tricode":    TEAM_TRICODES.get(tid, ""),
                "conference": _CONFERENCE.get(tid, "Unknown"),
                "logo_url":   get_team_logo_url(tid),
                "colors":     get_team_colors(tid),
                "wins":       0,
                "losses":     0,
                "home_wins":  0,
                "home_losses": 0,
                "away_wins":  0,
                "away_losses": 0,
                "last10_wins":  0,
                "last10_losses": 0,
                "_game_dates": [],
            }

        t   = teams[tid]
        won     = bool(r.get("win"))
        is_home = bool(r.get("home"))

        if won:
            t["wins"] += 1
        else:
            t["losses"] += 1

        if is_home:
            if won:
                t["home_wins"] += 1
            else:
                t["home_losses"] += 1
        else:
            if won:
                t["away_wins"] += 1
            else:
                t["away_losses"] += 1

        t["_game_dates"].append((r.get("game_date"), won))

    east, west = [], []

    for tid, t in teams.items():
        # Sort games by date desc to get last 10 and streak
        sorted_games = sorted(t["_game_dates"], key=lambda x: x[0] or "", reverse=True)
        last10 = sorted_games[:10]
        t["last10_wins"]   = sum(1 for _, w in last10 if w)
        t["last10_losses"] = sum(1 for _, w in last10 if not w)

        # Streak: count consecutive same results from most recent game
        streak_count = 0
        streak_result = None
        for _, won in sorted_games:
            if streak_result is None:
                streak_result = won
            if won == streak_result:
                streak_count += 1
            else:
                break
        t["streak"] = f"{'W' if streak_result else 'L'}{streak_count}" if streak_result is not None else "—"

        del t["_game_dates"]

        total    = t["wins"] + t["losses"]
        t["win_pct"] = round(t["wins"] / total, 3) if total > 0 else 0.0
        t["games_played"] = total
        t["home_record"]  = f"{t['home_wins']}-{t['home_losses']}"
        t["away_record"]  = f"{t['away_wins']}-{t['away_losses']}"
        t["last_10"]      = f"{t['last10_wins']}-{t['last10_losses']}"

        conf = t["conference"]
        if conf == "East":
            east.append(t)
        else:
            west.append(t)

    def _rank(standings: list) -> list:
        standings.sort(key=lambda x: (-x["win_pct"], -x["wins"]))
        leader = standings[0] if standings else None
        for i, t in enumerate(standings, start=1):
            if leader:
                t["gb"] = round(
                    ((leader["wins"] - t["wins"]) + (t["losses"] - leader["losses"])) / 2, 1
                )
            else:
                t["gb"] = 0.0
            if i <= 6:
                t["playoff_position"] = i
                t["standing_label"] = "Playoff"
            elif i <= 10:
                t["playoff_position"] = i
                t["standing_label"] = "Play-In"
            else:
                t["playoff_position"] = i
                t["standing_label"] = "Lottery"
        return standings

    return {"east": _rank(east), "west": _rank(west)}


@router.get("/")
@cached(ttl_seconds=1800)
def get_standings():
    """
    Return East and West standings aggregated from Neon team_game_stats.
    Falls back to an empty structure with an error field if the DB is unavailable.
    """
    try:
        rows = execute_query(
            """
            SELECT
                tgs.team_id,
                tgs.team_name,
                tgs.win,
                tgs.home,
                g.game_date
            FROM team_game_stats tgs
            JOIN games g ON g.game_id = tgs.game_id
            WHERE g.season = '2025'
            ORDER BY g.game_date ASC
            """
        )

        if not rows:
            return {
                "east":    [],
                "west":    [],
                "updated": datetime.now(timezone.utc).isoformat(),
                "warning": "No data found in team_game_stats for the current season",
            }

        standings = _compute_standings(rows)
        standings["updated"] = datetime.now(timezone.utc).isoformat()
        return standings

    except Exception as exc:
        log.error("get_standings failed: %s", exc)
        return {
            "east":    [],
            "west":    [],
            "updated": datetime.now(timezone.utc).isoformat(),
            "error":   str(exc),
        }
