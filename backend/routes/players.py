"""
routes/players.py
-----------------
Player stat endpoints backed by Neon player_game_stats table.
"""

import logging
import math
from datetime import datetime, timezone
from fastapi import APIRouter, Query

from utils.db import execute_query
from utils.cache import cached
from utils.nba_assets import get_player_headshot_url, get_team_logo_url, get_team_colors

log    = logging.getLogger(__name__)
router = APIRouter()


def _f(val, default=0.0) -> float:
    """Float coercion that treats NaN/None as default."""
    try:
        v = float(val or default)
        return default if math.isnan(v) or math.isinf(v) else v
    except (TypeError, ValueError):
        return default

SEASON = "2025"


@router.get("/stats")
@cached(ttl_seconds=3600)
def get_player_stats(
    min_gp: int = Query(default=5, description="Minimum games played"),
):
    """
    Season-to-date per-game averages for all players, aggregated from Neon.
    Sorted by points per game descending.
    """
    try:
        rows = execute_query(
            """
            SELECT
                pgs.player_id,
                pgs.player_name,
                pgs.team_id,
                COUNT(*)                              AS games_played,
                ROUND(AVG(pgs.pts)::numeric, 1)       AS pts,
                ROUND(AVG(pgs.reb)::numeric, 1)       AS reb,
                ROUND(AVG(pgs.ast)::numeric, 1)       AS ast,
                ROUND(AVG(pgs.stl)::numeric, 1)       AS stl,
                ROUND(AVG(pgs.blk)::numeric, 1)       AS blk,
                ROUND(AVG(pgs.tov)::numeric, 1)       AS tov,
                ROUND(AVG(pgs.minutes)::numeric, 1)   AS avg_min,
                ROUND(AVG(pgs.fg_pct)::numeric, 3)    AS fg_pct,
                ROUND(AVG(pgs.fg3_pct)::numeric, 3)   AS fg3_pct,
                ROUND(AVG(pgs.ft_pct)::numeric, 3)    AS ft_pct,
                ROUND(AVG(pgs.plus_minus)::numeric, 1) AS plus_minus
            FROM player_game_stats pgs
            JOIN games g ON g.game_id = pgs.game_id
            WHERE g.season = %s
            GROUP BY pgs.player_id, pgs.player_name, pgs.team_id
            HAVING COUNT(*) >= %s
            ORDER BY AVG(pgs.pts) DESC
            """,
            (SEASON, min_gp),
        )

        players = []
        for r in rows:
            pid  = int(r.get("player_id", 0))
            tid  = int(r.get("team_id", 0) or 0)
            colors = get_team_colors(tid)
            players.append({
                "player_id":     pid,
                "player_name":   r.get("player_name", ""),
                "team_id":       tid,
                "team_name":     "",
                "games_played":  int(r.get("games_played", 0)),
                "pts":           _f(r.get("pts")),
                "reb":           _f(r.get("reb")),
                "ast":           _f(r.get("ast")),
                "stl":           _f(r.get("stl")),
                "blk":           _f(r.get("blk")),
                "tov":           _f(r.get("tov")),
                "avg_min":       _f(r.get("avg_min")),
                "fg_pct":        _f(r.get("fg_pct")),
                "fg3_pct":       _f(r.get("fg3_pct")),
                "ft_pct":        _f(r.get("ft_pct")),
                "plus_minus":    _f(r.get("plus_minus")),
                "headshot_url":  get_player_headshot_url(pid),
                "logo_url":      get_team_logo_url(tid),
                "primary_color": colors["primary"],
                "secondary_color": colors["secondary"],
            })

        return {
            "players": players,
            "count":   len(players),
            "season":  SEASON,
            "updated": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        log.error("get_player_stats failed: %s", exc)
        return {
            "players": [],
            "count":   0,
            "season":  SEASON,
            "updated": datetime.now(timezone.utc).isoformat(),
            "error":   str(exc),
        }


@router.get("/{player_id}/gamelogs")
def get_player_gamelogs(player_id: int):
    """
    Return the last 20 game logs for a player, sorted by game_date descending.
    """
    try:
        rows = execute_query(
            """
            SELECT
                pgs.game_id,
                g.game_date,
                pgs.win,
                pgs.home,
                pgs.minutes,
                pgs.pts, pgs.reb, pgs.ast, pgs.stl, pgs.blk, pgs.tov,
                pgs.fg_pct, pgs.fg3_pct, pgs.ft_pct, pgs.plus_minus,
                pgs.team_id,
                CASE WHEN pgs.home THEN g.home_team_name ELSE g.away_team_name END AS team_name,
                CASE WHEN pgs.home
                     THEN g.home_team_name || ' vs ' || g.away_team_name
                     ELSE g.away_team_name || ' @ ' || g.home_team_name END AS matchup
            FROM player_game_stats pgs
            JOIN games g ON g.game_id = pgs.game_id
            WHERE pgs.player_id = %s
              AND g.season = %s
            ORDER BY g.game_date DESC
            LIMIT 20
            """,
            (player_id, SEASON),
        )

        clean = []
        for r in rows:
            clean.append({
                "game_id":    r.get("game_id", ""),
                "game_date":  str(r.get("game_date", ""))[:10],
                "matchup":    r.get("matchup", ""),
                "wl":         "W" if r.get("win") else "L",
                "min":        float(r.get("minutes") or 0),
                "pts":        int(r.get("pts") or 0),
                "reb":        int(r.get("reb") or 0),
                "ast":        int(r.get("ast") or 0),
                "stl":        int(r.get("stl") or 0),
                "blk":        int(r.get("blk") or 0),
                "tov":        int(r.get("tov") or 0),
                "fg_pct":     float(r.get("fg_pct") or 0),
                "fg3_pct":    float(r.get("fg3_pct") or 0),
                "ft_pct":     float(r.get("ft_pct") or 0),
                "plus_minus": float(r.get("plus_minus") or 0),
                "team_id":    int(r.get("team_id") or 0),
                "team_name":  r.get("team_name", ""),
            })

        return {"player_id": player_id, "games": clean, "count": len(clean)}

    except Exception as exc:
        log.error("get_player_gamelogs(%d) failed: %s", player_id, exc)
        return {"player_id": player_id, "games": [], "count": 0, "error": str(exc)}


@router.get("/{player_id}/info")
def get_player_info(player_id: int):
    """
    Return player bio and headshot URL.
    Queries Neon player_info table; enriches with CDN headshot URL.
    """
    try:
        rows = execute_query(
            """
            SELECT
                player_id, player_name, position, height, weight,
                birthdate, country, school, draft_year, draft_round,
                draft_number, experience, jersey, team_id, team_name, status
            FROM player_info
            WHERE player_id = %s
            LIMIT 1
            """,
            (player_id,),
        )

        if not rows:
            return {
                "player_id":    player_id,
                "headshot_url": get_player_headshot_url(player_id),
            }

        r = rows[0]
        tid = int(r.get("team_id") or 0)
        return {
            "player_id":    player_id,
            "player_name":  r.get("player_name", ""),
            "position":     r.get("position", ""),
            "height":       r.get("height", ""),
            "weight":       r.get("weight", ""),
            "birthdate":    str(r.get("birthdate", ""))[:10],
            "country":      r.get("country", ""),
            "school":       r.get("school", ""),
            "draft_year":   r.get("draft_year", ""),
            "draft_round":  r.get("draft_round", ""),
            "draft_number": r.get("draft_number", ""),
            "experience":   int(r.get("experience") or 0),
            "jersey":       r.get("jersey", ""),
            "team_id":      tid,
            "team_name":    r.get("team_name", ""),
            "status":       r.get("status", "Active"),
            "headshot_url": get_player_headshot_url(player_id),
            "logo_url":     get_team_logo_url(tid),
            "colors":       get_team_colors(tid),
        }

    except Exception as exc:
        log.error("get_player_info(%d) failed: %s", player_id, exc)
        return {
            "player_id":    player_id,
            "headshot_url": get_player_headshot_url(player_id),
            "error":        str(exc),
        }
