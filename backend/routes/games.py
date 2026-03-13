"""
routes/games.py
---------------
Game endpoints: today's scoreboard, games by date, box score,
play-by-play, and schedule — all sourced from the NBA CDN.
"""

import logging
from datetime import datetime, timezone
from fastapi import APIRouter

import pytz

from utils.nba_cdn import (
    get_todays_scoreboard,
    get_boxscore,
    get_playbyplay,
    get_schedule,
)
from utils.cache import cached

log    = logging.getLogger(__name__)
router = APIRouter()

_ET = pytz.timezone("America/New_York")


def _safe_int(val, default: int = 0) -> int:
    try:
        v = int(val)
        return v if v >= 0 else default
    except (TypeError, ValueError):
        return default


def _parse_game(g: dict, date_str: str) -> dict:
    """Normalize a raw CDN game dict into a consistent response shape."""
    home = g.get("homeTeam", {})
    away = g.get("awayTeam", {})
    return {
        "game_id":       g.get("gameId", ""),
        "home_team":     f"{home.get('teamCity', '')} {home.get('teamName', '')}".strip(),
        "away_team":     f"{away.get('teamCity', '')} {away.get('teamName', '')}".strip(),
        "home_team_id":  _safe_int(home.get("teamId")),
        "away_team_id":  _safe_int(away.get("teamId")),
        "home_tricode":  home.get("teamTricode", ""),
        "away_tricode":  away.get("teamTricode", ""),
        "home_score":    _safe_int(home.get("score")),
        "away_score":    _safe_int(away.get("score")),
        "status":        g.get("gameStatusText", ""),
        "period":        _safe_int(g.get("period")),
        "clock":         g.get("gameClock", ""),
        "date":          date_str,
        "arena":         g.get("arenaName", ""),
        "tip_off_utc":   g.get("gameTimeUTC", ""),
    }


@router.get("/today")
@cached(ttl_seconds=30)
def get_todays_games():
    """Return all games on today's NBA schedule with live scores."""
    try:
        data  = get_todays_scoreboard()
        board = data.get("scoreboard", data)
        games_raw = board.get("games", [])

        today = datetime.now(_ET).strftime("%Y-%m-%d")
        games = [_parse_game(g, today) for g in games_raw]

        return {
            "games":   games,
            "count":   len(games),
            "date":    today,
            "updated": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        log.error("get_todays_games failed: %s", exc)
        return {"games": [], "count": 0, "date": "", "updated": datetime.now(timezone.utc).isoformat(), "error": str(exc)}


@router.get("/date/{date}")
def get_games_by_date(date: str):
    """
    Return games for a given date (YYYY-MM-DD).
    Today uses the CDN live scoreboard; other dates fall back to the Neon games table.
    """
    today = datetime.now(_ET).strftime("%Y-%m-%d")

    if date == today:
        return get_todays_games()

    # Validate date format
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return {"games": [], "count": 0, "date": date, "error": "Invalid date format — use YYYY-MM-DD"}

    # Fallback: query Neon games table for non-today dates
    try:
        from utils.db import execute_query
        rows = execute_query(
            """
            SELECT game_id, home_team, away_team, home_team_id, away_team_id,
                   home_tricode, away_tricode, home_score, away_score,
                   status, period, game_clock AS clock, game_date AS date,
                   arena, tip_off_utc
            FROM games
            WHERE game_date = %s
            ORDER BY tip_off_utc ASC
            """,
            (date,),
        )
        return {"games": rows, "count": len(rows), "date": date, "updated": datetime.now(timezone.utc).isoformat()}
    except Exception as exc:
        log.error("get_games_by_date(%s) Neon fallback failed: %s", date, exc)
        return {"games": [], "count": 0, "date": date, "updated": datetime.now(timezone.utc).isoformat(), "error": str(exc)}


@router.get("/schedule")
@cached(ttl_seconds=3600)
def get_full_schedule():
    """
    Return scheduled games from the CDN league schedule JSON.
    Flattens game weeks into a flat list of game dicts.
    """
    try:
        data     = get_schedule()
        league   = data.get("leagueSchedule", {})
        weeks    = league.get("gameDates", [])

        games = []
        for week in weeks:
            game_date = week.get("gameDate", "")[:10]
            for g in week.get("games", []):
                home = g.get("homeTeam", {})
                away = g.get("awayTeam", {})
                games.append({
                    "game_id":       g.get("gameId", ""),
                    "game_date":     game_date,
                    "tip_off_utc":   g.get("gameDateTimeUTC", ""),
                    "home_team":     f"{home.get('teamCity', '')} {home.get('teamName', '')}".strip(),
                    "away_team":     f"{away.get('teamCity', '')} {away.get('teamName', '')}".strip(),
                    "home_team_id":  _safe_int(home.get("teamId")),
                    "away_team_id":  _safe_int(away.get("teamId")),
                    "home_tricode":  home.get("teamTricode", ""),
                    "away_tricode":  away.get("teamTricode", ""),
                    "arena":         g.get("arenaName", ""),
                    "status":        g.get("gameStatusText", ""),
                })

        return {"games": games, "count": len(games), "updated": datetime.now(timezone.utc).isoformat()}
    except Exception as exc:
        log.error("get_full_schedule failed: %s", exc)
        return {"games": [], "count": 0, "updated": datetime.now(timezone.utc).isoformat(), "error": str(exc)}


@router.get("/{game_id}/boxscore")
def get_game_boxscore(game_id: str):
    """
    Return box score for a game — home and away teams with per-player rows.
    Sourced from the NBA CDN live boxscore JSON.
    """
    try:
        data = get_boxscore(game_id)
        if not data:
            return {"game_id": game_id, "home": {}, "away": {}}

        game  = data.get("game", data)
        home  = game.get("homeTeam", {})
        away  = game.get("awayTeam", {})

        def fmt_players(team: dict) -> list[dict]:
            players = []
            for p in team.get("players", []):
                s = p.get("statistics", {})
                players.append({
                    "player_id":   _safe_int(p.get("personId")),
                    "name":        p.get("nameI", p.get("name", "")),
                    "jersey":      p.get("jerseyNum", ""),
                    "position":    p.get("position", ""),
                    "starter":     p.get("starter", "0") == "1",
                    "minutes":     s.get("minutesCalculated", ""),
                    "pts":         _safe_int(s.get("points")),
                    "reb":         _safe_int(s.get("reboundsTotal")),
                    "ast":         _safe_int(s.get("assists")),
                    "stl":         _safe_int(s.get("steals")),
                    "blk":         _safe_int(s.get("blocks")),
                    "tov":         _safe_int(s.get("turnovers")),
                    "pf":          _safe_int(s.get("foulsPersonal")),
                    "fg":          f"{s.get('fieldGoalsMade', 0)}/{s.get('fieldGoalsAttempted', 0)}",
                    "fg3":         f"{s.get('threePointersMade', 0)}/{s.get('threePointersAttempted', 0)}",
                    "ft":          f"{s.get('freeThrowsMade', 0)}/{s.get('freeThrowsAttempted', 0)}",
                    "plus_minus":  s.get("plusMinusPoints", 0),
                })
            return players

        def fmt_team(t: dict) -> dict:
            ts = t.get("statistics", {})
            return {
                "team_id":    _safe_int(t.get("teamId")),
                "team_name":  t.get("teamName", ""),
                "tricode":    t.get("teamTricode", ""),
                "score":      _safe_int(t.get("score")),
                "in_bonus":   str(t.get("inBonus", "0")).strip() in ("1", "True", "true"),
                "timeouts":   _safe_int(t.get("timeoutsRemaining")),
                "periods":    [{"period": p.get("period"), "score": _safe_int(p.get("score"))}
                               for p in t.get("periods", [])],
                "players":    fmt_players(t),
            }

        return {
            "game_id": game_id,
            "home":    fmt_team(home),
            "away":    fmt_team(away),
        }
    except Exception as exc:
        log.error("get_game_boxscore(%s) failed: %s", game_id, exc)
        return {"game_id": game_id, "home": {}, "away": {}, "error": str(exc)}


@router.get("/{game_id}/playbyplay")
def get_game_playbyplay(game_id: str):
    """
    Return play-by-play actions for a game sourced from the NBA CDN PBP JSON.
    """
    try:
        data = get_playbyplay(game_id)
        if not data:
            return {"game_id": game_id, "plays": [], "count": 0}

        game    = data.get("game", data)
        actions = game.get("actions", [])

        plays = []
        for a in actions:
            plays.append({
                "action_number": a.get("actionNumber"),
                "clock":         a.get("clock", ""),
                "period":        _safe_int(a.get("period")),
                "team_tricode":  a.get("teamTricode", ""),
                "player_name":   a.get("playerNameI", a.get("playerName", "")),
                "action_type":   a.get("actionType", ""),
                "sub_type":      a.get("subType", ""),
                "description":   a.get("description", ""),
                "score_home":    a.get("scoreHome", ""),
                "score_away":    a.get("scoreAway", ""),
                "shot_result":   a.get("shotResult", ""),
            })

        return {"game_id": game_id, "plays": plays, "count": len(plays)}
    except Exception as exc:
        log.error("get_game_playbyplay(%s) failed: %s", game_id, exc)
        return {"game_id": game_id, "plays": [], "count": 0, "error": str(exc)}
