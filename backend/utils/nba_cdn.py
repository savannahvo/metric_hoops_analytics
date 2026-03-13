"""
utils/nba_cdn.py
----------------
NBA CDN and Stats HTTP fetch helpers using httpx.
No nba_api library — raw HTTP only.
All public endpoints; no auth required.
"""

import time
import logging
import httpx

from utils.cache import cached

log = logging.getLogger(__name__)

BASE        = "https://cdn.nba.com/static/json/liveData"
STATS_BASE  = "https://stats.nba.com/stats"

STATS_HEADERS = {
    "Referer":             "https://www.nba.com",
    "User-Agent":          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":              "application/json",
    "x-nba-stats-origin":  "stats",
    "x-nba-stats-token":   "true",
}


def _get(url: str, headers: dict | None = None, timeout: int = 15) -> dict:
    """
    HTTP GET with retry (3 attempts, 1 s back-off).
    Returns parsed JSON dict, or raises on persistent failure.
    """
    last_exc: Exception = RuntimeError("No attempts made")
    for attempt in range(3):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                resp = client.get(url, headers=headers or {})
                resp.raise_for_status()
                return resp.json()
        except Exception as exc:
            last_exc = exc
            log.warning("_get attempt %d failed for %s: %s", attempt + 1, url, exc)
            if attempt < 2:
                time.sleep(1.0)
    raise last_exc


@cached(ttl_seconds=30)
def get_todays_scoreboard() -> dict:
    """
    Fetch live scoreboard for today.
    GET https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json
    """
    url = f"{BASE}/scoreboard/todaysScoreboard_00.json"
    try:
        return _get(url)
    except Exception as exc:
        log.error("get_todays_scoreboard failed: %s", exc)
        return {}


@cached(ttl_seconds=60)
def get_boxscore(game_id: str) -> dict:
    """
    Fetch live box score for a game.
    GET https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json
    """
    url = f"{BASE}/boxscore/boxscore_{game_id}.json"
    try:
        return _get(url)
    except Exception as exc:
        log.error("get_boxscore failed for game_id=%s: %s", game_id, exc)
        return {}


@cached(ttl_seconds=60)
def get_playbyplay(game_id: str) -> dict:
    """
    Fetch live play-by-play for a game.
    GET https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json
    """
    url = f"{BASE}/playbyplay/playbyplay_{game_id}.json"
    try:
        return _get(url)
    except Exception as exc:
        log.error("get_playbyplay failed for game_id=%s: %s", game_id, exc)
        return {}


@cached(ttl_seconds=30)
def get_todays_odds() -> dict:
    """
    Fetch today's game odds.
    GET https://cdn.nba.com/static/json/liveData/odds/odds_todaysGames.json
    """
    url = f"{BASE}/odds/odds_todaysGames.json"
    try:
        return _get(url)
    except Exception as exc:
        log.error("get_todays_odds failed: %s", exc)
        return {}


@cached(ttl_seconds=3600)
def get_schedule() -> dict:
    """
    Fetch full league schedule.
    GET https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json
    """
    url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
    try:
        return _get(url)
    except Exception as exc:
        log.error("get_schedule failed: %s", exc)
        return {}


@cached(ttl_seconds=3600)
def get_playoff_bracket(season_year: str = "2025-26", state: int = 1) -> dict:
    """
    Fetch playoff bracket data.
    GET https://stats.nba.com/stats/playoffbracket?LeagueID=00&SeasonYear={year}&State={state}
    """
    url = f"{STATS_BASE}/playoffbracket"
    params = f"?LeagueID=00&SeasonYear={season_year}&State={state}"
    try:
        return _get(url + params, headers=STATS_HEADERS, timeout=20)
    except Exception as exc:
        log.error("get_playoff_bracket failed: %s", exc)
        return {}


@cached(ttl_seconds=3600)
def get_transactions() -> dict:
    """
    Fetch NBA player movement / transactions.
    GET https://stats.nba.com/js/data/playermovement/NBA_Player_Movement.json
    """
    url = "https://stats.nba.com/js/data/playermovement/NBA_Player_Movement.json"
    try:
        return _get(url, headers=STATS_HEADERS, timeout=20)
    except Exception as exc:
        log.error("get_transactions failed: %s", exc)
        return {}
