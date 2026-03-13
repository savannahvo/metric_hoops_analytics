"""
routes/playoffs.py
------------------
Playoff bracket endpoint — sourced from NBA Stats CDN.
"""

import logging
from datetime import datetime, timezone
from fastapi import APIRouter

from utils.nba_cdn import get_playoff_bracket
from utils.cache import cached

log    = logging.getLogger(__name__)
router = APIRouter()


@router.get("/")
@cached(ttl_seconds=3600)
def get_playoffs(season_year: str = "2025-26", state: int = 1):
    """
    Return the NBA playoff bracket for the given season.
    Returns {status: "not_started"} when the bracket hasn't been seeded yet.
    """
    try:
        data = get_playoff_bracket(season_year=season_year, state=state)

        if not data:
            return {
                "status":  "not_started",
                "season":  season_year,
                "message": "Playoff bracket data is not yet available",
                "updated": datetime.now(timezone.utc).isoformat(),
            }

        # The NBA Stats endpoint returns resultSets; unwrap if present
        result_sets = data.get("resultSets", [])
        if not result_sets:
            # May already be 'not started' or empty payload
            return {
                "status":  "not_started",
                "season":  season_year,
                "message": "Playoff bracket has not started yet",
                "updated": datetime.now(timezone.utc).isoformat(),
            }

        # Parse bracket series from each result set
        bracket: dict = {
            "season":  season_year,
            "state":   state,
            "series":  [],
            "updated": datetime.now(timezone.utc).isoformat(),
        }

        for rs in result_sets:
            name    = rs.get("name", "")
            headers = rs.get("headers", [])
            rows    = rs.get("rowSet", [])
            series_list = []

            for row in rows:
                entry = dict(zip(headers, row))
                series_list.append(entry)

            bracket["series"].append({
                "name":   name,
                "series": series_list,
            })

        bracket["status"] = "active"
        return bracket

    except Exception as exc:
        log.error("get_playoffs failed: %s", exc)
        return {
            "status":  "not_started",
            "season":  season_year,
            "message": str(exc),
            "updated": datetime.now(timezone.utc).isoformat(),
            "error":   str(exc),
        }
