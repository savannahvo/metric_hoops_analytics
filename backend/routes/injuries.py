"""
routes/injuries.py
------------------
Injury report endpoint — reads snapshot data from Neon injuries table.
"""

import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Query

from utils.db import execute_query
from utils.cache import cached

log    = logging.getLogger(__name__)
router = APIRouter()


@router.get("/")
@cached(ttl_seconds=10800)
def get_injuries(
    team_name: str = Query(default=None, description="Filter by team name (partial match)"),
    status:    str = Query(default=None, description="Filter by status (Out, Questionable, etc.)"),
):
    """
    Return all injury records for the most recent snapshot date.
    Optionally filter by team_name and/or status.
    """
    try:
        # Get the latest snapshot date
        snapshot_rows = execute_query(
            "SELECT MAX(snapshot_date) AS max_date FROM injuries"
        )
        snapshot_date = snapshot_rows[0].get("max_date") if snapshot_rows else None

        if not snapshot_date:
            return {
                "injuries":      [],
                "snapshot_date": None,
                "count":         0,
                "warning":       "No injury snapshot data found in database",
            }

        # Build dynamic WHERE clauses
        conditions = ["snapshot_date = %s"]
        params: list = [snapshot_date]

        if team_name:
            conditions.append("LOWER(team_name) LIKE %s")
            params.append(f"%{team_name.lower()}%")

        if status:
            conditions.append("LOWER(status) = %s")
            params.append(status.lower())

        where = " AND ".join(conditions)

        rows = execute_query(
            f"""
            SELECT
                player_id, player_name, team_id, team_name,
                position, injury_type, status, updated,
                snapshot_date
            FROM injuries
            WHERE {where}
            ORDER BY team_name ASC, player_name ASC
            """,
            params,
        )

        return {
            "injuries":      rows,
            "snapshot_date": str(snapshot_date),
            "count":         len(rows),
            "updated":       datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        log.error("get_injuries failed: %s", exc)
        return {
            "injuries":      [],
            "snapshot_date": None,
            "count":         0,
            "updated":       datetime.now(timezone.utc).isoformat(),
            "error":         str(exc),
        }
