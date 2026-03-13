"""
routes/transactions.py
----------------------
Player transaction feed from stats.nba.com
"""
import logging
from fastapi import APIRouter
from utils.nba_cdn import get_transactions
from utils.cache import cached

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("")
@cached(ttl_seconds=3600)
def get_player_transactions():
    try:
        data = get_transactions()
        rows = []

        # stats.nba.com NBA_Player_Movement.json structure:
        # {"NBA_Player_Movement": {"rows": [...], "headers": [...]}}
        movement = data.get("NBA_Player_Movement", {})
        headers = movement.get("headers", [])
        raw_rows = movement.get("rows", [])

        # Normalize to dict rows
        if headers and raw_rows:
            for raw in raw_rows:
                if isinstance(raw, list) and len(raw) == len(headers):
                    row = dict(zip(headers, raw))
                elif isinstance(raw, dict):
                    row = raw
                else:
                    continue

                rows.append({
                    "player_name":        row.get("PLAYER_NAME", row.get("PlayerName", "")),
                    "team_from":          row.get("TEAM_FROM", row.get("TeamFrom", "")),
                    "team_to":            row.get("TEAM_TO", row.get("TeamTo", "")),
                    "transaction_type":   row.get("TRANSACTION_TYPE", row.get("TransactionType", "")),
                    "transaction_date":   row.get("TRANSACTION_DATE", row.get("TransactionDate", "")),
                    "notes":              row.get("TRANSACTION_DESCRIPTION", row.get("Notes", "")),
                })
        elif isinstance(raw_rows, list) and raw_rows and isinstance(raw_rows[0], dict):
            # Already dict format
            for raw in raw_rows:
                rows.append({
                    "player_name":        raw.get("PLAYER_NAME", ""),
                    "team_from":          raw.get("TEAM_FROM", ""),
                    "team_to":            raw.get("TEAM_TO", ""),
                    "transaction_type":   raw.get("TRANSACTION_TYPE", ""),
                    "transaction_date":   raw.get("TRANSACTION_DATE", ""),
                    "notes":              raw.get("TRANSACTION_DESCRIPTION", ""),
                })

        # Sort by date desc, return last 50
        rows.sort(key=lambda r: r.get("transaction_date", ""), reverse=True)
        rows = rows[:50]

        return {"transactions": rows, "count": len(rows)}

    except Exception as e:
        logger.error("Failed to fetch transactions: %s", e)
        return {"transactions": [], "count": 0, "error": str(e)}
