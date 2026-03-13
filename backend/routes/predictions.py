"""
routes/predictions.py
---------------------
Prediction endpoints — today's game predictions, historical log,
accuracy metrics, and model drift data.
All backed by the Neon predictions and drift_log tables.
"""

import logging
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Query

import pytz

from utils.db import execute_query
from utils.cache import cached

log    = logging.getLogger(__name__)
router = APIRouter()

_ET = pytz.timezone("America/New_York")


def _et_today() -> str:
    return datetime.now(_ET).strftime("%Y-%m-%d")


@router.get("/today")
@cached(ttl_seconds=60)
def get_todays_predictions():
    """
    Return all pre-game predictions for today's games from Neon predictions table.
    Includes top_features JSONB if present.
    """
    today = _et_today()
    try:
        rows = execute_query(
            """
            SELECT
                game_id, game_date, home_team_name AS home_team, away_team_name AS away_team,
                home_team_id, away_team_id,
                predicted_winner, home_win_prob,
                confidence, predicted_home_score, predicted_away_score,
                predicted_point_diff, top_features,
                actual_winner, actual_home_score, actual_away_score,
                correct, locked_at
            FROM predictions
            WHERE game_date = %s
            ORDER BY locked_at ASC
            """,
            (today,),
        )

        # Ensure home_win_prob / away_win_prob are floats
        for r in rows:
            hwp = float(r.get("home_win_prob") or 0)
            r["home_win_prob"] = hwp
            r["away_win_prob"] = round(1.0 - hwp, 4)

        return {
            "predictions": rows,
            "count":       len(rows),
            "date":        today,
            "updated":     datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        log.error("get_todays_predictions failed: %s", exc)
        return {
            "predictions": [],
            "count":       0,
            "date":        today,
            "updated":     datetime.now(timezone.utc).isoformat(),
            "error":       str(exc),
        }


@router.get("/log")
def get_prediction_log(
    page: int = Query(default=1,  ge=1,  description="Page number (1-based)"),
    size: int = Query(default=25, ge=1, le=100, description="Results per page"),
):
    """
    Paginated log of settled predictions (actual_winner is not null).
    Sorted by game_date descending.
    """
    offset = (page - 1) * size
    try:
        rows = execute_query(
            """
            SELECT
                game_id, game_date,
                home_team_name AS home_team, away_team_name AS away_team,
                predicted_winner, home_win_prob,
                confidence, predicted_home_score, predicted_away_score,
                actual_winner, actual_home_score, actual_away_score,
                correct, score_error, locked_at
            FROM predictions
            WHERE actual_winner IS NOT NULL
            ORDER BY game_date DESC
            LIMIT %s OFFSET %s
            """,
            (size, offset),
        )

        total_rows = execute_query(
            "SELECT COUNT(*) AS total FROM predictions WHERE actual_winner IS NOT NULL"
        )
        total = int(total_rows[0].get("total", 0)) if total_rows else 0

        return {
            "predictions": rows,
            "count":       len(rows),
            "page":        page,
            "size":        size,
            "total":       total,
            "pages":       (total + size - 1) // size if size else 1,
            "updated":     datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        log.error("get_prediction_log failed: %s", exc)
        return {
            "predictions": [],
            "count":       0,
            "page":        page,
            "size":        size,
            "total":       0,
            "pages":       0,
            "updated":     datetime.now(timezone.utc).isoformat(),
            "error":       str(exc),
        }


@router.get("/accuracy")
@cached(ttl_seconds=1800)
def get_accuracy():
    """
    Compute overall season accuracy, 7-day rolling, and 30-day rolling accuracy
    from settled predictions in the Neon predictions table.
    """
    try:
        today  = _et_today()
        d7_ago = (datetime.now(_ET) - timedelta(days=7)).strftime("%Y-%m-%d")
        d30_ago = (datetime.now(_ET) - timedelta(days=30)).strftime("%Y-%m-%d")

        rows = execute_query(
            """
            SELECT
                game_date,
                correct
            FROM predictions
            WHERE actual_winner IS NOT NULL
            ORDER BY game_date ASC
            """
        )

        if not rows:
            return {
                "season_accuracy": 0.0,
                "rolling_7d":      0.0,
                "rolling_30d":     0.0,
                "total_games":     0,
                "correct_games":   0,
                "updated":         datetime.now(timezone.utc).isoformat(),
            }

        def _acc(subset: list) -> float:
            if not subset:
                return 0.0
            correct = sum(1 for r in subset if r.get("correct"))
            return round(correct / len(subset), 4)

        rows_7d  = [r for r in rows if str(r.get("game_date", ""))[:10] >= d7_ago]
        rows_30d = [r for r in rows if str(r.get("game_date", ""))[:10] >= d30_ago]
        total    = len(rows)
        correct  = sum(1 for r in rows if r.get("correct"))

        return {
            "season_accuracy": round(correct / total, 4) if total else 0.0,
            "rolling_7d":      _acc(rows_7d),
            "rolling_30d":     _acc(rows_30d),
            "total_games":     total,
            "correct_games":   correct,
            "updated":         datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        log.error("get_accuracy failed: %s", exc)
        return {
            "season_accuracy": 0.0,
            "rolling_7d":      0.0,
            "rolling_30d":     0.0,
            "total_games":     0,
            "correct_games":   0,
            "updated":         datetime.now(timezone.utc).isoformat(),
            "error":           str(exc),
        }


@router.get("/drift")
@cached(ttl_seconds=3600)
def get_drift():
    """
    Return the last 30 entries from the drift_log table, ordered by log_date descending.
    """
    try:
        rows = execute_query(
            """
            SELECT
                log_date, daily_accuracy, rolling_7d, rolling_30d,
                season_accuracy, total_games, correct_games,
                drift_flag, retrain_triggered, notes
            FROM drift_log
            ORDER BY log_date DESC
            LIMIT 30
            """
        )

        return {
            "drift_log": rows,
            "count":     len(rows),
            "updated":   datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        log.error("get_drift failed: %s", exc)
        return {
            "drift_log": [],
            "count":     0,
            "updated":   datetime.now(timezone.utc).isoformat(),
            "error":     str(exc),
        }
