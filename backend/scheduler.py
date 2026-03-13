"""
scheduler.py
------------
APScheduler prediction locking and settlement.

Jobs:
1. On startup: read today's CDN schedule → queue one DateTrigger job per game (fires 25 min before tip-off)
2. Prediction job: fetch live odds → build features → run models → write to Neon
3. Settlement job: midnight ET — settle yesterday's predictions → update Neon + drift_log
"""

import os
import json
import logging
import numpy as np
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import joblib
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger

from utils.db import execute_query, execute_write
from utils.nba_cdn import get_todays_scoreboard, get_todays_odds

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")

_scheduler: BackgroundScheduler | None = None
_classifier = None
_regressor = None
_selected_features: list[str] = []


# ── Model loading ──────────────────────────────────────────────────────────────

def _load_models():
    global _classifier, _regressor, _selected_features
    clf_path = os.path.join(MODELS_DIR, "classifier.pkl")
    reg_path = os.path.join(MODELS_DIR, "regressor.pkl")
    feat_path = os.path.join(MODELS_DIR, "selected_features.json")

    if os.path.exists(clf_path):
        _classifier = joblib.load(clf_path)
        logger.info("Classifier loaded from %s", clf_path)
    else:
        logger.warning("classifier.pkl not found — predictions disabled")

    if os.path.exists(reg_path):
        _regressor = joblib.load(reg_path)
        logger.info("Regressor loaded from %s", reg_path)
    else:
        logger.warning("regressor.pkl not found — score predictions disabled")

    if os.path.exists(feat_path):
        with open(feat_path) as f:
            data = json.load(f)
        _selected_features = data.get("selected_features", data.get("features", []))
        logger.info("Feature list loaded: %d features", len(_selected_features))


# ── Feature building ───────────────────────────────────────────────────────────

def _build_prediction_features(
    game_id: str,
    home_team_id: int,
    away_team_id: int,
    live_odds: dict | None,
) -> dict:
    """
    Build the feature vector for a pre-game prediction.
    Queries Neon for season-to-date stats; injects live odds when available.
    """
    # Season-to-date stats for both teams (using team_game_stats)
    sql = """
        SELECT
            team_id,
            COUNT(*) AS gp,
            AVG(pts)::float AS avg_pts,
            AVG(opp_pts)::float AS avg_opp_pts,
            AVG(fg_pct)::float AS fg_pct,
            AVG(fg3_pct)::float AS fg3_pct,
            AVG(ft_pct)::float AS ft_pct,
            AVG(reb)::float AS reb,
            AVG(reb_off)::float AS reb_off,
            AVG(reb_def)::float AS reb_def,
            AVG(ast)::float AS ast,
            AVG(stl)::float AS stl,
            AVG(blk)::float AS blk,
            AVG(tov)::float AS tov,
            AVG(pts_paint)::float AS pts_paint,
            AVG(pts_bench)::float AS pts_bench,
            AVG(plus_minus)::float AS plus_minus,
            SUM(CASE WHEN win THEN 1 ELSE 0 END)::float / COUNT(*) AS win_pct,
            MAX(season_wins) AS season_wins,
            MAX(season_losses) AS season_losses
        FROM team_game_stats tgs
        JOIN games g ON tgs.game_id = g.game_id
        WHERE tgs.team_id = ANY(%s)
          AND g.season = '2025-26'
        GROUP BY team_id
    """
    rows = execute_query(sql, [[home_team_id, away_team_id]])
    stats = {r["team_id"]: r for r in rows}

    h = stats.get(home_team_id, {})
    a = stats.get(away_team_id, {})

    def d(key, default=0.0):
        return float(h.get(key, default) or default) - float(a.get(key, default) or default)

    def _poss(s):
        fga = float(s.get("avg_pts", 100) / 1.05)  # rough approx
        fta = fga * 0.22
        oreb = float(s.get("reb_off", 10) or 10)
        tov = float(s.get("tov", 13) or 13)
        return fga + 0.44 * fta - oreb + tov

    h_poss = max(_poss(h), 1.0)
    a_poss = max(_poss(a), 1.0)
    h_off_rtg = float(h.get("avg_pts", 110) or 110) / h_poss * 100
    a_off_rtg = float(a.get("avg_pts", 110) or 110) / a_poss * 100
    h_def_rtg = float(h.get("avg_opp_pts", 110) or 110) / h_poss * 100
    a_def_rtg = float(a.get("avg_opp_pts", 110) or 110) / a_poss * 100

    h_fga = max(float(h.get("avg_pts", 85) / 1.2), 1)
    h_efg = (float(h.get("fg_pct", 0.46) or 0.46) * h_fga + 0.5 * h_fga * float(h.get("fg3_pct", 0.36) or 0.36) * 0.35) / h_fga
    a_fga = max(float(a.get("avg_pts", 85) / 1.2), 1)
    a_efg = (float(a.get("fg_pct", 0.46) or 0.46) * a_fga + 0.5 * a_fga * float(a.get("fg3_pct", 0.36) or 0.36) * 0.35) / a_fga

    h_ts = float(h.get("avg_pts", 110) or 110) / (2 * (h_fga + 0.44 * h_fga * 0.22))
    a_ts = float(a.get("avg_pts", 110) or 110) / (2 * (a_fga + 0.44 * a_fga * 0.22))

    h_oreb = float(h.get("reb_off", 10) or 10)
    h_dreb = float(h.get("reb_def", 30) or 30)
    a_oreb = float(a.get("reb_off", 10) or 10)
    a_dreb = float(a.get("reb_def", 30) or 30)

    # Rolling form — last 10 games
    roll_sql = """
        SELECT team_id,
               AVG(pts) AS roll_pts,
               AVG(opp_pts) AS roll_opp_pts,
               AVG(CASE WHEN win THEN 1 ELSE 0 END) AS roll_win_pct,
               AVG(plus_minus) AS roll_pm,
               AVG(tov) AS roll_tov,
               AVG(fg_pct) AS roll_fg_pct,
               AVG(fg3_pct) AS roll_fg3_pct
        FROM (
            SELECT tgs.team_id, pts, opp_pts, win, plus_minus, tov, fg_pct, fg3_pct,
                   ROW_NUMBER() OVER (PARTITION BY tgs.team_id ORDER BY g.game_date DESC) AS rn
            FROM team_game_stats tgs
            JOIN games g ON tgs.game_id = g.game_id
            WHERE tgs.team_id = ANY(%s) AND g.season = '2025-26'
        ) t WHERE rn <= 10
        GROUP BY team_id
    """
    roll_rows = execute_query(roll_sql, [[home_team_id, away_team_id]])
    roll = {r["team_id"]: r for r in roll_rows}
    rh = roll.get(home_team_id, {})
    ra = roll.get(away_team_id, {})

    h_roll_net = float(rh.get("roll_pts", 110) or 110) - float(rh.get("roll_opp_pts", 110) or 110)
    a_roll_net = float(ra.get("roll_pts", 110) or 110) - float(ra.get("roll_opp_pts", 110) or 110)
    h_roll_efg = float(rh.get("roll_fg_pct", 0.46) or 0.46)
    a_roll_efg = float(ra.get("roll_fg_pct", 0.46) or 0.46)

    # Schedule / rest
    rest_sql = """
        SELECT tgs.team_id, MAX(g.game_date) AS last_game
        FROM team_game_stats tgs JOIN games g ON tgs.game_id = g.game_id
        WHERE tgs.team_id = ANY(%s) AND g.game_date < CURRENT_DATE
        GROUP BY tgs.team_id
    """
    rest_rows = execute_query(rest_sql, [[home_team_id, away_team_id]])
    rest_map = {r["team_id"]: r["last_game"] for r in rest_rows}
    today = datetime.now(ET).date()

    def days_rest(tid):
        last = rest_map.get(tid)
        if last is None:
            return 3
        delta = (today - last).days
        return max(0, min(7, delta))

    def is_b2b(tid):
        return 1 if days_rest(tid) <= 1 else 0

    games_7_sql = """
        SELECT tgs.team_id, COUNT(*) AS cnt
        FROM team_game_stats tgs JOIN games g ON tgs.game_id = g.game_id
        WHERE tgs.team_id = ANY(%s) AND g.game_date >= %s AND g.game_date < CURRENT_DATE
        GROUP BY tgs.team_id
    """
    week_ago = today - timedelta(days=7)
    g7_rows = execute_query(games_7_sql, [[home_team_id, away_team_id], week_ago])
    g7 = {r["team_id"]: r["cnt"] for r in g7_rows}

    # Injury features
    inj_sql = """
        SELECT team_id, player_name, status
        FROM injuries
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM injuries)
          AND team_id = ANY(%s)
    """
    inj_rows = execute_query(inj_sql, [[home_team_id, away_team_id]])

    # Player stats for injury impact
    player_sql = """
        SELECT player_id, player_name, team_id,
               AVG(pts) AS avg_pts, AVG(plus_minus) AS avg_pm,
               AVG(minutes) AS avg_min
        FROM player_game_stats pgs
        JOIN games g ON pgs.game_id = g.game_id
        WHERE pgs.team_id = ANY(%s) AND g.season = '2025-26'
        GROUP BY player_id, player_name, team_id
        ORDER BY avg_pts DESC
    """
    player_rows = execute_query(player_sql, [[home_team_id, away_team_id]])

    def injury_impact(tid):
        team_players = [p for p in player_rows if p["team_id"] == tid]
        team_pts = sum(p.get("avg_pts", 0) or 0 for p in team_players)
        if team_pts == 0:
            return 0.0, 1
        injured_pts = 0.0
        WEIGHTS = {"Out": 1.0, "Out For Season": 1.0, "Doubtful": 0.7,
                   "Day-To-Day": 0.3, "Questionable": 0.25}
        out_names = {r["player_name"].lower(): WEIGHTS.get(r["status"], 0)
                     for r in inj_rows if r["team_id"] == tid}
        for p in team_players:
            name = (p.get("player_name") or "").lower()
            w = out_names.get(name, 0)
            if w > 0:
                injured_pts += float(p.get("avg_pts", 0) or 0) * w
        impact = min(injured_pts / max(team_pts, 1.0), 1.0)
        star_active = sum(1 for p in team_players[:3]
                          if float(p.get("avg_pts", 0) or 0) >= 20
                          and out_names.get((p.get("player_name") or "").lower(), 0) < 0.5)
        return round(impact, 4), star_active

    h_inj, h_stars = injury_impact(home_team_id)
    a_inj, a_stars = injury_impact(away_team_id)

    def top3_ppg(tid):
        tp = sorted([p for p in player_rows if p["team_id"] == tid],
                    key=lambda p: float(p.get("avg_pts", 0) or 0), reverse=True)
        return sum(float(p.get("avg_pts", 0) or 0) for p in tp[:3])

    def top5_pm(tid):
        tp = sorted([p for p in player_rows if p["team_id"] == tid],
                    key=lambda p: float(p.get("avg_min", 0) or 0), reverse=True)
        return sum(float(p.get("avg_pm", 0) or 0) for p in tp[:5])

    # Elo — approximate from win pcts if not stored
    h_wins = int(h.get("season_wins", 20) or 20)
    a_wins = int(a.get("season_wins", 20) or 20)
    h_gp = max(int(h.get("gp", 40) or 40), 1)
    a_gp = max(int(a.get("gp", 40) or 40), 1)
    h_elo = 1500 + (h_wins / h_gp - 0.5) * 400
    a_elo = 1500 + (a_wins / a_gp - 0.5) * 400

    # Season progress
    h_season_progress = h_gp / 82.0
    a_season_progress = a_gp / 82.0

    # Streak (approximate from last 5 games)
    streak_sql = """
        SELECT tgs.team_id,
               SUM(CASE WHEN win THEN 1 ELSE -1 END) AS streak
        FROM (
            SELECT tgs.team_id, win,
                   ROW_NUMBER() OVER (PARTITION BY tgs.team_id ORDER BY g.game_date DESC) AS rn
            FROM team_game_stats tgs JOIN games g ON tgs.game_id = g.game_id
            WHERE tgs.team_id = ANY(%s) AND g.season = '2025-26'
        ) tgs WHERE rn <= 5
        GROUP BY tgs.team_id
    """
    streak_rows = execute_query(streak_sql, [[home_team_id, away_team_id]])
    streaks = {r["team_id"]: r["streak"] for r in streak_rows}

    # Odds features
    spread = 0.0
    ml_prob = 0.0
    over_under = 220.0
    if live_odds:
        spread = float(live_odds.get("spread", 0) or 0)
        ml_h = live_odds.get("ml_home")
        ml_a = live_odds.get("ml_away")
        if ml_h and ml_a:
            try:
                h_implied = (abs(ml_h) / (abs(ml_h) + 100)) if ml_h < 0 else (100 / (ml_h + 100))
                a_implied = (abs(ml_a) / (abs(ml_a) + 100)) if ml_a < 0 else (100 / (ml_a + 100))
                total = h_implied + a_implied
                ml_prob = h_implied / total if total > 0 else 0.5
            except Exception:
                ml_prob = 0.5
        over_under = float(live_odds.get("over_under", 220) or 220)

    features = {
        "OFF_RTG_DIFF":         round(h_off_rtg - a_off_rtg, 3),
        "DEF_RTG_DIFF":         round(h_def_rtg - a_def_rtg, 3),
        "EFG_PCT_DIFF":         round(h_efg - a_efg, 4),
        "TS_PCT_DIFF":          round(h_ts - a_ts, 4),
        "OREB_PCT_DIFF":        round(h_oreb / max(h_oreb + h_dreb, 1) - a_oreb / max(a_oreb + a_dreb, 1), 4),
        "DREB_PCT_DIFF":        round(h_dreb / max(h_oreb + h_dreb, 1) - a_dreb / max(a_oreb + a_dreb, 1), 4),
        "TOV_RATE_DIFF":        round(float(h.get("tov", 13) or 13) / h_poss * 100 - float(a.get("tov", 13) or 13) / a_poss * 100, 3),
        "FT_RATE_DIFF":         d("ft_pct"),
        "FG3_RATE_DIFF":        d("fg3_pct"),
        "BENCH_PTS_DIFF":       d("pts_bench"),
        "PAINT_PTS_DIFF":       d("pts_paint"),
        "ROLL10_WIN_PCT_DIFF":  round(float(rh.get("roll_win_pct", 0.5) or 0.5) - float(ra.get("roll_win_pct", 0.5) or 0.5), 4),
        "ROLL10_NET_RTG_DIFF":  round(h_roll_net - a_roll_net, 3),
        "ROLL10_PTS_DIFF":      round(float(rh.get("roll_pts", 110) or 110) - float(ra.get("roll_pts", 110) or 110), 2),
        "ROLL10_EFG_DIFF":      round(h_roll_efg - a_roll_efg, 4),
        "ROLL10_TOV_DIFF":      round(float(rh.get("roll_tov", 13) or 13) - float(ra.get("roll_tov", 13) or 13), 2),
        "DAYS_REST_DIFF":       float(days_rest(home_team_id) - days_rest(away_team_id)),
        "IS_B2B_DIFF":          float(is_b2b(home_team_id) - is_b2b(away_team_id)),
        "GAMES_LAST_7_DIFF":    float(g7.get(home_team_id, 3) - g7.get(away_team_id, 3)),
        "INJURY_IMPACT_DIFF":   round(a_inj - h_inj, 4),
        "STAR_AVAILABLE_DIFF":  float(h_stars - a_stars),
        "TOP3_PPG_DIFF":        round(top3_ppg(home_team_id) - top3_ppg(away_team_id), 2),
        "TOP5_PM_DIFF":         round(top5_pm(home_team_id) - top5_pm(away_team_id), 2),
        "HOME_COURT":           1.0,
        "ELO_DIFF":             round(h_elo - a_elo, 1),
        "WIN_PCT_DIFF":         round(float(h.get("win_pct", 0.5) or 0.5) - float(a.get("win_pct", 0.5) or 0.5), 4),
        "STREAK_DIFF":          float(streaks.get(home_team_id, 0) - streaks.get(away_team_id, 0)),
        "SEASON_PROGRESS":      round((h_season_progress + a_season_progress) / 2, 3),
        "IS_PLAYOFF":           0.0,
        "SPREAD_DIFF":          spread,
        "ML_PROB_DIFF":         round(ml_prob - 0.5, 4),
        "OVER_UNDER":           over_under,
    }

    return features


def _extract_shap_top5(model, feature_vector: dict, feature_names: list[str]) -> list[dict]:
    """Extract top-5 SHAP features for explainability."""
    try:
        import shap
        import pandas as pd
        X = pd.DataFrame([[feature_vector.get(f, 0.0) for f in feature_names]], columns=feature_names)
        # Use the base classifier if it's a pipeline
        base = getattr(model, "estimator", model)
        explainer = shap.TreeExplainer(base)
        shap_values = explainer.shap_values(X)
        if isinstance(shap_values, list):
            shap_values = shap_values[1]  # class 1 (home win)
        vals = shap_values[0]
        top5_idx = sorted(range(len(vals)), key=lambda i: abs(vals[i]), reverse=True)[:5]
        return [
            {"feature": feature_names[i], "value": round(float(vals[i]), 4), "label": feature_names[i]}
            for i in top5_idx
        ]
    except Exception as e:
        logger.warning("SHAP extraction failed: %s", e)
        return []


# ── Prediction locking ─────────────────────────────────────────────────────────

def lock_prediction(
    game_id: str,
    home_team_id: int,
    away_team_id: int,
    home_team_name: str,
    away_team_name: str,
):
    """Lock a prediction 25 minutes before tip-off."""
    if _classifier is None:
        logger.warning("Classifier not loaded — skipping prediction for %s", game_id)
        return

    logger.info("Locking prediction for game %s (%s vs %s)", game_id, home_team_name, away_team_name)
    try:
        # Fetch live odds at this exact moment
        live_odds = None
        try:
            odds_data = get_todays_odds()
            games_odds = odds_data.get("games", [])
            for g in games_odds:
                if str(g.get("gameId", "")) == str(game_id):
                    markets = g.get("markets", [{}])
                    if markets:
                        m = markets[0]
                        live_odds = {
                            "spread":      m.get("spread", {}).get("homeTeam"),
                            "ml_home":     m.get("moneyLine", {}).get("homeTeam"),
                            "ml_away":     m.get("moneyLine", {}).get("awayTeam"),
                            "over_under":  m.get("total", {}).get("total"),
                        }
                    break
        except Exception as e:
            logger.warning("Could not fetch live odds for %s: %s", game_id, e)

        features = _build_prediction_features(game_id, home_team_id, away_team_id, live_odds)

        feat_names = _selected_features if _selected_features else list(features.keys())
        import pandas as pd
        X = pd.DataFrame([[features.get(f, 0.0) for f in feat_names]], columns=feat_names)

        # Classifier
        home_win_prob = float(_classifier.predict_proba(X)[0][1])
        predicted_winner = home_team_name if home_win_prob >= 0.5 else away_team_name
        if home_win_prob > 0.65 or home_win_prob < 0.35:
            confidence = "HIGH"
        elif home_win_prob > 0.55 or home_win_prob < 0.45:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        # Regressor
        predicted_diff = 0.0
        predicted_home_score = None
        predicted_away_score = None
        if _regressor is not None:
            predicted_diff = float(_regressor.predict(X)[0])
            avg_pts = 113.5
            predicted_home_score = int(round(avg_pts + predicted_diff / 2))
            predicted_away_score = int(round(avg_pts - predicted_diff / 2))

        # SHAP top-5
        top_features = _extract_shap_top5(_classifier, features, feat_names)

        # Upsert to Neon
        sql = """
            INSERT INTO predictions (
                game_id, game_date, home_team_id, away_team_id,
                home_team_name, away_team_name,
                home_win_prob, predicted_winner, confidence,
                predicted_home_score, predicted_away_score, predicted_point_diff,
                model_version, top_features, odds_at_lock, locked_at
            ) VALUES (
                %s, CURRENT_DATE, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s::jsonb, %s::jsonb, NOW()
            )
            ON CONFLICT (game_id) DO UPDATE SET
                home_win_prob = EXCLUDED.home_win_prob,
                predicted_winner = EXCLUDED.predicted_winner,
                confidence = EXCLUDED.confidence,
                predicted_home_score = EXCLUDED.predicted_home_score,
                predicted_away_score = EXCLUDED.predicted_away_score,
                predicted_point_diff = EXCLUDED.predicted_point_diff,
                top_features = EXCLUDED.top_features,
                odds_at_lock = EXCLUDED.odds_at_lock,
                locked_at = EXCLUDED.locked_at
        """
        import json as _json
        execute_write(sql, [
            game_id, home_team_id, away_team_id,
            home_team_name, away_team_name,
            home_win_prob, predicted_winner, confidence,
            predicted_home_score, predicted_away_score, round(predicted_diff, 2),
            "v2.0",
            _json.dumps(top_features),
            _json.dumps(live_odds) if live_odds else "{}",
        ])
        logger.info(
            "Locked: %s → %s %.0f%% | Proj: %s-%s",
            game_id, predicted_winner,
            home_win_prob * 100,
            predicted_home_score, predicted_away_score,
        )

    except Exception as e:
        logger.error("Failed to lock prediction for %s: %s", game_id, e, exc_info=True)


# ── Settlement ─────────────────────────────────────────────────────────────────

def settle_predictions():
    """Settle yesterday's predictions and append drift_log row."""
    logger.info("Running nightly settlement...")
    try:
        from datetime import date, timedelta
        yesterday = date.today() - timedelta(days=1)

        unsettled = execute_query(
            "SELECT * FROM predictions WHERE game_date = %s AND actual_winner IS NULL",
            [yesterday],
        )
        if not unsettled:
            logger.info("No predictions to settle for %s", yesterday)
            return

        settled = 0
        correct = 0
        score_errors = []

        for pred in unsettled:
            game = execute_query(
                "SELECT * FROM games WHERE game_id = %s AND home_score IS NOT NULL",
                [pred["game_id"]],
            )
            if not game:
                continue
            g = game[0]
            actual_winner = g["home_team_name"] if g["home_score"] > g["away_score"] else g["away_team_name"]
            is_correct = actual_winner == pred["predicted_winner"]
            score_error = None
            if pred.get("predicted_point_diff") is not None:
                actual_diff = g["home_score"] - g["away_score"]
                score_error = abs(float(pred["predicted_point_diff"]) - actual_diff)
                score_errors.append(score_error)

            execute_write(
                """UPDATE predictions SET
                    actual_winner = %s, actual_home_score = %s, actual_away_score = %s,
                    correct = %s, score_error = %s
                WHERE game_id = %s""",
                [actual_winner, g["home_score"], g["away_score"],
                 is_correct, score_error, pred["game_id"]],
            )
            settled += 1
            if is_correct:
                correct += 1

        if settled == 0:
            return

        daily_acc = correct / settled

        # Rolling accuracy
        def rolling_acc(days):
            cutoff = date.today() - timedelta(days=days)
            rows = execute_query(
                "SELECT correct FROM predictions WHERE game_date >= %s AND correct IS NOT NULL",
                [cutoff],
            )
            if not rows:
                return None, 0
            n = len(rows)
            c = sum(1 for r in rows if r["correct"])
            return c / n, n

        acc_7d, n7 = rolling_acc(7)
        acc_30d, n30 = rolling_acc(30)

        execute_write(
            """INSERT INTO drift_log (
                log_date, games_settled, daily_accuracy,
                rolling_7d_accuracy, rolling_30d_accuracy,
                rolling_7d_sample, rolling_30d_sample,
                avg_score_error, model_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            [
                yesterday, settled, daily_acc,
                acc_7d, acc_30d, n7, n30,
                round(float(np.mean(score_errors)), 2) if score_errors else None,
                "v2.0",
            ],
        )
        logger.info("Settlement done: %d/%d correct (%.1f%%)", correct, settled, daily_acc * 100)

    except Exception as e:
        logger.error("Settlement failed: %s", e, exc_info=True)


# ── Scheduler ─────────────────────────────────────────────────────────────────

def schedule_todays_predictions():
    """Queue prediction lock jobs for today's games."""
    global _scheduler
    if _scheduler is None:
        return
    logger.info("Scheduling today's predictions...")
    try:
        data = get_todays_scoreboard()
        games = data.get("scoreboard", {}).get("games", [])
        now_utc = datetime.utcnow().replace(tzinfo=None)
        queued = 0

        for g in games:
            game_id = str(g.get("gameId", ""))
            if not game_id:
                continue

            # Parse tip-off time
            tip_utc_str = g.get("gameTimeUTC", "")
            if not tip_utc_str:
                continue
            try:
                from datetime import datetime as dt
                tip_utc = dt.fromisoformat(tip_utc_str.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                continue

            fire_time = tip_utc - timedelta(minutes=25)
            # Only schedule if fire_time is in future (or within last 5 min)
            if fire_time < now_utc - timedelta(minutes=5):
                continue

            home = g.get("homeTeam", {})
            away = g.get("awayTeam", {})
            job_id = f"predict_{game_id}"

            # Remove existing job if present
            try:
                _scheduler.remove_job(job_id)
            except Exception:
                pass

            _scheduler.add_job(
                lock_prediction,
                trigger=DateTrigger(run_date=fire_time),
                id=job_id,
                args=[
                    game_id,
                    int(home.get("teamId", 0)),
                    int(away.get("teamId", 0)),
                    home.get("teamName", ""),
                    away.get("teamName", ""),
                ],
                misfire_grace_time=600,
            )
            queued += 1
            logger.info("Queued prediction for %s at %s UTC", game_id, fire_time.isoformat())

        logger.info("Scheduled %d prediction jobs", queued)

    except Exception as e:
        logger.error("Failed to schedule predictions: %s", e, exc_info=True)


def start_scheduler() -> BackgroundScheduler:
    global _scheduler
    _load_models()

    _scheduler = BackgroundScheduler(timezone="America/New_York")

    # Nightly settlement at midnight ET
    _scheduler.add_job(
        settle_predictions,
        trigger=CronTrigger(hour=0, minute=5, timezone=ET),
        id="nightly_settlement",
        replace_existing=True,
    )

    # Re-schedule predictions every morning at 9 AM ET (catches newly posted schedules)
    _scheduler.add_job(
        schedule_todays_predictions,
        trigger=CronTrigger(hour=9, minute=0, timezone=ET),
        id="morning_schedule",
        replace_existing=True,
    )

    _scheduler.start()
    # Queue today's games immediately on startup
    schedule_todays_predictions()
    logger.info("APScheduler started")
    return _scheduler
