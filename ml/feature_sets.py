"""
feature_sets.py
---------------
Single source of truth for named feature sets used in v2 training.
No logic — only constants.
"""

SET_A_FEATURES = [
    "ELO_DIFF",
    "ROLL10_NET_RTG_DIFF",
    "ROLL10_EFG_DIFF",
    "TOV_RATE_DIFF",
    "IS_B2B_DIFF",
    "OREB_PCT_DIFF",
    "STREAK_DIFF",
    "EFG_PCT_DIFF",
    "ROLL10_PTS_DIFF",
]

SET_B_FEATURES = SET_A_FEATURES + [
    "OFF_RTG_DIFF",
    "DEF_RTG_DIFF",
    "DAYS_REST_DIFF",
    "SPREAD_DIFF",
    "ML_PROB_DIFF",
    "OVER_UNDER",
    "IS_PLAYOFF",
]

# Full playoff-aware feature set:
# All regular-season features + IS_PLAYOFF flag + playoff-specific series features.
# Playoff features are set to 0 for regular season games so the model can learn
# different patterns when IS_PLAYOFF = 1.
SET_C_FEATURES = [
    # Team efficiency (season-to-date)
    "OFF_RTG_DIFF", "DEF_RTG_DIFF", "EFG_PCT_DIFF", "TS_PCT_DIFF",
    "OREB_PCT_DIFF", "DREB_PCT_DIFF", "TOV_RATE_DIFF", "FT_RATE_DIFF", "FG3_RATE_DIFF",
    # Rolling form (last 10 games)
    "ROLL10_WIN_PCT_DIFF", "ROLL10_NET_RTG_DIFF", "ROLL10_PTS_DIFF",
    "ROLL10_EFG_DIFF", "ROLL10_TOV_DIFF",
    # Schedule / fatigue
    "DAYS_REST_DIFF", "IS_B2B_DIFF", "GAMES_LAST_7_DIFF",
    # Player
    "INJURY_IMPACT_DIFF", "STAR_AVAILABLE_DIFF", "TOP3_PPG_DIFF", "TOP5_PM_DIFF",
    # Context
    "HOME_COURT", "ELO_DIFF", "WIN_PCT_DIFF", "STREAK_DIFF", "SEASON_PROGRESS",
    # Odds
    "SPREAD_DIFF", "ML_PROB_DIFF", "OVER_UNDER",
    # Playoff context flag
    "IS_PLAYOFF",
    # Playoff-specific series features (0 for regular season)
    "SERIES_GAME_NUM", "SERIES_LEAD_DIFF", "IS_ELIMINATION_GAME", "CAN_CLINCH_SERIES",
    "SERIES_PTS_DIFF", "SERIES_EFG_DIFF", "PLAYOFF_GAMES_PLAYED_DIFF",
    "ROLL_PLAYOFF_WIN_PCT_DIFF",
]

FEATURE_SET_NAMES = {
    "A": SET_A_FEATURES,
    "B": SET_B_FEATURES,
    "C": SET_C_FEATURES,
}
