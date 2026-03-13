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
]

FEATURE_SET_NAMES = {
    "A": SET_A_FEATURES,
    "B": SET_B_FEATURES,
}
