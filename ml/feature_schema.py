"""
feature_schema.py
-----------------
Single source of truth for all model features.

All features are expressed as home-minus-away differentials (positive = home advantage)
unless otherwise noted in FEATURE_METADATA.

Import this module anywhere that needs the canonical feature list:
    from feature_schema import FEATURES, FEATURES_NO_ODDS, FEATURE_METADATA
"""

# All features as home-away differentials unless noted
FEATURES = [
    # Team efficiency (season-to-date)
    "OFF_RTG_DIFF",
    "DEF_RTG_DIFF",
    "EFG_PCT_DIFF",
    "TS_PCT_DIFF",
    "OREB_PCT_DIFF",
    "DREB_PCT_DIFF",
    "TOV_RATE_DIFF",
    "FT_RATE_DIFF",
    "FG3_RATE_DIFF",
    # Rolling form (last 10 games)
    "ROLL10_WIN_PCT_DIFF",
    "ROLL10_NET_RTG_DIFF",
    "ROLL10_PTS_DIFF",
    "ROLL10_EFG_DIFF",
    "ROLL10_TOV_DIFF",
    # Schedule/fatigue
    "DAYS_REST_DIFF",
    "IS_B2B_DIFF",
    "GAMES_LAST_7_DIFF",
    # Player-specific
    "INJURY_IMPACT_DIFF",
    "STAR_AVAILABLE_DIFF",
    "TOP3_PPG_DIFF",
    "TOP5_PM_DIFF",
    # Context
    "HOME_COURT",
    "ELO_DIFF",
    "WIN_PCT_DIFF",
    "STREAK_DIFF",
    "SEASON_PROGRESS",
    "IS_PLAYOFF",
    # Odds (NaN-imputed when not available)
    "SPREAD_DIFF",
    "ML_PROB_DIFF",
    "OVER_UNDER",
    # Playoff-specific (0 for regular season games)
    "SERIES_GAME_NUM",
    "SERIES_LEAD_DIFF",
    "IS_ELIMINATION_GAME",
    "CAN_CLINCH_SERIES",
    "SERIES_PTS_DIFF",
    "SERIES_EFG_DIFF",
    "PLAYOFF_GAMES_PLAYED_DIFF",
    "ROLL_PLAYOFF_WIN_PCT_DIFF",
]

FEATURES_NO_ODDS = [f for f in FEATURES if f not in ("SPREAD_DIFF", "ML_PROB_DIFF", "OVER_UNDER")]

# Human-readable labels and descriptions for the Model tab UI
FEATURE_METADATA = {
    "OFF_RTG_DIFF": {
        "label": "Offensive Rating Diff",
        "description": "Points scored per 100 possessions (home minus away). Higher = more efficient offense.",
        "why": "Best single measure of team offensive quality."
    },
    "DEF_RTG_DIFF": {
        "label": "Defensive Rating Diff",
        "description": "Points allowed per 100 possessions (home minus away). Lower opponent score = better defense.",
        "why": "Defense wins championships — and games."
    },
    "EFG_PCT_DIFF": {
        "label": "Effective FG% Diff",
        "description": "Accounts for the extra value of 3-pointers: (FGM + 0.5×3PM) / FGA.",
        "why": "Better shot quality than raw FG% — captures 3-point era efficiently."
    },
    "TS_PCT_DIFF": {
        "label": "True Shooting % Diff",
        "description": "Points per shooting attempt including free throws: PTS / (2 × (FGA + 0.44×FTA)).",
        "why": "Most complete measure of scoring efficiency."
    },
    "OREB_PCT_DIFF": {
        "label": "Off Rebound % Diff",
        "description": "Percentage of available offensive rebounds a team collects.",
        "why": "Second-chance points create major scoring advantages."
    },
    "DREB_PCT_DIFF": {
        "label": "Def Rebound % Diff",
        "description": "Percentage of available defensive rebounds a team collects.",
        "why": "Prevents opponent second chances."
    },
    "TOV_RATE_DIFF": {
        "label": "Turnover Rate Diff",
        "description": "Turnovers per 100 possessions.",
        "why": "Turnovers directly gift the opponent easy scoring opportunities."
    },
    "FT_RATE_DIFF": {
        "label": "Free Throw Rate Diff",
        "description": "FTA / FGA — how often a team gets to the line.",
        "why": "Getting to the line is a sustainable scoring advantage."
    },
    "FG3_RATE_DIFF": {
        "label": "3-Point Rate Diff",
        "description": "3PA / FGA — share of shots that are 3-pointers.",
        "why": "3-point volume drives variance and scoring upside."
    },
    "ROLL10_WIN_PCT_DIFF": {
        "label": "Last 10 Win % Diff",
        "description": "Win percentage over the last 10 games (home minus away).",
        "why": "Recent form better predicts performance than season average."
    },
    "ROLL10_NET_RTG_DIFF": {
        "label": "Last 10 Net Rating Diff",
        "description": "Net rating over last 10 games.",
        "why": "Rolling efficiency captures hot/cold streaks."
    },
    "ROLL10_PTS_DIFF": {
        "label": "Last 10 PPG Diff",
        "description": "Average points scored over last 10 games.",
        "why": "Recent scoring output is a strong momentum signal."
    },
    "ROLL10_EFG_DIFF": {
        "label": "Last 10 eFG% Diff",
        "description": "Effective FG% over the last 10 games.",
        "why": "Shooting trends matter more than season averages in-season."
    },
    "ROLL10_TOV_DIFF": {
        "label": "Last 10 TOV Rate Diff",
        "description": "Turnover rate over the last 10 games.",
        "why": "Ball security is often a sign of team chemistry and coaching adjustments."
    },
    "DAYS_REST_DIFF": {
        "label": "Days Rest Diff",
        "description": "Days since last game (home minus away). Positive = home team more rested.",
        "why": "Rest advantage is a proven predictor of game outcome."
    },
    "IS_B2B_DIFF": {
        "label": "Back-to-Back Diff",
        "description": "1 if home team on B2B but away is not, -1 if opposite, 0 if same.",
        "why": "Playing on 0 days rest significantly impairs performance."
    },
    "GAMES_LAST_7_DIFF": {
        "label": "Games in Last 7 Days Diff",
        "description": "Number of games played in the last 7 days (home minus away).",
        "why": "Schedule compression leads to fatigue and injury risk."
    },
    "INJURY_IMPACT_DIFF": {
        "label": "Injury Impact Diff",
        "description": "% of team's season PPG currently injured or out (away minus home — positive = away team more hurt).",
        "why": "Injuries to key players are the biggest variable the model can't fully see."
    },
    "STAR_AVAILABLE_DIFF": {
        "label": "Star Player Available Diff",
        "description": "Is the top scorer active? (1/0 differential).",
        "why": "Elite players have outsized win probability impact."
    },
    "TOP3_PPG_DIFF": {
        "label": "Top 3 Scorers PPG Diff",
        "description": "Combined PPG of the top 3 available scorers per team.",
        "why": "Offensive depth proxy — does the team have multiple reliable scorers?"
    },
    "TOP5_PM_DIFF": {
        "label": "Top 5 +/- Diff",
        "description": "Combined plus-minus of top 5 players by minutes (home minus away).",
        "why": "Best available proxy for lineup quality without needing lineup data."
    },
    "HOME_COURT": {
        "label": "Home Court",
        "description": "Always 1.0 for home team. Home teams win ~57% of NBA games.",
        "why": "The single most consistent advantage in basketball."
    },
    "ELO_DIFF": {
        "label": "Elo Rating Diff",
        "description": "Difference in Elo ratings (home minus away). Updated after every settled game.",
        "why": "Elo captures long-run team quality better than win percentage."
    },
    "WIN_PCT_DIFF": {
        "label": "Win % Diff",
        "description": "Season win percentage (home minus away).",
        "why": "Simple but powerful baseline measure of team quality."
    },
    "STREAK_DIFF": {
        "label": "Streak Diff",
        "description": "Current winning/losing streak (positive = win streak, negative = losing streak). Home minus away.",
        "why": "Teams on winning streaks often have momentum and confidence."
    },
    "SEASON_PROGRESS": {
        "label": "Season Progress",
        "description": "Games played / 82. Scales from 0 (start) to 1 (end of regular season).",
        "why": "Team quality estimates are more reliable later in the season."
    },
    "IS_PLAYOFF": {
        "label": "Playoff Game",
        "description": "1 if this is a playoff game, 0 for regular season.",
        "why": "Playoff games have different dynamics — home court advantage amplifies."
    },
    "SPREAD_DIFF": {
        "label": "Vegas Spread",
        "description": "Vegas point spread from home team's perspective (e.g., -4.5 = home favored by 4.5).",
        "why": "Markets aggregate enormous information — spread is the single strongest predictor."
    },
    "ML_PROB_DIFF": {
        "label": "Moneyline Implied Probability",
        "description": "Home team win probability implied by the moneyline odds.",
        "why": "Moneyline markets are even more efficient than spreads for win/loss prediction."
    },
    "OVER_UNDER": {
        "label": "Over/Under",
        "description": "Total points line set by oddsmakers.",
        "why": "Game total reflects pace and offensive expectations."
    },
    "SERIES_GAME_NUM": {
        "label": "Series Game #",
        "description": "Which game of this playoff series (1–7). 0 for regular season.",
        "why": "Later games in a series carry more pressure and predictability shifts.",
    },
    "SERIES_LEAD_DIFF": {
        "label": "Series Lead Diff",
        "description": "Home team's series wins minus away team's series wins (-3 to +3). 0 for regular season.",
        "why": "Teams leading a series have a psychological and strategic edge.",
    },
    "IS_ELIMINATION_GAME": {
        "label": "Elimination Game",
        "description": "1 if the home team faces elimination (away leads 3-0 to 3-2). 0 otherwise.",
        "why": "Teams playing to stay alive often perform differently than favorites.",
    },
    "CAN_CLINCH_SERIES": {
        "label": "Can Clinch",
        "description": "1 if the home team can win the series with a win (leads 3-0 to 3-2). 0 otherwise.",
        "why": "Closing-out teams have momentum but can sometimes let up.",
    },
    "SERIES_PTS_DIFF": {
        "label": "Series Avg Point Diff",
        "description": "Average point differential for the home team in this series so far. 0 for regular season.",
        "why": "Dominance in the series thus far is a strong predictor of the next game.",
    },
    "SERIES_EFG_DIFF": {
        "label": "Series eFG% Diff",
        "description": "Average effective FG% differential for home team in this series. 0 for regular season.",
        "why": "Shooting edge in a series often persists due to defensive schemes.",
    },
    "PLAYOFF_GAMES_PLAYED_DIFF": {
        "label": "Playoff Games Played Diff",
        "description": "Home team's total playoff games this season minus away team's. 0 for regular season.",
        "why": "Teams in longer series accumulate more fatigue than opponents with sweeps.",
    },
    "ROLL_PLAYOFF_WIN_PCT_DIFF": {
        "label": "Rolling Playoff Win % Diff",
        "description": "Win % over last 5 playoff games (home minus away). 0 for regular season.",
        "why": "Playoff momentum — teams playing well recently often continue that form.",
    },
}

# Convenience: mapping from feature name to human-readable label (for SHAP drivers UI)
FEATURE_LABELS = {k: v["label"] for k, v in FEATURE_METADATA.items()}
