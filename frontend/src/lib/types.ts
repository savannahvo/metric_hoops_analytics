export interface Game {
  game_id: string
  home_team: string
  away_team: string
  home_team_id: number
  away_team_id: number
  home_tricode: string
  away_tricode: string
  home_score: number
  away_score: number
  status: string
  period: number
  clock: string
  date: string
  arena?: string
  tip_off_utc?: string
}

export interface Prediction {
  id: number
  game_id: string
  game_date: string
  home_team_id: number
  away_team_id: number
  home_team_name: string
  away_team_name: string
  home_win_prob: number
  predicted_winner: string
  confidence: 'HIGH' | 'MEDIUM' | 'LOW'
  predicted_home_score: number
  predicted_away_score: number
  predicted_point_diff: number
  model_version?: string
  top_features?: FeatureBar[]
  odds_at_lock?: { spread?: number; ml_home?: number; ml_away?: number; over_under?: number }
  actual_winner?: string
  actual_home_score?: number
  actual_away_score?: number
  correct?: boolean
  score_error?: number
  locked_at?: string
}

export interface FeatureBar {
  feature: string
  value: number
  label: string
}

export interface TeamStanding {
  team_id: number
  team_name: string
  tricode: string
  conference: 'East' | 'West'
  wins: number
  losses: number
  win_pct: number
  gb: number
  home_record: string
  away_record: string
  last_10: string
  streak: string
  playoff_position: number
}

export interface Player {
  player_id: number
  player_name: string
  team_id: number
  team_name?: string
  position?: string
  pts: number
  reb: number
  ast: number
  stl: number
  blk: number
  tov: number
  fg_pct: number
  fg3_pct: number
  games_played: number
}

export interface PlayerGameLog {
  game_id: string
  game_date: string
  team_id: number
  home: boolean
  win: boolean
  pts: number
  reb: number
  ast: number
  stl: number
  blk: number
  tov: number
  fg_pct: number
  fg3_pct: number
  ft_pct: number
  plus_minus: number
  minutes: number
}

export interface Injury {
  id: number
  snapshot_date: string
  team_name: string
  team_id: number
  player_name: string
  position?: string
  injury_type?: string
  status: string
  updated?: string
  source: 'espn' | 'cbs'
}

export interface ModelFeature {
  feature: string
  label: string
  description: string
  why: string
  shap_importance: number
  perm_importance?: number
}

export interface AccuracyStats {
  season_accuracy: number
  rolling_7d: number
  rolling_30d: number
  total_games: number
  correct_games: number
}

export interface DriftLog {
  id: number
  log_date: string
  games_settled: number
  daily_accuracy: number
  rolling_7d_accuracy: number
  rolling_30d_accuracy: number
  rolling_7d_sample: number
  rolling_30d_sample: number
  avg_score_error: number
  model_version: string
}
