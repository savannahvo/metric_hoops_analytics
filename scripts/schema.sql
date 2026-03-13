-- Game results (from Kaggle)
CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    game_date DATE NOT NULL,
    season VARCHAR(10),
    home_team_id INT, away_team_id INT,
    home_team_name TEXT, away_team_name TEXT,
    home_score INT, away_score INT,
    winner TEXT, game_type TEXT,
    arena TEXT, attendance INT,
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_season ON games(season);

-- Team per-game box scores (from Kaggle TeamStatistics.csv)
CREATE TABLE IF NOT EXISTS team_game_stats (
    id SERIAL PRIMARY KEY,
    game_id TEXT REFERENCES games(game_id),
    team_id INT, team_name TEXT,
    opponent_team_id INT, home BOOLEAN, win BOOLEAN,
    pts INT, opp_pts INT,
    fg_pct NUMERIC(5,4), fg3_pct NUMERIC(5,4), ft_pct NUMERIC(5,4),
    reb INT, reb_off INT, reb_def INT,
    ast INT, stl INT, blk INT, tov INT, fouls INT,
    pts_paint INT, pts_fast_break INT, pts_bench INT,
    pts_from_tov INT, pts_second_chance INT,
    plus_minus NUMERIC(6,2),
    season_wins INT, season_losses INT,
    UNIQUE(game_id, team_id)
);
CREATE INDEX IF NOT EXISTS idx_tgs_team ON team_game_stats(team_id);
CREATE INDEX IF NOT EXISTS idx_tgs_game ON team_game_stats(game_id);

-- Player per-game stats (2020-21 onward only)
CREATE TABLE IF NOT EXISTS player_game_stats (
    id SERIAL PRIMARY KEY,
    game_id TEXT REFERENCES games(game_id),
    player_id INT, player_name TEXT,
    team_id INT, home BOOLEAN, win BOOLEAN,
    pts INT, reb INT, ast INT, stl INT, blk INT, tov INT,
    fg_pct NUMERIC(5,4), fg3_pct NUMERIC(5,4), ft_pct NUMERIC(5,4),
    plus_minus NUMERIC(6,2), minutes NUMERIC(5,2),
    UNIQUE(game_id, player_id)
);
CREATE INDEX IF NOT EXISTS idx_pgs_player ON player_game_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_pgs_team ON player_game_stats(team_id);
CREATE INDEX IF NOT EXISTS idx_pgs_game ON player_game_stats(game_id);

-- Historical odds (closing lines from SBR)
CREATE TABLE IF NOT EXISTS game_odds (
    id SERIAL PRIMARY KEY,
    game_id TEXT UNIQUE, game_date DATE,
    home_team TEXT, away_team TEXT,
    spread NUMERIC(5,2),
    ml_home INT, ml_away INT,
    over_under NUMERIC(5,2),
    source VARCHAR(20) DEFAULT 'sbr',
    created_at TIMESTAMP DEFAULT NOW()
);

-- ML predictions (locked by APScheduler 25 min before tip-off)
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    game_id TEXT UNIQUE, game_date DATE NOT NULL,
    home_team_id INT, away_team_id INT,
    home_team_name TEXT, away_team_name TEXT,
    home_win_prob NUMERIC(5,4),
    predicted_winner TEXT,
    confidence VARCHAR(10),
    predicted_home_score INT,
    predicted_away_score INT,
    predicted_point_diff NUMERIC(5,2),
    model_version VARCHAR(20),
    top_features JSONB,
    odds_at_lock JSONB,
    actual_winner TEXT,
    actual_home_score INT, actual_away_score INT,
    correct BOOLEAN,
    score_error NUMERIC(5,2),
    locked_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pred_date ON predictions(game_date);

-- Injury snapshots (daily from ESPN + CBS Sports)
CREATE TABLE IF NOT EXISTS injuries (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    team_name TEXT, team_id INT,
    player_name TEXT, player_id INT,
    position VARCHAR(10), injury_type TEXT,
    status VARCHAR(30),
    updated TEXT, source VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_inj_date ON injuries(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_inj_team ON injuries(team_id);

-- Drift log (appended nightly after settlement)
CREATE TABLE IF NOT EXISTS drift_log (
    id SERIAL PRIMARY KEY,
    log_date DATE NOT NULL,
    games_settled INT,
    daily_accuracy NUMERIC(5,4),
    rolling_7d_accuracy NUMERIC(5,4),
    rolling_30d_accuracy NUMERIC(5,4),
    rolling_7d_sample INT,
    rolling_30d_sample INT,
    avg_score_error NUMERIC(5,2),
    model_version VARCHAR(20),
    retrain_triggered BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);
