"""
train_regressor.py
------------------
Train the XGBoost point differential regressor.

Output: predicted_diff (home - away)
→ Convert to scores at inference:
    home_score = AVG_PTS + diff/2
    away_score = AVG_PTS - diff/2

Usage:
    python train_regressor.py
    python train_regressor.py --data-dir data/
"""

import os
import json
import logging
import argparse
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from xgboost import XGBRegressor

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

AVG_TOTAL_PTS = 113.5  # League average per team 2024-25


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "data"))
    args = parser.parse_args()

    training_path = os.path.join(args.data_dir, "training_data.csv")
    feat_path     = os.path.join(os.path.dirname(__file__), "models", "selected_features.json")

    if not os.path.exists(training_path):
        log.error("training_data.csv not found — run feature_engineering.py first")
        raise SystemExit(1)

    df = pd.read_csv(training_path)
    date_col   = next((c for c in ("GAME_DATE", "game_date", "gameDateTimeEst") if c in df.columns), None)
    season_col = next((c for c in ("SEASON", "season", "seasonYear") if c in df.columns), None)
    if date_col:
        df = df.sort_values(date_col)
    log.info("Loaded %d games", len(df))

    # Build point differential target (handles both upper and lower case column names)
    home_score_col = next((c for c in ("HOME_SCORE", "home_score", "homeScore") if c in df.columns), None)
    away_score_col = next((c for c in ("AWAY_SCORE", "away_score", "awayScore") if c in df.columns), None)
    if home_score_col and away_score_col:
        df["POINT_DIFF"] = df[home_score_col] - df[away_score_col]
    elif "POINT_DIFF" not in df.columns:
        log.error("Need home_score + away_score or POINT_DIFF column in training_data.csv")
        raise SystemExit(1)

    df = df.dropna(subset=["POINT_DIFF"])

    # Feature list
    if os.path.exists(feat_path):
        with open(feat_path) as f:
            feat_data = json.load(f)
        features = feat_data.get("selected_features", feat_data.get("features", []))
        if features and isinstance(features[0], dict):
            features = [f["feature"] for f in features]
        log.info("Using %d selected features", len(features))
    else:
        from feature_schema import FEATURES
        features = [f for f in FEATURES if f in df.columns]
        log.warning("selected_features.json not found — using all %d available features", len(features))

    available = [f for f in features if f in df.columns]
    X = df[available].fillna(0.0)
    y = df["POINT_DIFF"]

    # Train/test split
    if season_col:
        train_mask = ~df[season_col].isin(["2025-26"])
        test_mask  = df[season_col] == "2025-26"
        if test_mask.sum() == 0:
            split = int(len(df) * 0.85)
            train_mask = pd.Series([True] * split + [False] * (len(df) - split), index=df.index)
            test_mask  = ~train_mask
    else:
        split = int(len(df) * 0.85)
        train_mask = pd.Series([True] * split + [False] * (len(df) - split), index=df.index)
        test_mask  = ~train_mask

    X_train, y_train = X[train_mask], y[train_mask]
    X_test,  y_test  = X[test_mask],  y[test_mask]
    log.info("Train: %d | Test: %d | Avg diff: %.2f ± %.2f",
             len(X_train), len(X_test), y.mean(), y.std())

    # Train
    model = XGBRegressor(
        n_estimators=150,   # reduced from 300 to save RAM
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=0,
    )
    model.fit(X_train, y_train)

    # Evaluate
    pred_train = model.predict(X_train)
    pred_test  = model.predict(X_test)

    rmse_train = np.sqrt(mean_squared_error(y_train, pred_train))
    rmse_test  = np.sqrt(mean_squared_error(y_test,  pred_test))
    mae_test   = mean_absolute_error(y_test, pred_test)
    r2_test    = r2_score(y_test, pred_test)

    log.info("\n%s", "=" * 50)
    log.info("Train RMSE: %.2f pts", rmse_train)
    log.info("Test  RMSE: %.2f pts", rmse_test)
    log.info("Test  MAE:  %.2f pts", mae_test)
    log.info("Test  R²:   %.4f",     r2_test)
    log.info("\nNote: Predicted scores = %.1f ± diff/2", AVG_TOTAL_PTS)

    # Feature importance
    feat_imp = dict(zip(available, model.feature_importances_))
    top10 = sorted(feat_imp.items(), key=lambda x: -x[1])[:10]
    log.info("\nTop-10 features by importance:")
    for feat, imp in top10:
        log.info("  %-30s %.4f", feat, imp)

    # Save model
    models_dir = os.path.join(os.path.dirname(__file__), "models")
    os.makedirs(models_dir, exist_ok=True)
    model_path = os.path.join(models_dir, "regressor.pkl")
    joblib.dump(model, model_path)
    log.info("\nModel saved → %s", model_path)

    # Save metadata
    metadata = {
        "version":       "v1.0",
        "trained_at":    datetime.utcnow().isoformat(),
        "features":      available,
        "n_features":    len(available),
        "train_size":    int(len(X_train)),
        "test_size":     int(len(X_test)),
        "train_rmse":    round(rmse_train, 3),
        "test_rmse":     round(rmse_test, 3),
        "test_mae":      round(mae_test, 3),
        "test_r2":       round(r2_test, 4),
        "avg_team_pts":  AVG_TOTAL_PTS,
        "architecture":  "XGBRegressor(n_estimators=300, max_depth=4, lr=0.05)",
    }
    meta_path = os.path.join(models_dir, "regressor_metadata.json")
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)
    log.info("Metadata saved → %s", meta_path)


if __name__ == "__main__":
    main()
