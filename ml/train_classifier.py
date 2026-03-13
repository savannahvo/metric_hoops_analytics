"""
train_classifier.py
-------------------
Train the stacked ensemble classifier (win/loss prediction).

Architecture:
  XGBClassifier  ─┐
                  ├→ LogisticRegression → IsotonicCalibration → classifier.pkl
  RandomForest   ─┘

Usage:
    python train_classifier.py
    python train_classifier.py --data-dir data/
"""

import os
import json
import logging
import argparse
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, StackingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBClassifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def walk_forward_cv(X, y, n_splits=5):
    """Walk-forward cross-validation using TimeSeriesSplit."""
    tscv = TimeSeriesSplit(n_splits=n_splits)
    fold_aucs = []
    fold_accs = []

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model = _build_stacked(X_tr, y_tr)
        proba = model.predict_proba(X_val)[:, 1]
        pred  = (proba >= 0.5).astype(int)
        auc   = roc_auc_score(y_val, proba)
        acc   = accuracy_score(y_val, pred)
        fold_aucs.append(auc)
        fold_accs.append(acc)
        log.info("  Fold %d: AUC=%.4f  ACC=%.4f", fold + 1, auc, acc)

    return fold_aucs, fold_accs


def _build_stacked(X_train, y_train) -> CalibratedClassifierCV:
    """Build and fit the stacked ensemble."""
    xgb = XGBClassifier(
        n_estimators=150,       # reduced from 300 to save RAM
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        random_state=42,
        verbosity=0,
    )
    rf = RandomForestClassifier(
        n_estimators=150,       # reduced from 300 to save RAM
        max_depth=8,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=1,               # single-threaded to cap RAM usage
    )
    meta = LogisticRegression(C=1.0, max_iter=1000, random_state=42)

    stacked = StackingClassifier(
        estimators=[("xgb", xgb), ("rf", rf)],
        final_estimator=meta,
        cv=3,                   # reduced from 5 to save RAM
        passthrough=False,
        n_jobs=1,               # single-threaded to cap RAM usage
    )
    # CalibratedClassifierCV with cv=3 fits + calibrates in one step (RAM-friendly).
    # "prefit" was removed in newer sklearn; use cv=3 isotonic instead.
    calibrated = CalibratedClassifierCV(stacked, cv=3, method="isotonic")
    calibrated.fit(X_train, y_train)
    return calibrated


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "data"))
    parser.add_argument("--skip-cv", action="store_true", help="Skip walk-forward CV (faster)")
    args = parser.parse_args()

    training_path = os.path.join(args.data_dir, "training_data.csv")
    feat_path     = os.path.join(os.path.dirname(__file__), "models", "selected_features.json")

    if not os.path.exists(training_path):
        log.error("training_data.csv not found — run feature_engineering.py first")
        raise SystemExit(1)

    df = pd.read_csv(training_path)
    # Normalize column names: dataset uses uppercase (GAME_DATE, SEASON)
    date_col   = next((c for c in ("GAME_DATE", "game_date", "gameDateTimeEst") if c in df.columns), None)
    season_col = next((c for c in ("SEASON", "season", "seasonYear") if c in df.columns), None)
    if date_col:
        df = df.sort_values(date_col)
    log.info("Loaded %d games", len(df))

    # Feature list
    if os.path.exists(feat_path):
        with open(feat_path) as f:
            feat_data = json.load(f)
        features = feat_data.get("selected_features", feat_data.get("features", []))
        if features and isinstance(features[0], dict):
            features = [f["feature"] for f in features]
        log.info("Using %d selected features from select_features.json", len(features))
    else:
        from feature_schema import FEATURES
        features = [f for f in FEATURES if f in df.columns]
        log.warning("selected_features.json not found — using all %d available features", len(features))

    available = [f for f in features if f in df.columns]
    X = df[available].fillna(0.0)
    y = df["WIN"].astype(int)

    # Train/test split
    if season_col:
        train_mask = ~df[season_col].isin(["2025-26"])
        test_mask  = df[season_col] == "2025-26"
        # Fall back if no 2025-26 data yet
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
    log.info("Train: %d | Test: %d", len(X_train), len(X_test))

    # Walk-forward CV
    cv_aucs, cv_accs = [], []
    if not args.skip_cv:
        log.info("Running walk-forward CV (5 folds)...")
        cv_aucs, cv_accs = walk_forward_cv(X_train, y_train, n_splits=5)
        log.info("CV AUC: %.4f ± %.4f", np.mean(cv_aucs), np.std(cv_aucs))
        log.info("CV ACC: %.4f ± %.4f", np.mean(cv_accs), np.std(cv_accs))

    # Final model — train on all training data
    log.info("Training final stacked ensemble...")
    model = _build_stacked(X_train, y_train)

    # Evaluate on test set
    proba = model.predict_proba(X_test)[:, 1]
    pred  = (proba >= 0.5).astype(int)
    test_acc = accuracy_score(y_test, pred)
    test_auc = roc_auc_score(y_test, proba)
    train_acc = accuracy_score(y_train, (model.predict_proba(X_train)[:, 1] >= 0.5))

    log.info("\n%s", "=" * 50)
    log.info("Train accuracy: %.4f", train_acc)
    log.info("Test  accuracy: %.4f", test_acc)
    log.info("Test  AUC-ROC:  %.4f", test_auc)
    log.info("\n%s", classification_report(y_test, pred, target_names=["Away Win", "Home Win"]))

    # Save model
    models_dir = os.path.join(os.path.dirname(__file__), "models")
    os.makedirs(models_dir, exist_ok=True)
    model_path = os.path.join(models_dir, "classifier.pkl")
    joblib.dump(model, model_path)
    log.info("Model saved → %s", model_path)

    # Save metadata
    metadata = {
        "version":        "v1.0",
        "trained_at":     datetime.utcnow().isoformat(),
        "features":       available,
        "n_features":     len(available),
        "train_size":     int(len(X_train)),
        "test_size":      int(len(X_test)),
        "train_accuracy": round(train_acc, 4),
        "test_accuracy":  round(test_acc, 4),
        "auc":            round(test_auc, 4),
        "cv_aucs":        [round(a, 4) for a in cv_aucs],
        "cv_accs":        [round(a, 4) for a in cv_accs],
        "cv_auc_mean":    round(float(np.mean(cv_aucs)), 4) if cv_aucs else None,
        "architecture":   "StackingClassifier(XGB+RF → LogReg) + IsotonicCalibration",
    }
    meta_path = os.path.join(models_dir, "classifier_metadata.json")
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)
    log.info("Metadata saved → %s", meta_path)


if __name__ == "__main__":
    main()
