"""
train_v2.py
-----------
NBA ML Pipeline v2 — Stacked Ensemble with Time-Blocked CV.

Architecture:
  4 base models (logreg, rf, xgb, mlp) per feature set
  3-fold OOF stacking on 2020-24 → meta LogisticRegression
  Holdout evaluation on 2024-25
  Winner (Set A vs Set B) retrained on all 5 seasons (4 OOF folds)

Usage:
    # Dry run (saves candidate pkl, doesn't overwrite production)
    python train_v2.py --data-dir data/ --odds-db /path/to/OddsData.sqlite --output-dir models/ --dry-run

    # Promote to production
    python train_v2.py --data-dir data/ --odds-db /path/to/OddsData.sqlite --output-dir models/

    # Force specific feature set
    python train_v2.py ... --set A
    python train_v2.py ... --set B
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone

import sys
import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from feature_sets import SET_A_FEATURES, SET_B_FEATURES, SET_C_FEATURES, FEATURE_SET_NAMES
from feature_schema import FEATURE_METADATA
from odds_loader import load_odds, compute_training_medians, merge_odds

# Import StackingEnsemble from backend/ so the pickle class path matches what
# the backend can deserialize at inference time.
_BACKEND_DIR = os.path.join(os.path.dirname(__file__), "..", "backend")
sys.path.insert(0, os.path.abspath(_BACKEND_DIR))
from stacking_ensemble import StackingEnsemble  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

TRAIN_SEASONS = ["2020-21", "2021-22", "2022-23"]
VAL_SEASON = "2023-24"
HOLDOUT_SEASON = "2024-25"
LIVE_SEASON = "2025-26"

TRAIN_VAL_SEASONS = TRAIN_SEASONS + [VAL_SEASON]
ALL_SEASONS = TRAIN_SEASONS + [VAL_SEASON, HOLDOUT_SEASON]


# ---------------------------------------------------------------------------
# Base model builders
# ---------------------------------------------------------------------------

def _build_base_models() -> dict:
    """Return a dict of unfitted sklearn Pipeline objects, one per base model."""
    return {
        "logreg": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(C=1.0, max_iter=1000, random_state=42)),
        ]),
        "rf": Pipeline([
            ("clf", RandomForestClassifier(
                n_estimators=100,
                max_depth=8,
                min_samples_leaf=5,
                random_state=42,
                n_jobs=1,
            )),
        ]),
        "xgb": Pipeline([
            ("clf", XGBClassifier(
                n_estimators=100,
                max_depth=4,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                eval_metric="logloss",
                random_state=42,
                verbosity=0,
                n_jobs=1,
            )),
        ]),
        "mlp": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", MLPClassifier(
                hidden_layer_sizes=(64, 32, 16),
                alpha=0.001,
                max_iter=300,
                early_stopping=True,
                n_iter_no_change=15,
                random_state=42,
            )),
        ]),
    }


# ---------------------------------------------------------------------------
# Season splitting
# ---------------------------------------------------------------------------

def split_seasons(df: pd.DataFrame) -> dict:
    """Return a dict of DataFrames keyed by split name."""
    return {
        "train": df[df["SEASON"].isin(TRAIN_SEASONS)].copy(),
        "val": df[df["SEASON"] == VAL_SEASON].copy(),
        "holdout": df[df["SEASON"] == HOLDOUT_SEASON].copy(),
        "live": df[df["SEASON"] == LIVE_SEASON].copy(),
        "train_val": df[df["SEASON"].isin(TRAIN_VAL_SEASONS)].copy(),
        "all_5": df[df["SEASON"].isin(ALL_SEASONS)].copy(),
    }


# ---------------------------------------------------------------------------
# OOF stacking
# ---------------------------------------------------------------------------

def generate_oof_predictions(df: pd.DataFrame, feature_cols: list) -> pd.DataFrame:
    """
    3 expanding folds on 2020-24 (train_val data).

    Fold 1: train 2020-21       → OOF predict 2021-22
    Fold 2: train 2020-22       → OOF predict 2022-23
    Fold 3: train 2020-23       → OOF predict 2023-24

    Returns DataFrame indexed like df[seasons 2021-24] with columns
    [p_logreg, p_rf, p_xgb, p_mlp, y_actual].
    """
    fold_configs = [
        (["2020-21"], "2021-22"),
        (["2020-21", "2021-22"], "2022-23"),
        (["2020-21", "2021-22", "2022-23"], "2023-24"),
    ]

    oof_frames = []
    fold_metrics = []

    for fold_num, (train_seasons, oof_season) in enumerate(fold_configs, 1):
        log.info("OOF Fold %d: train=%s → oof=%s", fold_num, train_seasons, oof_season)

        train_mask = df["SEASON"].isin(train_seasons)
        oof_mask = df["SEASON"] == oof_season

        X_tr = df.loc[train_mask, feature_cols].values
        y_tr = df.loc[train_mask, "WIN"].values
        X_oof = df.loc[oof_mask, feature_cols].values
        y_oof = df.loc[oof_mask, "WIN"].values

        models = _build_base_models()
        preds = {}

        for name, pipeline in models.items():
            pipeline.fit(X_tr, y_tr)
            preds[f"p_{name}"] = pipeline.predict_proba(X_oof)[:, 1]

        fold_df = df.loc[oof_mask, ["GAME_DATE", "SEASON", "WIN"]].copy()
        for col, vals in preds.items():
            fold_df[col] = vals
        fold_df["y_actual"] = y_oof
        oof_frames.append(fold_df)

        # Log per-model AUC for this fold
        for name in ["logreg", "rf", "xgb", "mlp"]:
            auc = roc_auc_score(y_oof, preds[f"p_{name}"])
            log.info("  Fold %d  %-8s  AUC=%.4f", fold_num, name, auc)
        fold_metrics.append({"fold": fold_num, "train_seasons": train_seasons, "oof_season": oof_season})

    oof_df = pd.concat(oof_frames, ignore_index=True)
    log.info("OOF collected: %d rows covering 2021-24", len(oof_df))
    return oof_df


def train_meta_learner(oof_df: pd.DataFrame) -> LogisticRegression:
    """Fit meta LogisticRegression on OOF base-model predictions."""
    meta_X = oof_df[["p_logreg", "p_rf", "p_xgb", "p_mlp"]].values
    meta_y = oof_df["y_actual"].values
    meta = LogisticRegression(C=1.0, max_iter=1000, random_state=42)
    meta.fit(meta_X, meta_y)
    log.info("Meta learner fitted on %d OOF rows", len(oof_df))
    return meta


# ---------------------------------------------------------------------------
# Holdout evaluation
# ---------------------------------------------------------------------------

def evaluate_on_holdout(
    train_val_df: pd.DataFrame,
    holdout_df: pd.DataFrame,
    feature_cols: list,
    oof_df: pd.DataFrame,
    set_name: str,
) -> dict:
    """
    Train all 4 base models on 2020-24, predict on 2024-25 holdout.
    Feed to trained meta (from oof_df) → compute metrics.
    """
    log.info("Evaluating Set %s on 2024-25 holdout...", set_name)

    X_train = train_val_df[feature_cols].values
    y_train = train_val_df["WIN"].values
    X_hold = holdout_df[feature_cols].values
    y_hold = holdout_df["WIN"].values

    # Fit all 4 base models on full 2020-24
    fitted_models = {}
    hold_preds = {}

    for name, pipeline in _build_base_models().items():
        pipeline.fit(X_train, y_train)
        fitted_models[name] = pipeline
        hold_preds[f"p_{name}"] = pipeline.predict_proba(X_hold)[:, 1]

    # Train meta on OOF, then predict on holdout
    meta = train_meta_learner(oof_df)
    meta_X_hold = np.column_stack([hold_preds[f"p_{n}"] for n in ["logreg", "rf", "xgb", "mlp"]])
    final_proba = meta.predict_proba(meta_X_hold)[:, 1]
    final_pred = (final_proba >= 0.5).astype(int)

    base_aucs = {
        name: float(roc_auc_score(y_hold, hold_preds[f"p_{name}"]))
        for name in ["logreg", "rf", "xgb", "mlp"]
    }
    for name, auc in base_aucs.items():
        log.info("  %-8s holdout AUC=%.4f", name, auc)

    metrics = {
        "set": set_name,
        "accuracy": float(accuracy_score(y_hold, final_pred)),
        "log_loss": float(log_loss(y_hold, final_proba)),
        "brier_score": float(brier_score_loss(y_hold, final_proba)),
        "auc": float(roc_auc_score(y_hold, final_proba)),
        "base_model_aucs": base_aucs,
        "n_holdout": int(len(y_hold)),
        "n_features": len(feature_cols),
        "feature_cols": feature_cols,
    }

    log.info(
        "Set %s holdout → ACC=%.4f  AUC=%.4f  Brier=%.4f  LogLoss=%.4f",
        set_name, metrics["accuracy"], metrics["auc"],
        metrics["brier_score"], metrics["log_loss"],
    )

    return metrics, fitted_models, meta, final_proba


# ---------------------------------------------------------------------------
# Winner selection
# ---------------------------------------------------------------------------

def pick_winner(results_A: dict, results_B: dict) -> str:
    """
    Prefer Set A unless Set B improves Brier by >0.003 OR log_loss by >0.010.
    Ties always go to Set A (simpler, no odds dependency for live inference).
    """
    brier_improvement = results_A["brier_score"] - results_B["brier_score"]
    logloss_improvement = results_A["log_loss"] - results_B["log_loss"]

    log.info(
        "Set A  Brier=%.4f  LogLoss=%.4f  AUC=%.4f",
        results_A["brier_score"], results_A["log_loss"], results_A["auc"],
    )
    log.info(
        "Set B  Brier=%.4f  LogLoss=%.4f  AUC=%.4f",
        results_B["brier_score"], results_B["log_loss"], results_B["auc"],
    )
    log.info(
        "Improvement B over A → Brier: %.4f  LogLoss: %.4f",
        brier_improvement, logloss_improvement,
    )

    if brier_improvement > 0.003 or logloss_improvement > 0.010:
        reason = f"Set B wins: Brier Δ={brier_improvement:.4f}, LogLoss Δ={logloss_improvement:.4f}"
        log.info(reason)
        return "B", reason
    else:
        reason = (
            f"Set A retained: Brier Δ={brier_improvement:.4f} ≤ 0.003, "
            f"LogLoss Δ={logloss_improvement:.4f} ≤ 0.010"
        )
        log.info(reason)
        return "A", reason


# ---------------------------------------------------------------------------
# Final model training (4 expanding folds on 2020-25)
# ---------------------------------------------------------------------------

def train_final_model(all_5_df: pd.DataFrame, feature_cols: list) -> StackingEnsemble:
    """
    4-fold OOF on 2020-25, then fit all base models on full 5 seasons.
    Meta-learner trained on the 4-fold OOF predictions.
    """
    fold_configs = [
        (["2020-21"], "2021-22"),
        (["2020-21", "2021-22"], "2022-23"),
        (["2020-21", "2021-22", "2022-23"], "2023-24"),
        (["2020-21", "2021-22", "2022-23", "2023-24"], "2024-25"),
    ]

    oof_frames = []

    for fold_num, (train_seasons, oof_season) in enumerate(fold_configs, 1):
        log.info("Final fold %d: train=%s → oof=%s", fold_num, train_seasons, oof_season)

        train_mask = all_5_df["SEASON"].isin(train_seasons)
        oof_mask = all_5_df["SEASON"] == oof_season

        X_tr = all_5_df.loc[train_mask, feature_cols].values
        y_tr = all_5_df.loc[train_mask, "WIN"].values
        X_oof = all_5_df.loc[oof_mask, feature_cols].values
        y_oof = all_5_df.loc[oof_mask, "WIN"].values

        models = _build_base_models()
        preds = {}
        for name, pipeline in models.items():
            pipeline.fit(X_tr, y_tr)
            preds[f"p_{name}"] = pipeline.predict_proba(X_oof)[:, 1]

        fold_df = all_5_df.loc[oof_mask, ["GAME_DATE", "SEASON", "WIN"]].copy()
        for col, vals in preds.items():
            fold_df[col] = vals
        fold_df["y_actual"] = y_oof
        oof_frames.append(fold_df)

    oof_df = pd.concat(oof_frames, ignore_index=True)
    meta = train_meta_learner(oof_df)

    # Fit all base models on ALL 5 seasons for production use
    log.info("Fitting final base models on all 5 seasons (%d rows)...", len(all_5_df))
    X_all = all_5_df[feature_cols].values
    y_all = all_5_df["WIN"].values

    final_base_models = {}
    for name, pipeline in _build_base_models().items():
        pipeline.fit(X_all, y_all)
        final_base_models[name] = pipeline
        log.info("  Final base model fitted: %s", name)

    return StackingEnsemble(
        base_models=final_base_models,
        meta_learner=meta,
        feature_names=feature_cols,
    )


# ---------------------------------------------------------------------------
# Artifact saving
# ---------------------------------------------------------------------------

def save_artifacts(
    model: StackingEnsemble,
    feature_cols: list,
    set_name: str,
    results_A: dict,
    results_B: dict,
    winner_reason: str,
    match_rate: float,
    impute_medians: dict,
    output_dir: str,
    dry_run: bool,
):
    os.makedirs(output_dir, exist_ok=True)

    clf_fname = "classifier_candidate.pkl" if dry_run else "classifier.pkl"
    clf_path = os.path.join(output_dir, clf_fname)
    joblib.dump(model, clf_path)
    log.info("Saved model → %s", clf_path)

    # selected_features.json — preserves keys scheduler.py reads at line 61
    features_list = [
        {
            "feature": f,
            "label": FEATURE_METADATA.get(f, {}).get("label", f),
            "description": FEATURE_METADATA.get(f, {}).get("description", ""),
            "why": FEATURE_METADATA.get(f, {}).get("why", ""),
        }
        for f in feature_cols
    ]
    feat_fname = "selected_features_candidate.json" if dry_run else "selected_features.json"
    feat_path = os.path.join(output_dir, feat_fname)
    with open(feat_path, "w") as f:
        json.dump(
            {
                "selected_features": feature_cols,
                "features": features_list,
                "model_version": "v2.0",
                "feature_set": set_name,
            },
            f,
            indent=2,
        )
    log.info("Saved features → %s", feat_path)

    # classifier_metadata.json
    meta_path = os.path.join(output_dir, "classifier_metadata.json")
    with open(meta_path, "w") as f:
        json.dump(
            {
                "version": "v2.0",
                "feature_set": set_name,
                "n_features": len(feature_cols),
                "feature_cols": feature_cols,
                "holdout_results": {
                    "A": results_A,
                    "B": results_B,
                    "winner": set_name,
                    "reason": winner_reason,
                },
                "training_at": datetime.now(timezone.utc).isoformat(),
            },
            f,
            indent=2,
        )

    # v2_ab_comparison.json
    ab_path = os.path.join(output_dir, "v2_ab_comparison.json")
    with open(ab_path, "w") as f:
        json.dump({"set_A": results_A, "set_B": results_B, "winner": set_name, "reason": winner_reason}, f, indent=2)
    log.info("Saved A/B comparison → %s", ab_path)

    # v2_training_log.json
    log_path = os.path.join(output_dir, "v2_training_log.json")
    with open(log_path, "w") as f:
        json.dump(
            {
                "odds_match_rate": float(match_rate),
                "imputation_medians": impute_medians,
                "winner_set": set_name,
                "winner_reason": winner_reason,
                "training_at": datetime.now(timezone.utc).isoformat(),
                "dry_run": dry_run,
            },
            f,
            indent=2,
        )
    log.info("Saved training log → %s", log_path)

    if dry_run:
        log.info(
            "\n=== DRY RUN complete ===\n"
            "Review %s and %s before promoting to production.\n"
            "Re-run WITHOUT --dry-run to overwrite classifier.pkl and selected_features.json.",
            ab_path,
            log_path,
        )
    else:
        log.info("Production artifacts written. Restart FastAPI to load new model.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="NBA ML Pipeline v2 — train stacked ensemble")
    parser.add_argument("--data-dir", default="data/", help="Directory containing training_data.csv")
    parser.add_argument(
        "--odds-db",
        default="/Users/savannahvo/Downloads/NBA-Machine-Learning-Sports-Betting/Data/OddsData.sqlite",
        help="Path to OddsData.sqlite",
    )
    parser.add_argument("--odds-csv", help="Override path to historical_odds_2020_2025.csv")
    parser.add_argument("--output-dir", default="models/", help="Directory to write artifacts")
    parser.add_argument("--dry-run", action="store_true", help="Save candidate files without overwriting production")
    parser.add_argument("--set", choices=["A", "B", "C", "auto"], default="auto", help="Force feature set or auto-select")
    args = parser.parse_args()

    # --- Load training data ---
    training_csv = os.path.join(args.data_dir, "training_data.csv")
    log.info("Loading training data from %s", training_csv)
    df = pd.read_csv(training_csv)
    log.info("Training data: %d rows, %d cols", len(df), len(df.columns))

    # --- Load odds ---
    hist_csv = args.odds_csv or os.path.join(
        os.path.dirname(args.odds_db),
        "../../nba-ml-retrain/data/build_report/historical_odds_2020_2025.csv",
    )
    # Fall back to the known absolute path if the derived path doesn't exist
    known_hist_csv = "/Users/savannahvo/Downloads/nba-ml-retrain/data/build_report/historical_odds_2020_2025.csv"
    if not os.path.exists(hist_csv) and os.path.exists(known_hist_csv):
        hist_csv = known_hist_csv

    odds_df = load_odds(hist_csv, args.odds_db)

    # --- Compute medians from train seasons BEFORE merging ---
    train_only = df[df["SEASON"].isin(TRAIN_SEASONS)].copy()
    # Temporarily merge to get odds values for training rows
    temp_merged, _ = merge_odds(train_only, odds_df, impute_medians={"SPREAD_DIFF": 0.0, "ML_PROB_DIFF": 0.0, "OVER_UNDER": 220.0})
    impute_medians = compute_training_medians(temp_merged)

    # --- Merge odds into full dataset ---
    enriched_df, match_rate = merge_odds(df, odds_df, impute_medians)
    log.info("Enriched dataset: %d rows", len(enriched_df))

    # --- Season splits ---
    splits = split_seasons(enriched_df)
    train_val_df = splits["train_val"]
    holdout_df = splits["holdout"]
    all_5_df = splits["all_5"]

    log.info(
        "Splits — train+val: %d, holdout: %d, live: %d, all_5: %d",
        len(train_val_df), len(holdout_df), len(splits["live"]), len(all_5_df),
    )

    results = {}
    oof_dfs = {}

    if args.set in ("A", "auto"):
        log.info("\n=== Feature Set A (%d features) ===", len(SET_A_FEATURES))
        oof_dfs["A"] = generate_oof_predictions(train_val_df, SET_A_FEATURES)
        results["A"], _, _, _ = evaluate_on_holdout(
            train_val_df, holdout_df, SET_A_FEATURES, oof_dfs["A"], "A"
        )

    if args.set in ("B", "auto"):
        log.info("\n=== Feature Set B (%d features) ===", len(SET_B_FEATURES))
        oof_dfs["B"] = generate_oof_predictions(train_val_df, SET_B_FEATURES)
        results["B"], _, _, _ = evaluate_on_holdout(
            train_val_df, holdout_df, SET_B_FEATURES, oof_dfs["B"], "B"
        )

    if args.set == "C":
        log.info("\n=== Feature Set C (%d features) ===", len(SET_C_FEATURES))
        oof_dfs["C"] = generate_oof_predictions(train_val_df, SET_C_FEATURES)
        results["C"], _, _, _ = evaluate_on_holdout(
            train_val_df, holdout_df, SET_C_FEATURES, oof_dfs["C"], "C"
        )

    # --- Pick winner ---
    if args.set == "A":
        winner = "A"
        winner_reason = "Forced by --set A flag"
    elif args.set == "B":
        winner = "B"
        winner_reason = "Forced by --set B flag"
    elif args.set == "C":
        winner = "C"
        winner_reason = "Forced by --set C flag"
    else:
        winner, winner_reason = pick_winner(results["A"], results["B"])

    winner_features = FEATURE_SET_NAMES[winner]
    log.info("\nWinner: Set %s (%d features)", winner, len(winner_features))

    # Fill in missing results for single-set runs
    results_A = results.get("A", {})
    results_B = results.get("B", {})

    # --- Train final model on all 5 seasons ---
    log.info("\n=== Training final model (all 5 seasons, 4 OOF folds) ===")
    final_model = train_final_model(all_5_df, winner_features)

    # --- Save artifacts ---
    save_artifacts(
        model=final_model,
        feature_cols=winner_features,
        set_name=winner,
        results_A=results_A,
        results_B=results_B,
        winner_reason=winner_reason,
        match_rate=match_rate,
        impute_medians=impute_medians,
        output_dir=args.output_dir,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
