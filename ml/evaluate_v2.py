"""
evaluate_v2.py
--------------
Generate evaluation plots and reports after train_v2.py completes.

Usage:
    python evaluate_v2.py \
        --data-dir data/ \
        --odds-db /path/to/OddsData.sqlite \
        --models-dir models/
"""

import argparse
import json
import logging
import os

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "backend")))
from stacking_ensemble import StackingEnsemble  # noqa: E402

from odds_loader import load_odds, compute_training_medians, merge_odds
from train_v2 import (
    TRAIN_SEASONS, VAL_SEASON, HOLDOUT_SEASON, LIVE_SEASON, TRAIN_VAL_SEASONS,
    split_seasons,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def _load_enriched(data_dir: str, odds_db: str) -> pd.DataFrame:
    """Load training data and merge odds (mirrors train_v2.py logic)."""
    training_csv = os.path.join(data_dir, "training_data.csv")
    df = pd.read_csv(training_csv)

    known_hist_csv = "/Users/savannahvo/Downloads/nba-ml-retrain/data/build_report/historical_odds_2020_2025.csv"
    odds_df = load_odds(known_hist_csv, odds_db)

    train_only = df[df["SEASON"].isin(TRAIN_SEASONS)].copy()
    temp_merged, _ = merge_odds(train_only, odds_df, {"SPREAD_DIFF": 0.0, "ML_PROB_DIFF": 0.0, "OVER_UNDER": 220.0})
    impute_medians = compute_training_medians(temp_merged)

    enriched_df, _ = merge_odds(df, odds_df, impute_medians)
    return enriched_df


def _get_holdout_proba(model: StackingEnsemble, holdout_df: pd.DataFrame) -> np.ndarray:
    return model.predict_proba(holdout_df)[:, 1]


def plot_calibration_curves(
    holdout_df: pd.DataFrame,
    final_proba: np.ndarray,
    output_dir: str,
):
    """Reliability curves for the final ensemble on 2024-25 holdout."""
    y_true = holdout_df["WIN"].values

    fig, ax = plt.subplots(figsize=(8, 6))
    prob_true, prob_pred = calibration_curve(y_true, final_proba, n_bins=10)
    ax.plot(prob_pred, prob_true, marker="o", label="Ensemble v2", color="royalblue")
    ax.plot([0, 1], [0, 1], linestyle="--", color="gray", label="Perfect calibration")
    ax.set_xlabel("Mean predicted probability")
    ax.set_ylabel("Fraction of positives")
    ax.set_title("Calibration Curve — 2024-25 Holdout")
    ax.legend()
    ax.grid(True, alpha=0.3)

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "v2_calibration.png")
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved calibration plot → %s", out_path)


def plot_accuracy_by_month(
    holdout_df: pd.DataFrame,
    final_proba: np.ndarray,
    output_dir: str,
):
    """Monthly accuracy on 2024-25 holdout."""
    df = holdout_df.copy()
    df["prob"] = final_proba
    df["pred"] = (df["prob"] >= 0.5).astype(int)
    df["correct"] = (df["pred"] == df["WIN"]).astype(int)
    df["month"] = pd.to_datetime(df["GAME_DATE"]).dt.to_period("M").astype(str)

    monthly = df.groupby("month")["correct"].mean()

    fig, ax = plt.subplots(figsize=(10, 5))
    monthly.plot(kind="bar", ax=ax, color="steelblue", edgecolor="white")
    ax.axhline(monthly.mean(), color="red", linestyle="--", label=f"Overall: {monthly.mean():.1%}")
    ax.set_xlabel("Month")
    ax.set_ylabel("Accuracy")
    ax.set_title("Monthly Accuracy — 2024-25 Holdout")
    ax.legend()
    ax.set_ylim(0, 1)
    plt.xticks(rotation=45)

    out_path = os.path.join(output_dir, "v2_monthly_accuracy.png")
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved monthly accuracy plot → %s", out_path)


def plot_base_model_comparison(ab_comparison: dict, output_dir: str):
    """
    AUC per base model on 2024-25 holdout.
    Reads pre-computed per-model AUCs from v2_ab_comparison.json (models trained
    on 2020-24 only, so 2024-25 is truly out-of-sample for each base model).
    """
    winner = ab_comparison.get("winner", "B")
    winner_results = ab_comparison.get(f"set_{winner}", {})

    base_aucs = winner_results.get("base_model_aucs", {})
    ensemble_auc = winner_results.get("auc")

    if not base_aucs:
        log.warning("No base_model_aucs in v2_ab_comparison.json — skipping base model plot")
        return

    model_names = ["logreg", "rf", "xgb", "mlp", "ensemble"]
    aucs = [base_aucs.get(n, 0.0) for n in model_names[:4]] + [ensemble_auc]

    for name, auc in zip(model_names, aucs):
        log.info("Base model AUC on holdout — %-8s: %.4f", name, auc)

    fig, ax = plt.subplots(figsize=(8, 5))
    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B3"]
    bars = ax.bar(model_names, aucs, color=colors[:len(model_names)], edgecolor="white")
    ax.set_ylim(0.5, 1.0)
    ax.set_ylabel("AUC")
    ax.set_title(f"Base Model + Ensemble AUC — 2024-25 Holdout (Set {winner}, trained on 2020-24)")
    for bar, val in zip(bars, aucs):
        ax.text(bar.get_x() + bar.get_width() / 2, val + 0.003, f"{val:.4f}", ha="center", va="bottom", fontsize=9)

    out_path = os.path.join(output_dir, "v2_base_model_comparison.png")
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved base model comparison → %s", out_path)


def plot_feature_importance(model: StackingEnsemble, output_dir: str):
    """XGBoost gain importance from the final production model."""
    try:
        xgb_pipeline = model.base_models["xgb"]
        # The XGB estimator is in the last step of the pipeline
        xgb_clf = xgb_pipeline.named_steps.get("clf") or xgb_pipeline[-1]
        importances = xgb_clf.feature_importances_
        feature_names = model.feature_names

        sorted_idx = np.argsort(importances)[::-1]
        sorted_imp = importances[sorted_idx]
        sorted_names = [feature_names[i] for i in sorted_idx]

        fig, ax = plt.subplots(figsize=(8, max(4, len(feature_names) * 0.35)))
        ax.barh(sorted_names[::-1], sorted_imp[::-1], color="steelblue")
        ax.set_xlabel("XGBoost Gain Importance")
        ax.set_title("Feature Importance (XGB, Final Model)")

        out_path = os.path.join(output_dir, "v2_feature_importance.png")
        fig.savefig(out_path, dpi=120, bbox_inches="tight")
        plt.close(fig)
        log.info("Saved feature importance → %s", out_path)
    except Exception as exc:
        log.warning("Could not plot feature importance: %s", exc)


def evaluate_live_2025_26(enriched_df: pd.DataFrame, model: StackingEnsemble) -> dict | None:
    """Compute metrics on 2025-26 games that have outcomes (WIN is not NaN)."""
    live_df = enriched_df[enriched_df["SEASON"] == LIVE_SEASON].copy()
    live_df = live_df.dropna(subset=["WIN"])

    if len(live_df) == 0:
        log.info("No settled 2025-26 games available for live evaluation.")
        return None

    y_true = live_df["WIN"].values
    proba = model.predict_proba(live_df)[:, 1]
    pred = (proba >= 0.5).astype(int)

    metrics = {
        "season": LIVE_SEASON,
        "n_games": int(len(y_true)),
        "accuracy": float(accuracy_score(y_true, pred)),
        "log_loss": float(log_loss(y_true, proba)),
        "brier_score": float(brier_score_loss(y_true, proba)),
        "auc": float(roc_auc_score(y_true, proba)) if len(np.unique(y_true)) > 1 else None,
    }

    log.info(
        "Live 2025-26 (%d games) → ACC=%.4f  AUC=%s  Brier=%.4f  LogLoss=%.4f",
        metrics["n_games"],
        metrics["accuracy"],
        f"{metrics['auc']:.4f}" if metrics["auc"] is not None else "N/A",
        metrics["brier_score"],
        metrics["log_loss"],
    )
    return metrics


def main():
    parser = argparse.ArgumentParser(description="NBA ML Pipeline v2 — evaluation plots")
    parser.add_argument("--data-dir", default="data/")
    parser.add_argument(
        "--odds-db",
        default="/Users/savannahvo/Downloads/NBA-Machine-Learning-Sports-Betting/Data/OddsData.sqlite",
    )
    parser.add_argument("--models-dir", default="models/")
    parser.add_argument("--output-dir", default="eda_output/")
    args = parser.parse_args()

    # Load model
    clf_path = os.path.join(args.models_dir, "classifier.pkl")
    log.info("Loading model from %s", clf_path)
    model: StackingEnsemble = joblib.load(clf_path)

    # Load enriched data
    enriched_df = _load_enriched(args.data_dir, args.odds_db)
    splits = split_seasons(enriched_df)
    holdout_df = splits["holdout"]

    log.info("Holdout (2024-25): %d rows", len(holdout_df))

    final_proba = _get_holdout_proba(model, holdout_df)
    os.makedirs(args.output_dir, exist_ok=True)

    # Load A/B comparison for the base-model-AUC plot (uses OOS AUCs saved during training)
    ab_path = os.path.join(args.models_dir, "v2_ab_comparison.json")
    with open(ab_path) as f:
        ab_comparison = json.load(f)

    plot_calibration_curves(holdout_df, final_proba, args.output_dir)
    plot_accuracy_by_month(holdout_df, final_proba, args.output_dir)
    plot_base_model_comparison(ab_comparison, args.output_dir)
    plot_feature_importance(model, args.output_dir)

    live_metrics = evaluate_live_2025_26(enriched_df, model)
    if live_metrics:
        live_path = os.path.join(args.models_dir, "v2_live_metrics.json")
        with open(live_path, "w") as f:
            json.dump(live_metrics, f, indent=2)
        log.info("Saved live metrics → %s", live_path)

    log.info("Evaluation complete. Plots saved to %s/", args.output_dir)


if __name__ == "__main__":
    main()
