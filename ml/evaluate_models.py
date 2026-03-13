"""
evaluate_models.py
------------------
Comprehensive evaluation of classifier.pkl and regressor.pkl.

Usage:
    python evaluate_models.py
    python evaluate_models.py --data-dir data/
"""

import os
import json
import logging
import argparse

import joblib
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sklearn.metrics import (
    accuracy_score, roc_auc_score, classification_report,
    confusion_matrix, mean_squared_error, mean_absolute_error, r2_score,
)
from sklearn.calibration import calibration_curve

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir",  default=os.path.join(os.path.dirname(__file__), "data"))
    parser.add_argument("--models-dir", default=os.path.join(os.path.dirname(__file__), "models"))
    args = parser.parse_args()

    eda_dir = os.path.join(os.path.dirname(__file__), "eda_output")
    os.makedirs(eda_dir, exist_ok=True)

    training_path = os.path.join(args.data_dir, "training_data.csv")
    if not os.path.exists(training_path):
        log.error("training_data.csv not found")
        raise SystemExit(1)

    df = pd.read_csv(training_path)
    date_col   = next((c for c in ("GAME_DATE", "game_date", "gameDateTimeEst") if c in df.columns), None)
    season_col = next((c for c in ("SEASON", "season", "seasonYear") if c in df.columns), None)
    if date_col:
        df = df.sort_values(date_col)

    # Feature list
    feat_path = os.path.join(args.models_dir, "selected_features.json")
    if os.path.exists(feat_path):
        with open(feat_path) as f:
            feat_data = json.load(f)
        features = feat_data.get("selected_features", feat_data.get("features", []))
        if features and isinstance(features[0], dict):
            features = [f["feature"] for f in features]
    else:
        from feature_schema import FEATURES
        features = [f for f in FEATURES if f in df.columns]

    available = [f for f in features if f in df.columns]

    # Test set
    if season_col:
        test_mask = df[season_col] == "2025-26"
        if test_mask.sum() == 0:
            split = int(len(df) * 0.85)
            test_mask = pd.Series([False] * split + [True] * (len(df) - split), index=df.index)
    else:
        split = int(len(df) * 0.85)
        test_mask = pd.Series([False] * split + [True] * (len(df) - split), index=df.index)

    X_test = df[test_mask][available].fillna(0.0)
    y_test = df[test_mask]["WIN"].astype(int)
    log.info("Test set: %d games", len(X_test))

    results = {}

    # ── Classifier ─────────────────────────────────────────────────────────────
    clf_path = os.path.join(args.models_dir, "classifier.pkl")
    if os.path.exists(clf_path):
        clf = joblib.load(clf_path)
        proba = clf.predict_proba(X_test)[:, 1]
        pred  = (proba >= 0.5).astype(int)

        acc = accuracy_score(y_test, pred)
        auc = roc_auc_score(y_test, proba)
        cm  = confusion_matrix(y_test, pred).tolist()

        log.info("\n" + "=" * 60)
        log.info("CLASSIFIER EVALUATION")
        log.info("=" * 60)
        log.info("Accuracy: %.4f (%.1f%%)", acc, acc * 100)
        log.info("AUC-ROC:  %.4f", auc)
        log.info("Confusion Matrix:\n%s", np.array(cm))
        log.info("\n%s", classification_report(y_test, pred, target_names=["Away Win", "Home Win"]))

        # Calibration curve
        fraction_pos, mean_pred = calibration_curve(y_test, proba, n_bins=10)
        fig, ax = plt.subplots(figsize=(7, 5))
        ax.plot(mean_pred, fraction_pos, "b-o", label="Model")
        ax.plot([0, 1], [0, 1], "k--", label="Perfect calibration")
        ax.set_xlabel("Mean predicted probability")
        ax.set_ylabel("Fraction of positives")
        ax.set_title("Calibration Curve")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(eda_dir, "calibration.png"), dpi=120)
        plt.close(fig)
        log.info("Calibration curve saved → eda_output/calibration.png")

        # Rolling accuracy by month
        if "game_date" in df.columns:
            test_df = df[test_mask].copy()
            test_df["pred"] = pred
            test_df["correct"] = (test_df["pred"] == y_test.values)
            test_df["month"] = pd.to_datetime(test_df["game_date"]).dt.to_period("M")
            monthly = test_df.groupby("month")["correct"].mean().reset_index()
            monthly.columns = ["month", "accuracy"]

            fig2, ax2 = plt.subplots(figsize=(10, 4))
            ax2.plot(range(len(monthly)), monthly["accuracy"], "b-o", linewidth=2)
            ax2.axhline(acc, color="r", linestyle="--", label=f"Overall: {acc:.1%}")
            ax2.set_xticks(range(len(monthly)))
            ax2.set_xticklabels([str(m) for m in monthly["month"]], rotation=45, ha="right")
            ax2.set_ylabel("Accuracy")
            ax2.set_title("Rolling Monthly Accuracy")
            ax2.set_ylim(0.4, 0.85)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            fig2.tight_layout()
            fig2.savefig(os.path.join(eda_dir, "monthly_accuracy.png"), dpi=120)
            plt.close(fig2)

        results["classifier"] = {
            "accuracy":        round(acc, 4),
            "auc":             round(auc, 4),
            "confusion_matrix": cm,
            "test_games":      int(len(X_test)),
        }
    else:
        log.warning("classifier.pkl not found at %s", clf_path)

    # ── Regressor ──────────────────────────────────────────────────────────────
    reg_path = os.path.join(args.models_dir, "regressor.pkl")
    if os.path.exists(reg_path) and "home_score" in df.columns:
        reg = joblib.load(reg_path)
        y_diff = df[test_mask]["home_score"] - df[test_mask]["away_score"]
        y_diff = y_diff.fillna(0.0)
        pred_diff = reg.predict(X_test)

        rmse = np.sqrt(mean_squared_error(y_diff, pred_diff))
        mae  = mean_absolute_error(y_diff, pred_diff)
        r2   = r2_score(y_diff, pred_diff)

        log.info("\n" + "=" * 60)
        log.info("REGRESSOR EVALUATION")
        log.info("=" * 60)
        log.info("RMSE: %.2f pts", rmse)
        log.info("MAE:  %.2f pts", mae)
        log.info("R²:   %.4f", r2)

        # Residual plot
        residuals = y_diff.values - pred_diff
        fig3, ax3 = plt.subplots(figsize=(7, 5))
        ax3.scatter(pred_diff, residuals, alpha=0.4, s=10)
        ax3.axhline(0, color="r", linestyle="--")
        ax3.set_xlabel("Predicted point differential")
        ax3.set_ylabel("Residual (actual - predicted)")
        ax3.set_title(f"Regressor Residuals (RMSE={rmse:.1f})")
        ax3.grid(True, alpha=0.3)
        fig3.tight_layout()
        fig3.savefig(os.path.join(eda_dir, "residuals.png"), dpi=120)
        plt.close(fig3)
        log.info("Residual plot saved → eda_output/residuals.png")

        results["regressor"] = {
            "rmse": round(rmse, 3),
            "mae":  round(mae, 3),
            "r2":   round(r2, 4),
        }

    # Save evaluation results
    out_path = os.path.join(args.models_dir, "evaluation_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    log.info("\nEvaluation results saved → %s", out_path)

    # Summary table
    log.info("\n%s", "=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    if "classifier" in results:
        c = results["classifier"]
        log.info("  Win/Loss accuracy:  %.1f%%", c["accuracy"] * 100)
        log.info("  AUC-ROC:            %.4f",   c["auc"])
    if "regressor" in results:
        r = results["regressor"]
        log.info("  Score pred RMSE:    %.2f pts", r["rmse"])
        log.info("  Score pred MAE:     %.2f pts", r["mae"])
    log.info("=" * 60)


if __name__ == "__main__":
    main()
