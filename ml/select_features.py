"""
select_features.py
------------------
SHAP-based feature selection and permutation importance pruning.

Usage:
    python select_features.py
    python select_features.py --data-dir data/
"""

import os
import json
import logging
import argparse
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "data"))
    parser.add_argument("--shap-threshold", type=float, default=0.005)
    args = parser.parse_args()

    from feature_schema import FEATURES, FEATURE_METADATA

    training_path = os.path.join(args.data_dir, "training_data.csv")
    if not os.path.exists(training_path):
        log.error("training_data.csv not found at %s — run feature_engineering.py first", training_path)
        raise SystemExit(1)

    df = pd.read_csv(training_path)
    log.info("Loaded %d rows, %d columns", len(df), len(df.columns))

    # Determine available features
    available = [f for f in FEATURES if f in df.columns]
    missing = [f for f in FEATURES if f not in df.columns]
    if missing:
        log.warning("Features not in training data (will be skipped): %s", missing)

    X = df[available].fillna(0.0)
    y = df["WIN"].astype(int)

    # Train/test split — 2024-25 as test
    if "season" in df.columns:
        train_mask = df["season"] != "2025-26"
        test_mask  = df["season"] == "2025-26"
        X_train, y_train = X[train_mask], y[train_mask]
        X_test,  y_test  = X[test_mask],  y[test_mask]
    else:
        split = int(len(df) * 0.8)
        X_train, y_train = X.iloc[:split], y.iloc[:split]
        X_test,  y_test  = X.iloc[split:], y.iloc[split:]

    log.info("Train: %d rows | Test: %d rows", len(X_train), len(X_test))

    # ── Step 1: Train XGBoost on all features ──────────────────────────────────
    from xgboost import XGBClassifier
    from sklearn.metrics import roc_auc_score

    xgb = XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        random_state=42,
        verbosity=0,
    )
    xgb.fit(X_train, y_train)
    auc_before = roc_auc_score(y_test, xgb.predict_proba(X_test)[:, 1])
    log.info("AUC before selection (all %d features): %.4f", len(available), auc_before)

    # ── Step 2: Feature importances (SHAP preferred, XGBoost gain fallback) ──────
    # XGBoost 3.x / SHAP 0.46 have an incompatibility where XGBoost stores
    # base_score as a JSON array string that SHAP cannot parse.
    # Attempt SHAP; fall back to XGBoost built-in gain importance.
    mean_abs_shap: dict = {}
    try:
        import shap
        explainer = shap.TreeExplainer(xgb, feature_perturbation="tree_path_dependent")
        shap_values = explainer.shap_values(X_train)
        if isinstance(shap_values, list):
            shap_values = shap_values[1]
        mean_abs_shap = dict(zip(available, np.abs(shap_values).mean(axis=0)))
        log.info("SHAP importances computed successfully.")
    except Exception as shap_err:
        log.warning("SHAP failed (%s); using XGBoost gain importance as proxy.", shap_err)
        gain_imp = xgb.get_booster().get_score(importance_type="gain")
        total = max(sum(gain_imp.values()), 1e-9)
        mean_abs_shap = {f: gain_imp.get(f, 0.0) / total for f in available}

    log.info("\nFeature importances:")
    for feat, val in sorted(mean_abs_shap.items(), key=lambda x: -x[1]):
        marker = "  ✗ DROP" if val < args.shap_threshold else ""
        log.info("  %-30s %.5f%s", feat, val, marker)

    shap_selected = [f for f in available if mean_abs_shap.get(f, 0) >= args.shap_threshold]
    shap_dropped = [f for f in available if mean_abs_shap.get(f, 0) < args.shap_threshold]
    log.info("\nImportance pruning: dropped %d features: %s", len(shap_dropped), shap_dropped)

    # ── Step 3: Permutation importance ────────────────────────────────────────
    from sklearn.inspection import permutation_importance

    # Re-train on SHAP-selected features before running permutation importance
    # so the model's expected feature set matches the input.
    xgb_sel = XGBClassifier(
        n_estimators=200, max_depth=4, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, eval_metric="logloss",
        random_state=42, verbosity=0,
    )
    xgb_sel.fit(X_train[shap_selected], y_train)

    X_test_sel = X_test[shap_selected]
    perm = permutation_importance(xgb_sel, X_test_sel, y_test, n_repeats=5, random_state=42, n_jobs=1)
    perm_importances = dict(zip(shap_selected, perm.importances_mean))

    # Baseline: any feature with mean permutation importance > 0 survives.
    # (Noise column trick doesn't work with XGBoost — extra columns fail validation.)
    noise_baseline = 0.0
    log.info("Permutation noise baseline: %.5f (fixed threshold)", noise_baseline)

    perm_selected = [f for f in shap_selected if perm_importances.get(f, 0) > noise_baseline]
    perm_dropped = [f for f in shap_selected if perm_importances.get(f, 0) <= noise_baseline]
    log.info("Permutation pruning: dropped %d features: %s", len(perm_dropped), perm_dropped)

    # ── Step 4: AUC after selection ────────────────────────────────────────────
    xgb2 = XGBClassifier(
        n_estimators=200, max_depth=4, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, eval_metric="logloss",
        random_state=42, verbosity=0,
    )
    xgb2.fit(X_train[perm_selected], y_train)
    auc_after = roc_auc_score(y_test, xgb2.predict_proba(X_test[perm_selected])[:, 1])
    log.info("AUC after selection (%d features): %.4f", len(perm_selected), auc_after)

    # ── Step 5: Save ───────────────────────────────────────────────────────────
    models_dir = os.path.join(os.path.dirname(__file__), "models")
    os.makedirs(models_dir, exist_ok=True)

    # Enrich with FEATURE_METADATA for the frontend Model tab
    features_with_meta = []
    for feat in perm_selected:
        meta = FEATURE_METADATA.get(feat, {})
        features_with_meta.append({
            "feature":         feat,
            "label":           meta.get("label", feat),
            "description":     meta.get("description", ""),
            "why":             meta.get("why", ""),
            "shap_importance": round(mean_abs_shap.get(feat, 0.0), 5),
            "perm_importance": round(perm_importances.get(feat, 0.0), 5),
        })
    features_with_meta.sort(key=lambda x: -x["shap_importance"])

    output = {
        "selected_features":      perm_selected,
        "features":               features_with_meta,
        "shap_importances":       {k: round(v, 5) for k, v in mean_abs_shap.items()},
        "permutation_importances": {k: round(v, 5) for k, v in perm_importances.items()},
        "auc_before":             round(auc_before, 4),
        "auc_after":              round(auc_after, 4),
        "n_features_before":      len(available),
        "n_features_after":       len(perm_selected),
        "shap_threshold":         args.shap_threshold,
        "noise_baseline":         round(noise_baseline, 5),
        "model_version":          "v1.0",
    }

    out_path = os.path.join(models_dir, "selected_features.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    log.info("Saved selected_features.json → %s", out_path)
    log.info("Final: %d features selected (AUC %.4f → %.4f)", len(perm_selected), auc_before, auc_after)


if __name__ == "__main__":
    main()
