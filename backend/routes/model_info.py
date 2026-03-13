"""
routes/model_info.py
--------------------
Model metadata: feature list, importances, descriptions
"""
import os
import json
import logging
from fastapi import APIRouter

logger = logging.getLogger(__name__)
router = APIRouter()

MODELS_DIR = os.environ.get(
    "MODELS_DIR",
    os.path.join(os.path.dirname(__file__), "..", "models")
)


def _load_xgb_importances(feature_names: list) -> dict:
    """Extract normalised XGBoost gain importances from the production pkl."""
    try:
        import joblib
        clf_path = os.path.join(MODELS_DIR, "classifier.pkl")
        model = joblib.load(clf_path)
        xgb_pipeline = model.base_models.get("xgb")
        if xgb_pipeline is None:
            return {}
        xgb_clf = xgb_pipeline.named_steps.get("clf") or xgb_pipeline[-1]
        imps = xgb_clf.feature_importances_
        total = float(imps.sum()) or 1.0
        return {name: float(imp / total) for name, imp in zip(feature_names, imps)}
    except Exception as exc:
        logger.warning("Could not extract XGB importances: %s", exc)
        return {}


@router.get("/features")
def get_model_features():
    """Return selected features with importances and metadata."""
    try:
        features_path = os.path.join(MODELS_DIR, "selected_features.json")
        if not os.path.exists(features_path):
            return {
                "features": [],
                "status": "not_trained",
                "message": "Models not yet trained. Run ml/train_v2.py first.",
            }

        with open(features_path) as f:
            data = json.load(f)

        features = data.get("features", [])
        feature_names = [f["feature"] for f in features]

        # v1 stored shap_importances in JSON; v2 needs XGB gain from pkl
        stored_shap = data.get("shap_importances", {})
        if stored_shap:
            importance_map = stored_shap
        else:
            importance_map = _load_xgb_importances(feature_names)

        # Attach shap_importance to each feature entry so the frontend chart works
        for f in features:
            f["shap_importance"] = importance_map.get(f["feature"], 0.0)

        return {
            "features": features,
            "n_features": len(features),
            "model_version": data.get("model_version", "unknown"),
            "feature_set": data.get("feature_set", ""),
            "status": "ok",
        }

    except Exception as e:
        logger.error("Failed to load model features: %s", e)
        return {"features": [], "status": "error", "error": str(e)}
