"""
stacking_ensemble.py
--------------------
StackingEnsemble model class — must live in backend/ so joblib.load can
resolve it at inference time. train_v2.py imports from this module too
(via sys.path) so the pickle's class reference matches.
"""

import numpy as np
import pandas as pd


class StackingEnsemble:
    """
    Stacked ensemble of 4 base models + meta logistic regression.

    Backward-compatible with scheduler.py:
        model.predict_proba(X)[0][1]  → home win probability
    """

    def __init__(self, base_models: dict, meta_learner, feature_names: list):
        self.base_models = base_models        # {"logreg": pipeline, "rf": pipeline, ...}
        self.meta_learner = meta_learner      # LogisticRegression fitted on OOF preds
        self.feature_names = feature_names    # list of feature column names

    def _to_array(self, X) -> np.ndarray:
        if isinstance(X, pd.DataFrame):
            return X[self.feature_names].values
        return np.asarray(X)

    def predict_proba(self, X) -> np.ndarray:
        """Returns shape (n, 2) with [prob_loss, prob_win]."""
        X_arr = self._to_array(X)
        base_preds = np.column_stack([
            self.base_models[name].predict_proba(X_arr)[:, 1]
            for name in ["logreg", "rf", "xgb", "mlp"]
        ])
        return self.meta_learner.predict_proba(base_preds)

    def predict(self, X) -> np.ndarray:
        proba = self.predict_proba(X)[:, 1]
        return (proba >= 0.5).astype(int)
