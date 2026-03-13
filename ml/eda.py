"""
eda.py
------
Exploratory Data Analysis for the NBA prediction pipeline.
Loads training_data.csv (built by feature_engineering.py) and produces
diagnostic plots saved to ml/eda_output/.

Plots generated:
  1. Correlation matrix heatmap        → eda_output/correlation_matrix.png
  2. Feature distributions histograms  → eda_output/feature_distributions.png
  3. Point-biserial feature-WIN corr   → eda_output/feature_correlations.png
  4. Temporal stability (mean by season)→ eda_output/temporal_stability.png

Also prints multicollinear feature pairs (|r| > 0.85) to stdout.

Usage:
    python eda.py
    python eda.py --data-dir ml/data/ --save-only
"""

from __future__ import annotations

import argparse
import logging
import os

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
DEFAULT_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "eda_output")

MULTICOLLINEARITY_THRESHOLD = 0.85


def load_training_data(data_dir: str) -> pd.DataFrame:
    """Load training_data.csv from data_dir."""
    path = os.path.join(data_dir, "training_data.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"training_data.csv not found at {path}. "
            "Run feature_engineering.py first."
        )
    df = pd.read_csv(path, low_memory=False)
    log.info(f"Loaded training_data.csv: {len(df):,} rows, {len(df.columns)} columns")
    return df


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    """Return numeric feature columns (exclude target and metadata)."""
    exclude = {"WIN", "GAME_DATE", "game_date", "gameId", "game_id",
               "season", "seasonYear", "season_year", "homeTeamId", "awayTeamId",
               "homeTeamName", "awayTeamName", "GAME_ID"}
    feature_cols = [
        c for c in df.columns
        if c not in exclude
        and pd.api.types.is_numeric_dtype(df[c])
    ]
    log.info(f"Feature columns identified: {len(feature_cols)}")
    return feature_cols


# ── Plot 1: Correlation Matrix ────────────────────────────────────────────────

def plot_correlation_matrix(
    df: pd.DataFrame,
    feature_cols: list[str],
    output_dir: str,
    save_only: bool,
) -> pd.DataFrame:
    """Compute and plot the feature-feature correlation matrix."""
    log.info("Plotting correlation matrix...")
    corr = df[feature_cols].corr()

    n = len(feature_cols)
    fig_size = max(12, n * 0.4)
    fig, ax = plt.subplots(figsize=(fig_size, fig_size * 0.85))

    mask = np.triu(np.ones_like(corr, dtype=bool))
    sns.heatmap(
        corr,
        mask=mask,
        annot=(n <= 30),
        fmt=".2f" if n <= 30 else "",
        cmap="RdYlGn",
        center=0,
        vmin=-1,
        vmax=1,
        linewidths=0.3 if n <= 30 else 0,
        ax=ax,
        cbar_kws={"shrink": 0.7},
    )
    ax.set_title("Feature Correlation Matrix (lower triangle)", fontsize=14, pad=14)
    plt.tight_layout()

    out_path = os.path.join(output_dir, "correlation_matrix.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    log.info(f"  Saved → {out_path}")

    if not save_only:
        plt.show()
    plt.close()

    return corr


# ── Plot 2: Feature Distributions ────────────────────────────────────────────

def plot_feature_distributions(
    df: pd.DataFrame,
    feature_cols: list[str],
    output_dir: str,
    save_only: bool,
) -> None:
    """Plot histogram of each feature, coloured by WIN outcome."""
    log.info("Plotting feature distributions...")

    n_features = len(feature_cols)
    n_cols = 4
    n_rows = (n_features + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 4, n_rows * 3))
    axes_flat = axes.flatten() if n_features > 1 else [axes]

    has_win = "WIN" in df.columns
    palette = {1: "#2ecc71", 0: "#e74c3c"}

    for idx, col in enumerate(feature_cols):
        ax = axes_flat[idx]
        if has_win:
            for label, grp in df.groupby("WIN"):
                ax.hist(
                    grp[col].dropna(),
                    bins=30,
                    alpha=0.55,
                    label=("Home Win" if label == 1 else "Away Win"),
                    color=palette[label],
                    density=True,
                )
            ax.legend(fontsize=7)
        else:
            ax.hist(df[col].dropna(), bins=30, color="#3498db", alpha=0.7)

        ax.set_title(col, fontsize=8, pad=3)
        ax.set_xlabel("")
        ax.tick_params(labelsize=7)

    # Hide unused axes
    for idx in range(n_features, len(axes_flat)):
        axes_flat[idx].set_visible(False)

    fig.suptitle("Feature Distributions (green = home win, red = away win)", fontsize=12, y=1.01)
    plt.tight_layout()

    out_path = os.path.join(output_dir, "feature_distributions.png")
    plt.savefig(out_path, dpi=130, bbox_inches="tight")
    log.info(f"  Saved → {out_path}")

    if not save_only:
        plt.show()
    plt.close()


# ── Plot 3: Feature Correlations with WIN ─────────────────────────────────────

def plot_feature_correlations(
    df: pd.DataFrame,
    feature_cols: list[str],
    output_dir: str,
    save_only: bool,
) -> pd.Series:
    """
    Compute point-biserial correlation of each feature with the WIN target
    and plot as a horizontal bar chart sorted by absolute correlation.
    """
    if "WIN" not in df.columns:
        log.warning("WIN column not found — skipping feature correlation plot.")
        return pd.Series(dtype=float)

    log.info("Plotting point-biserial feature-WIN correlations...")

    correlations = {}
    for col in feature_cols:
        valid = df[[col, "WIN"]].dropna()
        if len(valid) < 30 or valid[col].std() == 0:
            correlations[col] = 0.0
            continue
        r = valid[col].astype(float).corr(valid["WIN"].astype(float))
        correlations[col] = round(float(r), 4)

    corr_series = pd.Series(correlations).sort_values(key=abs, ascending=True)

    colors = ["#e74c3c" if v < 0 else "#2ecc71" for v in corr_series.values]

    fig, ax = plt.subplots(figsize=(9, max(6, len(corr_series) * 0.32)))
    bars = ax.barh(corr_series.index, corr_series.values, color=colors, edgecolor="white", linewidth=0.4)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Point-Biserial Correlation with WIN")
    ax.set_title("Feature Correlations with Game Outcome (WIN=1 → home win)", fontsize=12)
    ax.tick_params(axis="y", labelsize=8)

    # Annotate bars
    for bar, val in zip(bars, corr_series.values):
        ax.text(
            val + (0.003 if val >= 0 else -0.003),
            bar.get_y() + bar.get_height() / 2,
            f"{val:+.3f}",
            va="center",
            ha="left" if val >= 0 else "right",
            fontsize=7,
        )

    plt.tight_layout()
    out_path = os.path.join(output_dir, "feature_correlations.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    log.info(f"  Saved → {out_path}")

    if not save_only:
        plt.show()
    plt.close()

    return corr_series


# ── Plot 4: Temporal Stability ────────────────────────────────────────────────

def plot_temporal_stability(
    df: pd.DataFrame,
    feature_cols: list[str],
    output_dir: str,
    save_only: bool,
) -> None:
    """
    Plot the mean value of each feature grouped by season.
    Features whose mean shifts significantly across seasons may become stale.
    Only plot the top 12 features by variance to keep the chart readable.
    """
    log.info("Plotting temporal stability by season...")

    season_col = None
    for col in ("seasonYear", "season_year", "season", "SEASON"):
        if col in df.columns:
            season_col = col
            break

    if season_col is None:
        log.warning("No season column found — skipping temporal stability plot.")
        return

    season_means = df.groupby(season_col)[feature_cols].mean()
    if season_means.empty:
        log.warning("No seasonal data — skipping temporal stability plot.")
        return

    # Select top-12 features by across-season variance
    season_variance = season_means.var(axis=0).sort_values(ascending=False)
    top_features = season_variance.head(12).index.tolist()

    n_cols = 3
    n_rows = (len(top_features) + n_cols - 1) // n_cols
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 5, n_rows * 3.2))
    axes_flat = axes.flatten() if len(top_features) > 1 else [axes]

    for idx, col in enumerate(top_features):
        ax = axes_flat[idx]
        ax.plot(
            season_means.index.astype(str),
            season_means[col],
            marker="o",
            linewidth=1.8,
            color="#3498db",
        )
        ax.set_title(col, fontsize=9, pad=4)
        ax.set_xlabel("Season", fontsize=8)
        ax.tick_params(axis="x", rotation=30, labelsize=7)
        ax.tick_params(axis="y", labelsize=7)
        ax.grid(True, alpha=0.3)

    for idx in range(len(top_features), len(axes_flat)):
        axes_flat[idx].set_visible(False)

    fig.suptitle("Temporal Stability: Feature Means by Season (top-12 by variance)", fontsize=12)
    plt.tight_layout()

    out_path = os.path.join(output_dir, "temporal_stability.png")
    plt.savefig(out_path, dpi=130, bbox_inches="tight")
    log.info(f"  Saved → {out_path}")

    if not save_only:
        plt.show()
    plt.close()


# ── Multicollinearity Check ───────────────────────────────────────────────────

def check_multicollinearity(corr_matrix: pd.DataFrame, threshold: float = MULTICOLLINEARITY_THRESHOLD) -> list[tuple]:
    """
    Identify feature pairs with |r| > threshold.
    Returns a list of (feature_a, feature_b, correlation) tuples, sorted by |r| desc.
    """
    pairs = []
    cols = corr_matrix.columns.tolist()
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            r = corr_matrix.iloc[i, j]
            if abs(r) > threshold:
                pairs.append((cols[i], cols[j], round(float(r), 4)))

    pairs.sort(key=lambda x: abs(x[2]), reverse=True)
    return pairs


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="EDA for NBA training data — generates diagnostic plots.",
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help=f"Directory containing training_data.csv (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to save EDA plots (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--save-only",
        action="store_true",
        help="Save plots to disk without displaying them (useful for headless servers).",
    )
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    if args.save_only:
        matplotlib.use("Agg")

    log.info(f"Data directory  : {data_dir}")
    log.info(f"Output directory: {output_dir}")
    log.info(f"Save-only mode  : {args.save_only}")

    # Load data
    df = load_training_data(data_dir)
    feature_cols = get_feature_columns(df)

    if not feature_cols:
        log.error("No numeric feature columns found. Ensure training_data.csv is correct.")
        return

    # Plot 1: Correlation matrix
    corr_matrix = plot_correlation_matrix(df, feature_cols, output_dir, args.save_only)

    # Plot 2: Feature distributions
    plot_feature_distributions(df, feature_cols, output_dir, args.save_only)

    # Plot 3: Feature-WIN correlations
    win_corrs = plot_feature_correlations(df, feature_cols, output_dir, args.save_only)

    # Plot 4: Temporal stability
    plot_temporal_stability(df, feature_cols, output_dir, args.save_only)

    # Multicollinearity report
    multicollinear_pairs = check_multicollinearity(corr_matrix)
    if multicollinear_pairs:
        print(f"\nMulticollinear feature pairs (|r| > {MULTICOLLINEARITY_THRESHOLD}):")
        print(f"  {'Feature A':<30s} {'Feature B':<30s} {'r':>8}")
        print(f"  {'-'*30} {'-'*30} {'-'*8}")
        for a, b, r in multicollinear_pairs:
            print(f"  {a:<30s} {b:<30s} {r:>+8.4f}")
        print(
            f"\n  Consider dropping one feature from each multicollinear pair "
            f"before training (use select_features.py for SHAP-based selection)."
        )
    else:
        print(f"\nNo multicollinear pairs found above |r| > {MULTICOLLINEARITY_THRESHOLD}.")

    if not win_corrs.empty:
        print(f"\nTop 10 features by |correlation| with WIN:")
        top10 = win_corrs.reindex(win_corrs.abs().sort_values(ascending=False).index).head(10)
        for feat, r in top10.items():
            print(f"  {feat:<35s} r = {r:+.4f}")

    log.info("\nEDA complete.")


if __name__ == "__main__":
    main()
