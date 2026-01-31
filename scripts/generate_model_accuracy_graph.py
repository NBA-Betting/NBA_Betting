#!/usr/bin/env python
"""
Generate the cls_model_accuracy.png graph for the README.

Compares all 4 model variants:
- Classification (without Vegas line)
- Regression (without Vegas line) - converted to spread picks
- Classification with Vegas line
- Regression with Vegas line - converted to spread picks
"""

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from src.database import get_engine

# Colors
GRAY = "#6c757d"        # Neutral gray for baseline
GREEN = "#228b22"       # Forest green for profitable threshold
NBA_BLUE = "#1a5490"    # Classification
NBA_RED = "#b33040"     # Regression
LIGHT_BLUE = "#4a90d9"  # Classification + Vegas
LIGHT_RED = "#d96070"   # Regression + Vegas


def load_model_accuracies():
    """Load predictions and calculate accuracy for all model variants."""
    engine = get_engine()

    query = """
    SELECT
        p.game_id,
        p.model_variant,
        p.ml_cls_pred_1,
        p.ml_reg_pred_1,
        p.open_line_hv,
        g.home_score,
        g.away_score,
        g.open_line
    FROM predictions p
    JOIN games g ON p.game_id = g.game_id
    WHERE g.home_score IS NOT NULL
      AND g.open_line IS NOT NULL
    """

    df = pd.read_sql_query(query, engine)

    if len(df) == 0:
        print("No predictions data found")
        return None

    # Calculate actual outcome (1 = home team covers, 0 = away team covers)
    df["actual_spread_diff"] = (df["home_score"] - df["away_score"]) + df["open_line"]
    df["actual_outcome"] = (df["actual_spread_diff"] > 0).astype(int)

    results = {}

    # Calculate accuracy for each model variant
    for variant in ["standard", "vegas"]:
        variant_df = df[df["model_variant"] == variant].copy()

        if len(variant_df) == 0:
            print(f"No {variant} predictions found")
            continue

        # --- Classification Model Accuracy ---
        cls_df = variant_df[variant_df["ml_cls_pred_1"].notna()].copy()
        if len(cls_df) > 0:
            cls_df["cls_correct"] = (cls_df["ml_cls_pred_1"] == cls_df["actual_outcome"]).astype(int)
            cls_accuracy = cls_df["cls_correct"].mean() * 100
            cls_n = len(cls_df)
        else:
            cls_accuracy, cls_n = 0, 0

        # --- Regression Model Accuracy ---
        reg_df = variant_df[variant_df["ml_reg_pred_1"].notna()].copy()
        if len(reg_df) > 0:
            reg_df["reg_pick"] = (reg_df["ml_reg_pred_1"] > reg_df["open_line_hv"]).astype(int)
            reg_df["reg_correct"] = (reg_df["reg_pick"] == reg_df["actual_outcome"]).astype(int)
            reg_accuracy = reg_df["reg_correct"].mean() * 100
            reg_n = len(reg_df)
        else:
            reg_accuracy, reg_n = 0, 0

        # Store results with appropriate keys
        if variant == "standard":
            results["cls_accuracy"] = cls_accuracy
            results["cls_n"] = cls_n
            results["reg_accuracy"] = reg_accuracy
            results["reg_n"] = reg_n
            print(f"Standard Classification: {cls_accuracy:.2f}% ({cls_n:,} predictions)")
            print(f"Standard Regression:     {reg_accuracy:.2f}% ({reg_n:,} predictions)")
        else:  # vegas
            results["cls_vegas_accuracy"] = cls_accuracy
            results["cls_vegas_n"] = cls_n
            results["reg_vegas_accuracy"] = reg_accuracy
            results["reg_vegas_n"] = reg_n
            print(f"Vegas Classification:    {cls_accuracy:.2f}% ({cls_n:,} predictions)")
            print(f"Vegas Regression:        {reg_accuracy:.2f}% ({reg_n:,} predictions)")

    return results if results else None


def plot_model_accuracy(results, save=False, image_name=None):
    """Generate model accuracy bar chart comparing CLS and REG models."""

    sns.set_theme()
    sns.set_style()
    sns.set_context("notebook")

    fig, ax = plt.subplots(figsize=(9, 7))

    # Data: Random baseline, Classification, Regression
    categories = ["Random\nBaseline", "Classification\nModel", "Regression\nModel"]
    accuracies = [50.0, results["cls_accuracy"], results["reg_accuracy"]]
    colors = [GRAY, NBA_BLUE, NBA_RED]

    # Create bar chart
    bars = ax.bar(categories, accuracies, color=colors, width=0.55)

    # Add value labels on bars
    for bar, value in zip(bars, accuracies):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value - 2.5,
            f"{value:.1f}%",
            ha="center",
            va="top",
            fontsize=20,
            fontweight="bold",
            color="white"
        )

    # Title with sample size subtitle
    n_games = results["cls_n"]
    ax.set_title("Spread Prediction Accuracy", fontsize=22, pad=28, fontweight="bold")
    ax.text(0.5, 1.01, f"{n_games:,} NBA Games (2008-2026)",
            transform=ax.transAxes, ha="center", fontsize=12, color=GRAY)

    ax.set_ylabel("Accuracy %", fontsize=16, labelpad=8, fontweight="bold")
    ax.set_xlabel("")

    # Set y-axis range
    ax.set_ylim(0, 60)

    # Add horizontal line at 52.4% (break-even threshold) with label
    ax.axhline(y=52.4, color=GREEN, linestyle="--", linewidth=2, alpha=0.8)
    ax.text(2.35, 52.8, "52.4% Break-Even", fontsize=10, color=GREEN, fontweight="bold")

    # Style ticks
    plt.xticks(fontsize=13, fontweight="bold")
    plt.yticks(fontsize=12)

    # Add subtle horizontal grid
    ax.yaxis.grid(True, linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)

    # Clean up spines
    sns.despine()

    plt.tight_layout()

    if save:
        plt.savefig(f"images/{image_name}.png", dpi=300, bbox_inches="tight")
        print(f"Saved to images/{image_name}.png")


def plot_model_accuracy_4variants(results, save=False, image_name=None):
    """Generate model accuracy bar chart comparing all 4 model variants."""

    sns.set_theme()
    sns.set_style()
    sns.set_context("notebook")

    fig, ax = plt.subplots(figsize=(12, 7))

    # Data for all 4 variants plus random baseline
    categories = [
        "Random\nBaseline",
        "CLS",
        "REG",
        "CLS\n+Vegas",
        "REG\n+Vegas"
    ]
    accuracies = [
        50.0,
        results.get("cls_accuracy", 50.0),
        results.get("reg_accuracy", 50.0),
        results.get("cls_vegas_accuracy", 50.0),
        results.get("reg_vegas_accuracy", 50.0),
    ]
    colors = [GRAY, NBA_BLUE, NBA_RED, LIGHT_BLUE, LIGHT_RED]

    # Create bar chart
    bars = ax.bar(categories, accuracies, color=colors, width=0.6)

    # Add value labels on bars
    for bar, value in zip(bars, accuracies):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value - 2.5,
            f"{value:.1f}%",
            ha="center",
            va="top",
            fontsize=18,
            fontweight="bold",
            color="white"
        )

    # Title with sample size subtitle
    n_games = results.get("cls_n", 0)
    ax.set_title("Spread Prediction Accuracy", fontsize=22, pad=28, fontweight="bold")
    ax.text(0.5, 1.01, f"{n_games:,} NBA Games (2008-2026)",
            transform=ax.transAxes, ha="center", fontsize=12, color=GRAY)

    ax.set_ylabel("Accuracy %", fontsize=16, labelpad=8, fontweight="bold")
    ax.set_xlabel("")

    # Set y-axis range
    ax.set_ylim(0, 60)

    # Add horizontal line at 52.4% (break-even threshold) with label
    ax.axhline(y=52.4, color=GREEN, linestyle="--", linewidth=2, alpha=0.8)
    ax.text(3.55, 53.5, "52.4% Break-Even", fontsize=10, color=GREEN, fontweight="bold")

    # Style ticks
    plt.xticks(fontsize=12, fontweight="bold")
    plt.yticks(fontsize=12)

    # Add subtle horizontal grid
    ax.yaxis.grid(True, linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)

    # Add legend (horizontal, upper left)
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=NBA_BLUE, label='CLS'),
        Patch(facecolor=NBA_RED, label='REG'),
        Patch(facecolor=LIGHT_BLUE, label='CLS +Vegas'),
        Patch(facecolor=LIGHT_RED, label='REG +Vegas'),
    ]
    ax.legend(handles=legend_elements, loc='upper left', fontsize=9, ncol=4)

    # Clean up spines
    sns.despine()

    plt.tight_layout()

    if save:
        plt.savefig(f"images/{image_name}.png", dpi=300, bbox_inches="tight")
        print(f"Saved to images/{image_name}.png")


def main():
    print("Generating model accuracy graph...")
    print()

    results = load_model_accuracies()

    if results is None:
        print("ERROR: No prediction data available. Run backfill_predictions.py first.")
        return

    print()

    # Check if we have vegas predictions
    has_vegas = results.get("cls_vegas_n", 0) > 0

    if has_vegas:
        print("Plotting (4 variants - with and without Vegas)...")
        plot_model_accuracy_4variants(
            results,
            save=True,
            image_name="cls_model_accuracy"
        )
    else:
        print("Plotting (2 variants - without Vegas)...")
        plot_model_accuracy(
            results,
            save=True,
            image_name="cls_model_accuracy"
        )

    print("Done!")


if __name__ == "__main__":
    main()
