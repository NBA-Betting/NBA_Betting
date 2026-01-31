#!/usr/bin/env python
"""Generate the vegas_miss_by_week.png graph for the README."""

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from src.database import get_engine


def fast_get_season(game_datetime):
    """Fast season calculation - NBA season spans Oct-Jun, named by start year."""
    month = game_datetime.month
    year = game_datetime.year
    if month >= 10:
        start_year = year
    else:
        start_year = year - 1
    return f"{start_year}-{start_year + 1}"


def fast_get_season_type(game_datetime):
    """Fast determination of regular vs postseason."""
    month = game_datetime.month
    day = game_datetime.day
    year = game_datetime.year

    # Special case: 2020 COVID bubble (July-Oct 2020 = 2019-2020 postseason)
    if year == 2020 and month in [7, 8, 9, 10]:
        return "post"

    # Normal postseason: mid-April through June
    if month in [5, 6]:
        return "post"
    elif month == 4 and day >= 15:
        return "post"
    elif month in [7, 8, 9]:
        return None  # Offseason
    else:
        return "reg"


def generate_reg_vs_post_graph(games, save=False, image_name=None):
    """Generate Regular Season vs Postseason Vegas Accuracy graph."""

    # NBA-inspired colors (balanced)
    NBA_BLUE = "#1a5490"
    NBA_RED = "#b33040"

    # Calculate averages by season and season_type
    season_stats = games.groupby(["season", "season_type"]).agg(
        avg_miss=("vegas_miss_abs", "mean")
    ).reset_index()

    # Calculate overall averages for each season type
    reg_avg = games[games["season_type"] == "reg"]["vegas_miss_abs"].mean()
    post_avg = games[games["season_type"] == "post"]["vegas_miss_abs"].mean()

    # Pivot for grouped bar chart
    pivot_data = season_stats.pivot(index="season", columns="season_type", values="avg_miss")
    pivot_data = pivot_data.sort_index()

    # Setup figure
    sns.set_theme()
    sns.set_style()
    sns.set_context("notebook")

    fig, ax = plt.subplots(figsize=(16, 7))

    # Bar positions
    x = np.arange(len(pivot_data.index))
    width = 0.35

    # Create bars with averages in legend labels (in front of lines)
    bars_reg = ax.bar(x - width/2, pivot_data["reg"], width,
                      label=f"Regular Season ({reg_avg:.2f})", color=NBA_BLUE, zorder=3)
    bars_post = ax.bar(x + width/2, pivot_data["post"], width,
                       label=f"Postseason ({post_avg:.2f})", color=NBA_RED, zorder=3)

    # Average lines for each season type (behind bars)
    ax.axhline(reg_avg, color=NBA_BLUE, linestyle="--", linewidth=2, alpha=0.7, zorder=1)
    ax.axhline(post_avg, color=NBA_RED, linestyle="--", linewidth=2, alpha=0.7, zorder=1)

    # Style the chart
    ax.set_title("Vegas Spread Error: Regular Season vs Postseason", fontsize=24, pad=16, fontweight="bold")
    ax.set_xlabel("Season", fontsize=18, labelpad=8, fontweight="bold")
    ax.set_ylabel("Average Error Per Game (Points)", fontsize=18, labelpad=8, fontweight="bold")

    # X-axis labels - full XXXX-XXXX format
    ax.set_xticks(x)
    ax.set_xticklabels(pivot_data.index, rotation=45, ha="right", fontsize=10)

    # Y-axis
    plt.yticks(fontsize=14)

    # Legend
    ax.legend(loc="upper left", fontsize=12, frameon=True, facecolor="white")

    # Clean up
    sns.despine()
    plt.tight_layout()

    if save:
        plt.savefig(f"{image_name}.png", dpi=300, bbox_inches="tight")
        print(f"Saved to {image_name}.png")


def main():
    print("Starting graph generation...")

    # Get database engine
    engine = get_engine()
    print("Database connected")

    # Load games from 2006
    start_date = "2006-09-01"
    games_query = f"""SELECT game_id, game_datetime, open_line, home_score, away_score
                      FROM games
                      WHERE game_datetime >= '{start_date}'
                      AND open_line IS NOT NULL
                      AND home_score IS NOT NULL"""

    games = pd.read_sql_query(games_query, engine)
    games["game_datetime"] = pd.to_datetime(games["game_datetime"], format="mixed")
    print(f"Loaded {len(games)} games with betting lines")

    # Fast season calculation
    print("Calculating seasons...")
    games["season"] = games["game_datetime"].apply(fast_get_season)
    games["season_type"] = games["game_datetime"].apply(fast_get_season_type)

    # Filter to reg and post only (exclude offseason/None)
    games = games[games["season_type"].isin(["reg", "post"])].copy()
    print(f"Games with season type: {len(games)}")

    # Calculate vegas miss
    games["vegas_miss_abs"] = abs(
        (games["home_score"] - games["away_score"])
        - (-games["open_line"])
    )

    # Generate graph
    generate_reg_vs_post_graph(
        games,
        save=True,
        image_name="images/vegas_miss_by_week"
    )

    print("Done!")


if __name__ == "__main__":
    main()
