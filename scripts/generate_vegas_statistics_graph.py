#!/usr/bin/env python
"""Generate the vegas_miss_statistics.png graph for the README."""

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
    if month in [5, 6]:
        return "post"
    elif month == 4 and day >= 15:
        return "post"
    elif month in [7, 8, 9]:
        return None
    else:
        return "reg"


def get_era(start_year):
    """Classify season into era based on start year."""
    if start_year <= 2015:
        return "Traditional Era"
    elif start_year <= 2019:
        return "Three-Point Revolution"
    else:
        return "Modern Variance"


def generate_era_graph(games_reg, save=False, image_name=None):
    """Generate the Vegas Spread Accuracy by Era graph."""

    # Extract start year from season
    games_reg = games_reg.copy()
    games_reg["start_year"] = games_reg["season"].apply(lambda x: int(x.split("-")[0]))
    games_reg["era"] = games_reg["start_year"].apply(get_era)

    # Calculate stats by era
    era_stats = games_reg.groupby("era").agg(
        avg_miss=("vegas_miss_abs", "mean"),
        game_count=("vegas_miss_abs", "count"),
        min_year=("start_year", "min"),
        max_year=("start_year", "max")
    ).reset_index()

    # Create year range labels
    era_stats["year_range"] = era_stats.apply(
        lambda x: f"{x['min_year']}-{x['max_year'] + 1}", axis=1
    )

    # Order eras: oldest on top (index 0), newest on bottom
    era_order = ["Traditional Era", "Three-Point Revolution", "Modern Variance"]
    era_stats["era_order"] = era_stats["era"].apply(lambda x: era_order.index(x))
    era_stats = era_stats.sort_values("era_order")  # oldest first in dataframe
    era_stats = era_stats.iloc[::-1].reset_index(drop=True)  # reverse so oldest is at top (higher y)

    # NBA-inspired colors (balanced)
    NBA_BLUE = "#1a5490"
    NBA_RED = "#b33040"
    colors = [NBA_RED, NBA_BLUE, NBA_BLUE]  # Modern, Three-Point, Traditional (bottom to top)

    # Create figure
    sns.set_theme()
    sns.set_style()
    sns.set_context("notebook")

    fig, ax = plt.subplots(figsize=(14, 6))

    # Create horizontal bars with NBA colors
    y_positions = np.arange(len(era_stats))
    bars = ax.barh(y_positions, era_stats["avg_miss"], height=0.6, color=colors)

    # Style the chart
    ax.set_title("Vegas Spread Accuracy by Era", fontsize=24, pad=16, fontweight="bold")
    ax.set_xlabel("Average Miss Per Game (Points)", fontsize=18, labelpad=8, fontweight="bold")

    # Set y-axis tick labels to year ranges
    ax.set_yticks(y_positions)
    ax.set_yticklabels(era_stats["year_range"], fontsize=14, fontweight="bold")
    ax.set_ylabel("")

    # Fixed x-positions for aligned text
    era_name_x = 5.5  # All era names aligned toward center
    game_count_x = 0.15  # All game counts at left edge of bars

    # Add era names and stats on each bar
    for i, (idx, row) in enumerate(era_stats.iterrows()):
        bar_width = row["avg_miss"]

        # Era name - vertically aligned at fixed x position
        ax.text(era_name_x, i, row["era"],
                ha="center", va="center", fontsize=16, fontweight="bold", color="white")

        # Game count - centered vertically, fixed horizontal position
        ax.text(game_count_x, i, f"{row['game_count']:,} games",
                ha="left", va="center", fontsize=14, fontweight="bold", color="white")

        # Average value - outside the bar to the right
        ax.text(bar_width + 0.15, i, f"{row['avg_miss']:.2f}",
                ha="left", va="center", fontsize=14, fontweight="bold", color="black")

    # Set x-axis limits (add space for values outside bars)
    ax.set_xlim(0, max(era_stats["avg_miss"]) + 1.5)

    # Style ticks
    plt.xticks(fontsize=14)

    # Remove top and right spines
    sns.despine(left=True)

    plt.tight_layout()

    if save:
        plt.savefig(f"{image_name}.png", dpi=300, bbox_inches="tight")
        print(f"Saved to {image_name}.png")


def main():
    print("Starting statistics graph generation...")

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

    # Filter to regular season only
    games_reg = games[games["season_type"] == "reg"].copy()
    print(f"Regular season games: {len(games_reg)}")

    # Calculate vegas miss
    games_reg["vegas_miss_abs"] = abs(
        (games_reg["home_score"] - games_reg["away_score"])
        - (-games_reg["open_line"])
    )

    # Generate era graph
    generate_era_graph(
        games_reg,
        save=True,
        image_name="images/vegas_miss_statistics"
    )

    print("Done!")


if __name__ == "__main__":
    main()
