#!/usr/bin/env python
"""Generate the vegas_miss_abs.png graph for the README.

Uses a fast season calculation instead of the slow add_season_timeframe_info.
"""

import matplotlib
matplotlib.use('Agg')  # Use non-GUI backend

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from src.database import get_engine


def fast_get_season(game_datetime):
    """Fast season calculation - NBA season spans Oct-Jun, named by start year."""
    month = game_datetime.month
    year = game_datetime.year
    # NBA season starts in October, so Oct-Dec is start year, Jan-Sep is end year
    if month >= 10:  # Oct, Nov, Dec
        start_year = year
    else:  # Jan-Sep
        start_year = year - 1
    return f"{start_year}-{start_year + 1}"


def fast_get_season_type(game_datetime):
    """Fast determination of regular vs postseason.

    Regular season: roughly Oct - mid-April
    Postseason: mid-April - June (and 2020 COVID bubble)
    """
    month = game_datetime.month
    day = game_datetime.day
    year = game_datetime.year

    # Special case: 2020 COVID bubble (July-Oct 2020 = 2019-2020 postseason)
    if year == 2020 and month in [7, 8, 9, 10]:
        return "post"

    # Postseason is roughly mid-April through June
    if month in [5, 6]:  # May, June - definitely playoffs
        return "post"
    elif month == 4 and day >= 15:  # Mid-April onwards
        return "post"
    elif month in [7, 8, 9]:  # Off-season
        return None
    else:
        return "reg"


def vegas_miss_graph(vegas_miss_abs, season, save=False, image_name=None):
    """Generate the Vegas miss graph with improved formatting."""
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.set_title(
        "Vegas Spread Accuracy",
        fontsize=24,
        pad=16,
        fontweight="bold",
    )
    ax.set_xlabel("Season", fontsize=18, labelpad=8, fontweight="bold")
    ax.set_ylabel("Average Miss Per Game (Points)", fontsize=18, labelpad=8, fontweight="bold")

    # Create a new DataFrame from the two Series
    df = pd.DataFrame({"vegas_miss_abs": vegas_miss_abs, "season": season})
    df = df.dropna()

    # Sort and aggregate by season
    season_avg = df.groupby("season")["vegas_miss_abs"].mean().reset_index()
    season_avg = season_avg.sort_values("season")

    # Calculate overall mean value
    overall_avg = df["vegas_miss_abs"].mean()

    # Plot
    sns.lineplot(
        x="season", y="vegas_miss_abs", data=season_avg, ax=ax, linewidth=4, marker="o"
    )

    ax.axhline(overall_avg, color="#b33040", linestyle="--", linewidth=2)
    ax.text(
        x=0,
        y=overall_avg + 0.05,
        s=f"Overall Average: {overall_avg:.2f}",
        color="#b33040",
        fontsize=16,
        fontweight="bold",
    )

    # Format x-tick labels - full XXXX-XXXX format
    unique_seasons = season_avg["season"].tolist()

    ax.set_xticks(range(len(unique_seasons)))
    ax.set_xticklabels(unique_seasons, rotation=45, ha="right")

    plt.xticks(fontsize=11)
    plt.yticks(fontsize=16)
    plt.tight_layout()

    if save:
        plt.savefig(f"{image_name}.png", dpi=300, bbox_inches="tight")
        print(f"Saved to {image_name}.png")


def main():
    print("Starting graph generation...")

    # Setup
    sns.set_theme()
    sns.set_style()
    sns.set_context("notebook")

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

    # Fast season calculation (vectorized)
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

    print(f"Seasons: {games_reg['season'].min()} to {games_reg['season'].max()}")

    # Generate the graph
    vegas_miss_graph(
        games_reg["vegas_miss_abs"],
        games_reg["season"],
        save=True,
        image_name="images/vegas_miss_abs",
    )

    print("Done!")


if __name__ == "__main__":
    main()
