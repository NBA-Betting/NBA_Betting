"""General utilities: season date lookups and helpers."""

from datetime import datetime

import pandas as pd

from src.config import NBA_IMPORTANT_DATES


def find_season_information(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # Iterate over each season
    for season, info in NBA_IMPORTANT_DATES.items():
        reg_season_start_date = datetime.strptime(info["reg_season_start_date"], "%Y-%m-%d")
        reg_season_end_date = datetime.strptime(info["reg_season_end_date"], "%Y-%m-%d")
        postseason_start_date = datetime.strptime(info["postseason_start_date"], "%Y-%m-%d")
        postseason_end_date = datetime.strptime(info["postseason_end_date"], "%Y-%m-%d")

        # Check if date_obj falls within this season's regular or postseason
        if reg_season_start_date <= date_obj <= postseason_end_date:
            season_info = {
                "date": date_str,
                "season": season,
                "year1": season.split("-")[0],  # Getting the first part of the season
                "year2": season.split("-")[1],  # Getting the second part of the season
                "reg_season_start_date": info["reg_season_start_date"],
                "reg_season_end_date": info["reg_season_end_date"],
                "postseason_start_date": info["postseason_start_date"],
                "postseason_end_date": info["postseason_end_date"],
            }

            # Determine whether it's the regular season or playoffs
            if reg_season_start_date <= date_obj <= reg_season_end_date:
                season_info["season_type"] = "Regular Season"
            elif postseason_start_date <= date_obj <= postseason_end_date:
                season_info["season_type"] = "Playoffs"
            else:
                season_info["season_type"] = "Other"

            return season_info

    # If no matching season is found, raise an error
    raise ValueError(f"Could not find season information for date: {date_str}.")


def determine_season_type(date, important_dates):
    date = pd.to_datetime(date)
    day_of_week = date.dayofweek  # Day of week as an integer (Monday=0, Sunday=6)
    month = date.month  # Month as January=1, December=12

    for season, dates in important_dates.items():
        reg_start_date = pd.to_datetime(dates["reg_season_start_date"])
        reg_end_date = pd.to_datetime(dates["reg_season_end_date"])
        post_start_date = pd.to_datetime(dates["postseason_start_date"])
        post_end_date = pd.to_datetime(dates["postseason_end_date"])

        if reg_start_date <= date <= reg_end_date:
            month_of_season = (date.to_period("M") - reg_start_date.to_period("M")).n + 1
            week_of_season = (date.to_period("W-SUN") - reg_start_date.to_period("W-SUN")).n + 1
            return pd.Series([season, "reg", day_of_week, month, month_of_season, week_of_season])
        elif post_start_date <= date <= post_end_date:
            month_of_season = (date.to_period("M") - post_start_date.to_period("M")).n + 1
            week_of_season = (date.to_period("W-SUN") - post_start_date.to_period("W-SUN")).n + 1
            return pd.Series([season, "post", day_of_week, month, month_of_season, week_of_season])

    return pd.Series([None, None, None, None, None, None])


def add_season_timeframe_info(df):
    season_info_columns = df["game_datetime"].apply(
        lambda x: determine_season_type(x, NBA_IMPORTANT_DATES)
    )
    season_info_columns.columns = [
        "season",
        "season_type",
        "day_of_week",
        "month",
        "month_of_season",
        "week_of_season",
    ]
    df = pd.concat([df, season_info_columns], axis=1)
    return df
