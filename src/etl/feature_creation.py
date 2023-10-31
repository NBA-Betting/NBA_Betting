import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))
sys.path.append(os.path.join(here, ".."))

from config import FEATURE_TABLE_INFO
from utils.general_utils import find_season_information


class FeatureCreationPreMerge:
    def __init__(self, features_tables):
        self.features_tables = features_tables
        self.updated_features_tables = features_tables.copy()

    def full_feature_creation(self):
        for table_name in self.features_tables:
            try:
                method = getattr(self, f"_create_features_{table_name}")
            except AttributeError:
                print(f"No feature creation method for table: {table_name}")
                continue
            else:
                self.updated_features_tables[table_name] = method(
                    self.features_tables[table_name]
                )
        return self.updated_features_tables

    # FEATURE CREATION METHODS
    def _create_features_team_nbastats_general_traditional(self, df):
        table_info = FEATURE_TABLE_INFO["team_nbastats_general_traditional"]
        return self._zscore_and_percentiles(
            df, table_info["feature_columns"], table_info["date_column"]
        )

    def _create_features_team_nbastats_general_advanced(self, df):
        table_info = FEATURE_TABLE_INFO["team_nbastats_general_advanced"]
        return self._zscore_and_percentiles(
            df, table_info["feature_columns"], table_info["date_column"]
        )

    def _create_features_team_nbastats_general_fourfactors(self, df):
        table_info = FEATURE_TABLE_INFO["team_nbastats_general_fourfactors"]
        return self._zscore_and_percentiles(
            df, table_info["feature_columns"], table_info["date_column"]
        )

    def _create_features_team_nbastats_general_opponent(self, df):
        table_info = FEATURE_TABLE_INFO["team_nbastats_general_opponent"]
        return self._zscore_and_percentiles(
            df, table_info["feature_columns"], table_info["date_column"]
        )

    # HELPER METHODS

    def _zscore_and_percentiles(self, df, all_feature_cols, date_col):
        def safe_zscore(s, ddof=1):
            mean = s.mean()
            std = s.std(ddof=ddof)
            if std == 0:
                return pd.Series([np.nan] * len(s), index=s.index)
            return (s - mean) / std

        def safe_percentile(s):
            return s.rank(pct=True)

        for col in all_feature_cols:
            df[col + "_zscore"] = (
                df.groupby(date_col)[col].transform(safe_zscore).where(df[col].notnull())
            )
            df[col + "_percentile"] = (
                df.groupby(date_col)[col]
                .transform(safe_percentile)
                .where(df[col].notnull())
            )

        return df


class FeatureCreationPostMerge:
    def __init__(self, combined_features):
        self.updated_combined_features = combined_features.copy()

    def full_feature_creation(self):
        self.updated_combined_features = self._add_season_timeframe_info(
            self.updated_combined_features
        )
        self.updated_combined_features = self._add_day_of_season(
            self.updated_combined_features
        )
        self.updated_combined_features = self._calculate_days_since_last_game(
            self.updated_combined_features
        )
        self.updated_combined_features = self._calculate_team_performance_metrics(
            self.updated_combined_features
        )
        self.updated_combined_features = self._encode_home_team(
            self.updated_combined_features
        )
        self.updated_combined_features = self._encode_away_team(
            self.updated_combined_features
        )

        self.updated_combined_features = self.updated_combined_features.drop(
            columns=["reg_season_start_date"]
        )

        return self.updated_combined_features

    def _add_season_timeframe_info(self, df):
        # Convert the 'game_datetime' to a string format and apply 'find_season_information'
        season_info_series = (
            df["game_datetime"].dt.strftime("%Y-%m-%d").apply(find_season_information)
        )

        # Extract relevant info from the dictionaries and add as new columns
        df["season"] = season_info_series.apply(lambda x: x["season"])
        df["season_type"] = season_info_series.apply(lambda x: x["season_type"])
        df["reg_season_start_date"] = season_info_series.apply(
            lambda x: x["reg_season_start_date"]
        )
        df["reg_season_start_date"] = pd.to_datetime(df["reg_season_start_date"])
        return df

    def _add_day_of_season(self, df):
        df["day_of_season"] = (
            df["game_datetime"] - df["reg_season_start_date"]
        ).dt.days + 1
        return df

    def _encode_home_team(self, df):
        dummies = pd.get_dummies(df["home_team"], prefix="home")
        df = pd.concat([df, dummies], axis=1)
        return df

    def _encode_away_team(self, df):
        dummies = pd.get_dummies(df["away_team"], prefix="away")
        df = pd.concat([df, dummies], axis=1)
        return df

    def _calculate_days_since_last_game(self, df):
        # Create two copies of the dataframe, one for home games, one for away games
        home_df = df[["game_datetime", "season", "home_team"]].rename(
            columns={"home_team": "team"}
        )
        away_df = df[["game_datetime", "season", "away_team"]].rename(
            columns={"away_team": "team"}
        )

        # Concatenate along the row axis (i.e., append the dataframes one below the other)
        all_games_df = pd.concat([home_df, away_df], axis=0)

        # Sort the dataframe by team and date
        all_games_df.sort_values(["team", "game_datetime"], inplace=True)

        # Initialize an empty list to store the intermediate dataframes
        df_list = []

        # Loop over each season
        for season in all_games_df["season"].unique():
            # Filter the dataframe for the current season
            season_df = all_games_df[all_games_df["season"] == season].copy()

            # Calculate the days since the last game for each team
            season_df["days_since_last_game"] = (
                season_df.groupby("team")["game_datetime"].diff().dt.days
            )

            # Append the results to the df_list
            df_list.append(season_df)

        # Concatenate all the intermediate dataframes in df_list into a single dataframe
        df_result = pd.concat(df_list, axis=0)

        # Merge the original dataframe with the result dataframe
        df = pd.merge(
            df,
            df_result,
            how="left",
            left_on=["game_datetime", "season", "home_team"],
            right_on=["game_datetime", "season", "team"],
        )
        df = pd.merge(
            df,
            df_result,
            how="left",
            left_on=["game_datetime", "season", "away_team"],
            right_on=["game_datetime", "season", "team"],
            suffixes=("_home", "_away"),
        )

        # Drop the unnecessary 'team' columns
        df.drop(columns=["team_home", "team_away"], inplace=True)
        df["rest_diff_hv"] = (
            df["days_since_last_game_home"] - df["days_since_last_game_away"]
        )
        return df

    def _calculate_team_performance_metrics(self, inbound_df):
        df = inbound_df.copy()
        df.sort_values(["season", "game_datetime"], inplace=True)

        # initializing new columns
        metrics_columns = [
            "home_team_last_5_games_result",
            "away_team_last_5_games_result",
            "home_team_streak",
            "away_team_streak",
            "home_team_win_pct",
            "away_team_win_pct",
            "home_team_avg_point_diff",
            "away_team_avg_point_diff",
            "home_team_avg_point_diff_last_5",
            "away_team_avg_point_diff_last_5",
        ]
        for col in metrics_columns:
            df[col] = np.nan

        # Loop through each season
        # Metrics reset at the beginning of each season
        # print(df.info(verbose=True, max_cols=1000, show_counts=True))
        for season in df["season"].unique():
            season_df = df.loc[df["season"] == season].copy()

            # Loop through each team
            # Metrics are calculated for each team
            for team in pd.concat(
                [season_df["home_team"], season_df["away_team"]]
            ).unique():
                mask = (season_df["home_team"] == team) | (
                    season_df["away_team"] == team
                )
                team_df = season_df[mask].copy()
                team_df["team"] = team

                # Calculate the win/loss performance of the team in each game
                conditions = [
                    (~team_df["game_completed"]),
                    (
                        (team_df["home_team"] == team)
                        & (team_df["home_score"] > team_df["away_score"])
                    ),
                    (
                        (team_df["away_team"] == team)
                        & (team_df["away_score"] > team_df["home_score"])
                    ),
                    (
                        (team_df["home_team"] == team)
                        & (team_df["home_score"] < team_df["away_score"])
                    ),
                    (
                        (team_df["away_team"] == team)
                        & (team_df["away_score"] < team_df["home_score"])
                    ),
                ]

                choices = [0, 1, 1, -1, -1]

                team_df["performance"] = np.select(conditions, choices, default=np.nan)

                # Calculate the results of the last 5 games (excluding current game),
                # with a value even if there are fewer than 5 previous games.
                # Fill NaN with 0 for no prior games.
                team_df["last_5_games_result"] = (
                    team_df["performance"]
                    .rolling(window=5, min_periods=1)
                    .sum()
                    .shift(1)
                    .fillna(0)
                )

                # Initialize an empty list to store the streaks
                streaks = []

                # Initialize streak for the team
                current_streak = 0

                # Loop through each game for the team
                for i, row in team_df.iterrows():
                    # Append the current streak to the list
                    streaks.append(current_streak)

                    # Update the streak based on the game outcome
                    performance = row["performance"]

                    if (
                        performance == 0
                    ):  # If game is in progress, skip updating the streak
                        continue

                    if current_streak == 0:
                        current_streak = performance
                    elif np.sign(current_streak) == np.sign(performance):
                        current_streak += performance
                    else:
                        current_streak = performance

                # Add the calculated streaks to the DataFrame
                team_df["streak"] = streaks

                # Calculate win percentage (excluding current game)
                team_df["win_pct"] = team_df["performance"].expanding().mean().shift(1)

                # Calculate point differential
                team_df["point_diff"] = np.where(
                    team_df["home_team"] == team,
                    team_df["home_score"] - team_df["away_score"],
                    team_df["away_score"] - team_df["home_score"],
                )

                # Compute the average point differential over all games (excluding current game)
                team_df["avg_point_diff"] = (
                    team_df["point_diff"].expanding().mean().shift(1).fillna(0)
                )

                # Compute the average point differential over the last 5 games (excluding current game)
                team_df["avg_point_diff_last_5"] = (
                    team_df["point_diff"].rolling(window=5).mean().shift(1).fillna(0)
                )

                # Update the main dataframe with the calculated metrics
                update_cols = [
                    "last_5_games_result",
                    "streak",
                    "win_pct",
                    "avg_point_diff",
                    "avg_point_diff_last_5",
                ]

                for col in update_cols:
                    # For home team
                    home_mask = (df["home_team"] == team) & mask
                    df.loc[home_mask, f"home_team_{col}"] = team_df.loc[home_mask, col]

                    # For away team
                    away_mask = (df["away_team"] == team) & mask
                    df.loc[away_mask, f"away_team_{col}"] = team_df.loc[away_mask, col]

        # Compute the "home view" metrics
        df["last_5_hv"] = (
            df["home_team_last_5_games_result"] - df["away_team_last_5_games_result"]
        )
        df["streak_hv"] = df["home_team_streak"] - df["away_team_streak"]
        df["win_pct_hv"] = df["home_team_win_pct"] - df["away_team_win_pct"]
        df["point_diff_hv"] = (
            df["home_team_avg_point_diff"] - df["away_team_avg_point_diff"]
        )
        df["point_diff_last_5_hv"] = (
            df["home_team_avg_point_diff_last_5"] - df["away_team_avg_point_diff_last_5"]
        )

        return df


if __name__ == "__main__":
    pass
