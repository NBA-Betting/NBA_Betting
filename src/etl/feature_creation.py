import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from scipy.stats import zscore

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))
sys.path.append(os.path.join(here, ".."))
import config
from data_sources.data_source_utils import find_season_information


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

    def _create_features_team_fivethirtyeight_games(self, df):
        return df

    def _create_features_team_nbastats_general_traditional(self, df):
        table_info = config.FEATURE_TABLE_INFO["team_nbastats_general_traditional"]
        return self._zscore_and_percentiles(
            df, table_info["feature_columns"], table_info["date_column"]
        )

    def _create_features_team_nbastats_general_advanced(self, df):
        table_info = config.FEATURE_TABLE_INFO["team_nbastats_general_advanced"]
        return self._zscore_and_percentiles(
            df, table_info["feature_columns"], table_info["date_column"]
        )

    def _create_features_team_nbastats_general_fourfactors(self, df):
        table_info = config.FEATURE_TABLE_INFO["team_nbastats_general_fourfactors"]
        return self._zscore_and_percentiles(
            df, table_info["feature_columns"], table_info["date_column"]
        )

    def _create_features_team_nbastats_general_opponent(self, df):
        table_info = config.FEATURE_TABLE_INFO["team_nbastats_general_opponent"]
        return self._zscore_and_percentiles(
            df, table_info["feature_columns"], table_info["date_column"]
        )

    # HELPER METHODS

    def _zscore_and_percentiles(self, df, all_feature_cols, date_col):
        def safe_zscore(s, ddof=1):
            if s.isnull().any() or s.std(ddof=ddof) == 0:
                return pd.Series([None] * len(s), index=s.index)
            else:
                return (s - s.mean()) / s.std(ddof=ddof)

        def safe_percentile(s):
            if s.isnull().any():
                return pd.Series([None] * len(s), index=s.index)
            else:
                return s.rank(pct=True)

        zscore_cols = all_feature_cols
        for col in zscore_cols:
            df[col + "_zscore"] = df.groupby([date_col])[col].transform(
                lambda x: safe_zscore(x, ddof=1)
            )
        percentile_cols = all_feature_cols
        for col in percentile_cols:
            df[col + "_percentile"] = df.groupby([date_col])[col].transform(
                lambda x: safe_percentile(x)
            )
        return df


class FeatureCreationPostMerge:
    def __init__(self, combined_features):
        self.combined_features = combined_features
        self.updated_combined_features = combined_features.copy()

    def full_feature_creation(self):
        self.updated_combined_features = self._add_day_of_season(self.combined_features)
        self.updated_combined_features = self._calculate_days_since_last_game(
            self.updated_combined_features
        )
        # self.updated_combined_features = self._calculate_team_performance_metrics(
        #     self.updated_combined_features
        # )
        # self.updated_combined_features = self._encode_home_team(
        #     self.updated_combined_features
        # )
        # self.updated_combined_features = self._encode_away_team(
        #     self.updated_combined_features
        # )

        return self.updated_combined_features

    def _add_day_of_season(self, df):
        df["day_of_season"] = df["game_datetime"].apply(
            lambda x: (
                x
                - datetime.strptime(
                    find_season_information(x.strftime("%Y-%m-%d"))[
                        "reg_season_start_date"
                    ],
                    "%Y-%m-%d",
                )
            ).days
        )
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

    # WARNING: This isn't working properly yet
    def _calculate_team_performance_metrics(self, df):
        df.sort_values(["season", "game_datetime"], inplace=True)

        # initializing new columns
        df["home_team_last_5"] = np.nan
        df["away_team_last_5"] = np.nan
        df["home_team_streak"] = np.nan
        df["away_team_streak"] = np.nan
        df["home_team_win_pct"] = np.nan
        df["away_team_win_pct"] = np.nan
        df["home_team_avg_point_diff"] = np.nan
        df["away_team_avg_point_diff"] = np.nan
        df["home_team_avg_point_diff_last_5"] = np.nan
        df["away_team_avg_point_diff_last_5"] = np.nan

        for season in df["season"].unique():
            season_df = df[df["season"] == season].copy()

            for team in pd.concat(
                [season_df["home_team"], season_df["away_team"]]
            ).unique():
                team_df = season_df[
                    (season_df["home_team"] == team) | (season_df["away_team"] == team)
                ].copy()

                # calculate last 5 games result and streak
                team_df["last_5_games_result"] = (
                    (team_df["home_team"] == team)
                    .rolling(window=5)
                    .sum()
                    .shift(1)
                    .fillna(1)
                )
                team_df["streak"] = team_df["last_5_games_result"].diff().eq(0).groupby(
                    team_df["last_5_games_result"]
                    .ne(team_df["last_5_games_result"].shift())
                    .cumsum()
                ).cumsum() * team_df["last_5_games_result"].apply(
                    lambda x: 1 if x > 2.5 else -1
                )

                # calculate win percentage
                team_df["win"] = team_df["home_team"] == team
                team_df["win_pct"] = team_df["win"].expanding().mean().shift(1)

                # calculate average point differential
                team_df["point_diff"] = np.where(
                    team_df["home_team"] == team,
                    team_df["home_score"] - team_df["away_score"],
                    team_df["away_score"] - team_df["home_score"],
                )
                team_df["avg_point_diff"] = (
                    team_df["point_diff"].expanding().mean().shift(1)
                )
                team_df["avg_point_diff_last_5"] = (
                    team_df["point_diff"].rolling(window=5).mean().shift(1)
                )

                # When setting the values back into the main DataFrame, use index to ensure matching sizes
                home_indices = df[
                    (df["season"] == season) & (df["home_team"] == team)
                ].index
                df.loc[home_indices, "home_team_last_5"] = team_df.loc[
                    home_indices, "last_5_games_result"
                ]
                df.loc[home_indices, "home_team_streak"] = team_df.loc[
                    home_indices, "streak"
                ]
                df.loc[home_indices, "home_team_win_pct"] = team_df.loc[
                    home_indices, "win_pct"
                ]
                df.loc[home_indices, "home_team_avg_point_diff"] = team_df.loc[
                    home_indices, "avg_point_diff"
                ]
                df.loc[home_indices, "home_team_avg_point_diff_last_5"] = team_df.loc[
                    home_indices, "avg_point_diff_last_5"
                ]

                away_indices = df[
                    (df["season"] == season) & (df["away_team"] == team)
                ].index
                df.loc[away_indices, "away_team_last_5"] = team_df.loc[
                    away_indices, "last_5_games_result"
                ]
                df.loc[away_indices, "away_team_streak"] = team_df.loc[
                    away_indices, "streak"
                ]
                df.loc[away_indices, "away_team_win_pct"] = team_df.loc[
                    away_indices, "win_pct"
                ]
                df.loc[away_indices, "away_team_avg_point_diff"] = team_df.loc[
                    away_indices, "avg_point_diff"
                ]
                df.loc[away_indices, "away_team_avg_point_diff_last_5"] = team_df.loc[
                    away_indices, "avg_point_diff_last_5"
                ]

        df["last_5_hv"] = df["home_team_last_5"] - df["away_team_last_5"]
        df["streak_hv"] = df["home_team_streak"] - df["away_team_streak"]
        df["win_pct_hv"] = df["home_team_win_pct"] - df["away_team_win_pct"]
        df["point_diff_hv"] = (
            df["home_team_avg_point_diff"] - df["away_team_avg_point_diff"]
        )
        df["point_diff_last_5_hv"] = (
            df["home_team_avg_point_diff_last_5"] - df["away_team_avg_point_diff_last_5"]
        )

        return df
