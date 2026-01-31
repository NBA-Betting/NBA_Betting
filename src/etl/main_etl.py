"""
ETL pipeline: loads games + stats, engineers features, saves to all_features_json.

Usage: python -m src.etl.main_etl
"""

import json
from datetime import timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.etl.feature_creation import FeatureCreationPostMerge, FeatureCreationPreMerge
from src import config
from src.database import get_engine
from src.utils.timezone import get_current_date

pd.set_option("display.max_columns", 200)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)


class ETLPipeline:
    """Loads data, merges features, and saves to database."""

    def __init__(
        self,
        start_date,
        config=config,
    ):
        self.database_engine = get_engine()
        self.config = config
        self.start_date = start_date
        self.game_data = self.load_game_data()
        self.features_data = {}
        self.combined_features = None

    # DATA LOADING

    def load_table(self, table_name, date_column, columns_to_load=None, start_date=None):
        if columns_to_load is None:
            columns = "*"
        else:
            columns = ", ".join(columns_to_load)
        if start_date is None:
            start_date = self.start_date

        todays_date = get_current_date()
        # Include tomorrow's games so predictions can be generated for them
        day_after_tomorrow = todays_date + timedelta(days=2)

        # Use parameterized query to prevent SQL injection
        # Note: table_name, columns, and date_column come from internal config (trusted)
        # but start_date and tomorrows_date are parameterized for safety
        query = text(
            f"SELECT {columns} FROM {table_name} "
            f"WHERE {date_column} >= :start_date AND {date_column} < :end_date"
        )
        # Use engine directly for pandas 2.x compatibility with SQLite
        # Don't use parse_dates - SQLite stores dates as TEXT with mixed formats
        df = pd.read_sql_query(
            query,
            self.database_engine,
            params={"start_date": str(start_date), "end_date": str(day_after_tomorrow)},
        )
        # Convert date column manually with mixed format support (handles both
        # '2024-01-01 12:00:00' and '2024-01-01 12:00:00.000000' formats)
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], format="mixed")
        return df

    def load_game_data(self):
        features = [
            "game_id",
            "game_datetime",
            "home_team",
            "away_team",
            "open_line",
            "home_score",
            "away_score",
            "game_completed",
        ]
        try:
            game_data = self.load_table("games", "game_datetime", features)
            print(f"  Loaded {game_data.shape[0]} games")
            return game_data
        except Exception as e:
            print("  ERROR: Failed to load game data")
            raise e

    def load_features_data(self, table_names):
        features_data = {}
        for table_name in table_names:
            try:
                date_column = self.config.FEATURE_TABLE_INFO[table_name]["date_column"]
                columns = (
                    self.config.FEATURE_TABLE_INFO[table_name]["info_columns"]
                    + self.config.FEATURE_TABLE_INFO[table_name]["feature_columns"]
                )
                features_data[table_name] = self.load_table(table_name, date_column, columns)
            except Exception as e:
                print(f"  ERROR: Failed to load {table_name}")
                raise e
        self.features_data = features_data
        total_rows = sum(v.shape[0] for v in features_data.values())
        print(f"  Loaded {total_rows} stat records from {len(features_data)} tables")

    # DATA CLEANING and PREPARATION
    def prepare_table(self, table, table_name):
        working_table = table.copy()

        # Standardize Team Names
        working_table = self._standardize_teams_in_dataframe(working_table, table_name)

        # Manage Duplicate Rows
        working_table = self._check_duplicates(working_table, table_name)

        # Downcast Data Types
        working_table, _ = self.downcast_data_types(working_table, downcast_floats=True)

        self.features_data[table_name] = working_table

    def prepare_all_tables(self):
        for table_name, table in self.features_data.items():
            self.prepare_table(table, table_name)
        # Silent - preparation is internal step

    def feature_creation_pre_merge(self):
        self.features_data = FeatureCreationPreMerge(self.features_data).full_feature_creation()
        # Silent - feature creation is internal step

    def merge_features_data(self):
        self.combined_features = self._merge_game_to_nbastats_team(
            self.game_data,
            {
                "team_nbastats_general_traditional": self.features_data[
                    "team_nbastats_general_traditional"
                ],
                "team_nbastats_general_advanced": self.features_data[
                    "team_nbastats_general_advanced"
                ],
                "team_nbastats_general_fourfactors": self.features_data[
                    "team_nbastats_general_fourfactors"
                ],
                "team_nbastats_general_opponent": self.features_data[
                    "team_nbastats_general_opponent"
                ],
            },
        )
        # Silent - merge is internal step

    def feature_creation_post_merge(self):
        self.combined_features = FeatureCreationPostMerge(
            self.combined_features
        ).full_feature_creation()
        # Silent - feature creation is internal step

    def clean_and_save_combined_features(self):
        self.combined_features, info = self.check_duplicates(
            self.combined_features, "game_id", filter=False
        )
        if info["num_duplicate_keys"] > 0:
            raise Exception("\n*** Duplicate Game IDs Found Before Saving")

        self.combined_features, info = self.downcast_data_types(
            self.combined_features, downcast_floats=True
        )

        self._save_as_jsonb(self.combined_features)
        print(
            f"  Created {self.combined_features.shape[1]} features for {self.combined_features.shape[0]} games"
        )

    def _save_as_jsonb(self, df):
        """Save combined features to the all_features_json table (SQLite compatible)."""
        try:
            # Work on a defragmented copy to avoid PerformanceWarning
            df = df.copy()

            # Convert datetime to string format
            df["game_datetime"] = df["game_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

            # Constructing the data column - convert each row to a dict
            df["data"] = df.apply(
                lambda row: {
                    key: (value if pd.notna(value) else None)
                    for key, value in row.items()
                    if key != "game_id"
                },
                axis=1,
            )

            # Convert the 'data' column directly to JSON string
            df["data"] = df["data"].apply(json.dumps)

            # Filter to keep only 'game_id' and 'data' columns
            df = df[["game_id", "data"]]

            # Save to temporary table
            df.to_sql("temp_table", self.database_engine, if_exists="replace", index=False)

            with self.database_engine.begin() as connection:
                # SQLite-compatible upsert using INSERT OR REPLACE
                # Note: Removed ::jsonb cast (SQLite handles JSON as text)
                query = text("""
                    INSERT OR REPLACE INTO all_features_json (game_id, data)
                    SELECT game_id, data FROM temp_table
                """)

                connection.execute(query)

                drop_query = text("DROP TABLE temp_table;")
                connection.execute(drop_query)
        except Exception as e:
            print("\n*** Error Saving Combined Features to JSON Table")
            raise e

    def _merge_game_to_nbastats_team(self, game, nbastats_team_dfs):
        # Create a copy of the game DataFrame to avoid modifying the original
        features_df = game.copy()
        # Create game_date string column for merging
        features_df["game_date"] = features_df["game_datetime"].dt.date.astype("str")
        # Creating a merge_date column within each nbastats_team_df
        # merge_date = to_date + 1 day (stats are from day before the game)
        for table_name, table in nbastats_team_dfs.items():
            table["merge_date"] = (
                pd.to_datetime(table["to_date"]) + pd.Timedelta(days=1)
            ).dt.strftime("%Y-%m-%d")

        for table_name, table in nbastats_team_dfs.items():
            for team in ["home", "away"]:
                for game_set in ["all", "l2w"]:
                    # Defragment DataFrame to prevent PerformanceWarning
                    # The pd.concat inside merge_asof loop causes fragmentation
                    features_df = features_df.copy()

                    sub_df = table.loc[table.games == game_set].copy()

                    # Rename columns to avoid duplicate column names
                    table_suffix = table_name.split("_")[-1]
                    columns_to_not_rename = [
                        "team_name",
                        "to_date",
                        "merge_date",
                        "games",
                    ]
                    sub_df.columns = [
                        (
                            f"{col}_{team}_{game_set}_{table_suffix}"
                            if col not in columns_to_not_rename
                            else col
                        )
                        for col in sub_df.columns
                    ]

                    # Use merge_asof for point-in-time correct joining
                    # This finds the most recent stats BEFORE each game date
                    # Convert date strings to datetime for merge_asof (requires numeric/datetime keys)
                    features_df["_game_date_dt"] = pd.to_datetime(features_df["game_date"])
                    sub_df["_merge_date_dt"] = pd.to_datetime(sub_df["merge_date"])

                    features_df = features_df.sort_values("_game_date_dt").reset_index(drop=True)

                    # For each team in the game, find their most recent stats
                    # We need to do this per-team since merge_asof requires sorted keys
                    merged_rows = []
                    for team_name in features_df[f"{team}_team"].unique():
                        game_rows = features_df[features_df[f"{team}_team"] == team_name].copy()
                        team_stats = sub_df[sub_df["team_name"] == team_name].copy()

                        if len(team_stats) > 0:
                            team_stats = team_stats.sort_values("_merge_date_dt")
                            merged = pd.merge_asof(
                                game_rows,
                                team_stats.drop(columns=["team_name"]),
                                left_on="_game_date_dt",
                                right_on="_merge_date_dt",
                                direction="backward",
                                suffixes=("", f"_{table_name}_{team}_{game_set}"),
                            )
                        else:
                            merged = game_rows
                        merged_rows.append(merged)

                    if merged_rows:
                        features_df = pd.concat(merged_rows, ignore_index=True)

                    # Clean up temporary datetime columns
                    features_df = features_df.drop(columns=["_game_date_dt"], errors="ignore")
                    if "_merge_date_dt" in features_df.columns:
                        features_df = features_df.drop(columns=["_merge_date_dt"])

                    # Drop the columns that were used for merging
                    columns_to_drop = [
                        "to_date",
                        "merge_date",
                        "team_name",
                        "games",
                    ]

                    # Check and drop only the columns that exist in the DataFrame
                    columns_to_drop = [col for col in columns_to_drop if col in features_df.columns]
                    # Now, safely drop the columns
                    features_df = features_df.drop(columns=columns_to_drop)

        features_df = features_df.drop(columns=["game_date"])

        # Defragment the DataFrame after the merge loop
        # The repeated merge operations cause memory fragmentation
        features_df = features_df.copy()

        return features_df

    def _standardize_teams_in_dataframe(self, df, table_name):
        possible_team_columns = [
            "home_team",
            "away_team",
            "team",
            "opponent",
            "team_name",
            "team1",
            "team2",
        ]
        try:
            columns_to_standardize = [
                column for column in possible_team_columns if column in df.columns
            ]
            if len(columns_to_standardize) == 0:
                print(f"\n---No Team Columns Found for {table_name}")
                # raise Exception(f"***No Team Columns Found for {table_name}")
            else:
                df, info = self.standardize_team_names(
                    df,
                    columns_to_standardize,
                    self.config.TEAM_MAP,
                    filter=True,
                )
                if info["rows_removed"] > 0:
                    print(
                        f"***{info['rows_removed']} Rows Removed from {table_name} due to Unknown Team Names: {info['not_found']}"
                    )
                    # raise Exception("Unknown Team Names")
        except Exception as e:
            print(f"\n*** Error Standardizing Team Names for {table_name}")
            raise e

        return df

    def _check_duplicates(self, df, table_name):
        try:
            primary_key = self.config.FEATURE_TABLE_INFO[table_name]["primary_key"]
            df, info = self.check_duplicates(df, primary_key, filter=True)
            if info["num_non_perfect_duplicates"] > 0:
                print(
                    f"\n***{info['num_non_perfect_duplicates']} Non-Perfect Duplicates Removed from {table_name}"
                )
                raise Exception("Non-Perfect Duplicates")

        except Exception as e:
            print(f"\n*** Error Checking for Duplicates in {table_name}")
            raise e

        return df

    @staticmethod
    def standardize_team_names(df, columns, mapping, filter=False, print_details=False):
        # Step 1: Create a copy of the relevant columns
        original_values = df[columns].copy()

        # Replace team names in the DataFrame with the standardized names from the mapping
        df[columns] = df[columns].replace(mapping)

        info = {"not_found": [], "rows_removed": 0, "rows_updated": 0}

        # Find team names that are in the DataFrame but not in the mapping
        for column in columns:
            unique_teams = df[column].unique()
            for team in unique_teams:
                if team not in mapping:
                    info["not_found"].append(team)

        info["not_found"] = list(set(info["not_found"]))

        # Step 2: Compare original values to updated values to find the number of rows updated
        for column in columns:
            info["rows_updated"] += (original_values[column] != df[column]).sum()

        # Store the initial number of rows
        initial_rows = df.shape[0]

        if filter:
            # Remove rows where the team names are not found in the mapping
            for column in columns:
                df = df[df[column].isin(mapping.keys())]

            # Calculate the number of rows removed
            info["rows_removed"] = initial_rows - df.shape[0]

        if print_details:
            print(f"Columns: {columns}")
            print(f"Initial Rows: {initial_rows}")
            print(f"Rows Updated: {info['rows_updated']}")
            print(f"Rows Removed: {info['rows_removed']}")
            print(f"Final Rows: {df.shape[0]}")

            return df
        else:
            return df, info

    @staticmethod
    def check_duplicates(df, primary_key, filter=False, print_details=False):
        # if primary_key is a single string, make it a list
        if isinstance(primary_key, str):
            primary_key = [primary_key]

        # Check for duplicates based on primary keys
        duplicate_keys_df = df[df.duplicated(subset=primary_key, keep=False)]
        num_duplicate_keys = len(duplicate_keys_df)

        # Check for perfect duplicates
        perfect_duplicates_df = df[df.duplicated(keep=False)]
        num_perfect_duplicates = len(perfect_duplicates_df)

        # Calculate the number of non-perfect duplicates
        num_non_perfect_duplicates = num_duplicate_keys - num_perfect_duplicates

        # Filter to remove all duplicates based on the primary key if required
        if filter:
            df = df.drop_duplicates(subset=primary_key, keep="first")

        # Create the info dictionary
        info = {
            "num_duplicate_keys": num_duplicate_keys,
            "num_perfect_duplicates": num_perfect_duplicates,
            "num_non_perfect_duplicates": num_non_perfect_duplicates,
        }

        if print_details:
            print(f"Primary Key: {primary_key}")
            print(f"Number of Duplicate Keys: {num_duplicate_keys}")
            print(f"Number of Perfect Duplicates: {num_perfect_duplicates}")
            print(f"Number of Non-Perfect Duplicates: {num_non_perfect_duplicates}")
            return df
        else:
            return df, info

    @staticmethod
    def downcast_data_types(df, downcast_floats=True, print_details=False):
        # Function to convert 'None' to 'NaN'
        def none_to_nan(x):
            return x.apply(lambda elem: np.nan if elem is None else elem)

        # Information about memory usage
        info = {}
        mem_used_before = round(df.memory_usage(deep=True).sum() / 1024**2, 2)
        info["Before"] = {
            "Total (MB)": mem_used_before,
            "By Type (MB)": round(
                df.memory_usage(deep=True).groupby([df.dtypes]).sum() / 1024**2, 2
            ),
        }

        # Convert 'None' to 'NaN' across the entire DataFrame
        df = df.apply(none_to_nan)

        # Downcasting floats if required
        if downcast_floats:
            float_cols = df.select_dtypes(include=["float"]).columns
            df[float_cols] = df[float_cols].apply(lambda x: pd.to_numeric(x, downcast="float"))

        mem_used_after = round(df.memory_usage(deep=True).sum() / 1024**2, 2)
        info["After"] = {
            "Total (MB)": mem_used_after,
            "By Type (MB)": round(
                df.memory_usage(deep=True).groupby([df.dtypes]).sum() / 1024**2, 2
            ),
        }

        savings = round(mem_used_before - mem_used_after, 2)
        savings_pct = round((savings / mem_used_before) * 100, 2)  # Add precision for percentage
        info["Savings"] = f"{savings} MB ({savings_pct}%)"

        if print_details:
            print(f"Memory Usage Before (MB): {mem_used_before}")
            print(f"Memory Usage After (MB): {mem_used_after}")
            print(f"Savings (MB): {savings} ({savings_pct}%)")
            return df  # Consistency in return type
        else:
            return df, info

    @staticmethod
    def get_merge_statistics(merged_df, indicator_column="_merge"):
        # Total number of records in the merged dataframe
        total_records = len(merged_df)

        # Number of records from df1 only, df2 only, and both
        records_df1_only = len(merged_df[merged_df[indicator_column] == "left_only"])
        records_df2_only = len(merged_df[merged_df[indicator_column] == "right_only"])
        records_both = len(merged_df[merged_df[indicator_column] == "both"])

        # Calculate the percentages
        percentage_df1_only = records_df1_only / total_records * 100
        percentage_df2_only = records_df2_only / total_records * 100
        percentage_both = records_both / total_records * 100

        print(f"Total number of records: {total_records}")
        print(f"Number of records from df1 only: {records_df1_only} ({percentage_df1_only:.2f}%)")
        print(f"Number of records from df2 only: {records_df2_only} ({percentage_df2_only:.2f}%)")
        print(
            f"Number of records present in both df1 and df2: {records_both} ({percentage_both:.2f}%)"
        )


if __name__ == "__main__":
    start_date = "2020-09-01"
    ETL = ETLPipeline(start_date)

    # Use ACTIVE_FEATURE_TABLES from config for consistency
    ETL.load_features_data(config.ACTIVE_FEATURE_TABLES)

    ETL.prepare_all_tables()

    ETL.feature_creation_pre_merge()

    ETL.merge_features_data()

    ETL.feature_creation_post_merge()

    ETL.clean_and_save_combined_features()
