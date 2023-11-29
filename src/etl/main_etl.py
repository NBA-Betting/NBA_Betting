import json
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz
from dotenv import load_dotenv
from sqlalchemy import create_engine

from .feature_creation import FeatureCreationPostMerge, FeatureCreationPreMerge

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))

import config

load_dotenv()
DB_ENDPOINT = os.getenv("DB_ENDPOINT")
DB_PASSWORD = os.getenv("DB_PASSWORD")

pd.set_option("display.max_columns", 200)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)


class ETLPipeline:
    def __init__(
        self,
        start_date,
        DB_ENDPOINT=DB_ENDPOINT,
        DB_PASSWORD=DB_PASSWORD,
        config=config,
    ):
        self.database_engine = create_engine(
            f"postgresql://postgres:{DB_PASSWORD}@{DB_ENDPOINT}/nba_betting"
        )
        self.config = config
        self.start_date = start_date
        self.game_data = self.load_game_data()
        self.features_data = {}
        self.combined_features = None

    # DATA LOADING

    def load_table(
        self, table_name, date_column, columns_to_load=None, start_date=None
    ):
        if columns_to_load is None:
            columns = "*"
        else:
            columns = ", ".join(columns_to_load)
        if start_date is None:
            start_date = self.start_date

        todays_date = datetime.now(pytz.timezone("America/Denver")).date()
        tomorrows_date = todays_date + timedelta(days=1)

        query = f"SELECT {columns} FROM {table_name} WHERE {date_column} >= '{start_date}' AND {date_column} < '{tomorrows_date}';"
        with self.database_engine.connect() as connection:
            return pd.read_sql_query(query, connection, parse_dates=[date_column])

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
            print("\n+++ Game Data Loaded", game_data.shape)
            return game_data
        except Exception as e:
            print("\n*** Error Loading Game Data")
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
                features_data[table_name] = self.load_table(
                    table_name, date_column, columns
                )
            except Exception as e:
                print(f"\n*** Error Loading Feature Table: {table_name}")
                raise e
        self.features_data = features_data
        print("\n+++ Features Data Loaded")
        print("Tables Loaded: ")
        for k, v in features_data.items():
            print(k, v.shape)

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
        print("\n+++ All Tables Prepared")

    def feature_creation_pre_merge(self):
        self.features_data = FeatureCreationPreMerge(
            self.features_data
        ).full_feature_creation()
        print("\n+++ Feature Creation Pre-Merge Complete")
        print("Updated Tables:")
        for k, v in self.features_data.items():
            print(k, v.shape)

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
        print("\n+++ Features Data Merged with Game Data")
        print("Combined Data Shape:", self.combined_features.shape)

    def feature_creation_post_merge(self):
        self.combined_features = FeatureCreationPostMerge(
            self.combined_features
        ).full_feature_creation()
        print("\n+++ Feature Creation Post-Merge Complete")
        print("Updated Combined Features Shape:", self.combined_features.shape)

    def clean_and_save_combined_features(self):
        self.combined_features, info = self.check_duplicates(
            self.combined_features, "game_id", filter=False
        )
        if info["num_duplicate_keys"] > 0:
            raise Exception("\n*** Duplicate Game IDs Found Before Saving")

        self.combined_features, info = self.downcast_data_types(
            self.combined_features, downcast_floats=True
        )
        print("\n+++ Data Types Downcasted")
        print("Total Savings:", info["Savings"])

        # print(self.combined_features.info(verbose=True, null_counts=True, max_cols=1000))

        self._save_as_jsonb(self.combined_features)
        print("\n+++ Combined Features Saved to JSONB Table")

    def _save_as_jsonb(self, df):
        try:
            # Convert datetime to string format
            df["game_datetime"] = df["game_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

            # Constructing the data column
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
            df.to_sql(
                "temp_table", self.database_engine, if_exists="replace", index=False
            )

            with self.database_engine.begin() as connection:
                query = """
                    INSERT INTO all_features_json (game_id, data)
                    SELECT game_id, data::jsonb FROM temp_table
                    ON CONFLICT (game_id) 
                    DO UPDATE
                    SET data = excluded.data
                """

                result = connection.execute(query)

                # Optional: Check the number of rows affected
                # print(f"{result.rowcount} rows affected.")

                drop_query = "DROP TABLE temp_table;"
                connection.execute(drop_query)
        except Exception as e:
            print("\n*** Error Saving Combined Features as JSONB")
            raise e

    def _merge_game_to_nbastats_team(self, game, nbastats_team_dfs):
        # Create a copy of the game DataFrame to avoid modifying the original
        features_df = game.copy()
        # Create game_date string column for merging
        features_df["game_date"] = features_df["game_datetime"].dt.date.astype("str")
        # Creating a merge_date column within each nbastats_team_df
        for table_name, table in nbastats_team_dfs.items():
            table["merge_date"] = (
                table["to_date"].dt.date + pd.DateOffset(days=1)
            ).astype("str")

        for table_name, table in nbastats_team_dfs.items():
            for team in ["home", "away"]:
                for game_set in ["all", "l2w"]:
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
                        f"{col}_{team}_{game_set}_{table_suffix}"
                        if col not in columns_to_not_rename
                        else col
                        for col in sub_df.columns
                    ]

                    # Merge the sub_df to the combo_df
                    features_df = features_df.merge(
                        sub_df,
                        left_on=["game_date", f"{team}_team"],
                        right_on=["merge_date", "team_name"],
                        how="left",
                        suffixes=("", f"_{table_name}_{team}_{game_set}"),
                        validate="1:1",
                    )

                    # Drop the columns that were used for merging
                    columns_to_drop = [
                        "to_date",
                        "merge_date",
                        "team_name",
                        "games",
                    ]

                    # Check and drop only the columns that exist in the DataFrame
                    columns_to_drop = [
                        col for col in columns_to_drop if col in features_df.columns
                    ]
                    # Now, safely drop the columns
                    features_df = features_df.drop(columns=columns_to_drop)

        features_df = features_df.drop(columns=["game_date"])

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
            df[float_cols] = df[float_cols].apply(
                lambda x: pd.to_numeric(x, downcast="float")
            )

        mem_used_after = round(df.memory_usage(deep=True).sum() / 1024**2, 2)
        info["After"] = {
            "Total (MB)": mem_used_after,
            "By Type (MB)": round(
                df.memory_usage(deep=True).groupby([df.dtypes]).sum() / 1024**2, 2
            ),
        }

        savings = round(mem_used_before - mem_used_after, 2)
        savings_pct = round(
            (savings / mem_used_before) * 100, 2
        )  # Add precision for percentage
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
        print(
            f"Number of records from df1 only: {records_df1_only} ({percentage_df1_only:.2f}%)"
        )
        print(
            f"Number of records from df2 only: {records_df2_only} ({percentage_df2_only:.2f}%)"
        )
        print(
            f"Number of records present in both df1 and df2: {records_both} ({percentage_both:.2f}%)"
        )


if __name__ == "__main__":
    start_date = "2020-09-01"
    ETL = ETLPipeline(start_date)

    ETL.load_features_data(
        [
            "team_nbastats_general_traditional",
            "team_nbastats_general_advanced",
            "team_nbastats_general_fourfactors",
            "team_nbastats_general_opponent",
        ]
    )

    ETL.prepare_all_tables()

    ETL.feature_creation_pre_merge()

    ETL.merge_features_data()

    ETL.feature_creation_post_merge()

    ETL.clean_and_save_combined_features()
