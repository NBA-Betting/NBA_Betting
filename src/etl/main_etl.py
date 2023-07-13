import os
import sys
from datetime import datetime

import pandas as pd
import pytz
from dotenv import load_dotenv
from feature_creation import FeatureCreationPreMerge
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import reflection

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))
import config

load_dotenv()
RDS_ENDPOINT = os.getenv("RDS_ENDPOINT")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

pd.set_option("display.max_columns", None)


class ETLPipeline:
    def __init__(
        self,
        start_date,
        RDS_ENDPOINT=RDS_ENDPOINT,
        RDS_PASSWORD=RDS_PASSWORD,
        config=config,
    ):
        self.database_engine = create_engine(
            f"postgresql://postgres:{RDS_PASSWORD}@{RDS_ENDPOINT}/nba_betting"
        )
        self.config = config
        self.start_date = start_date
        self.game_data = self.load_game_data()
        self.features_data = {}

    # DATA LOADING

    def load_table(self, table_name, date_column, columns_to_load=None, start_date=None):
        if columns_to_load is None:
            columns = "*"
        else:
            columns = ", ".join(columns_to_load)
        if start_date is None:
            start_date = self.start_date

        todays_date = datetime.now(pytz.timezone("America/Denver")).strftime("%Y-%m-%d")

        query = f"SELECT {columns} FROM {table_name} WHERE {date_column} >= '{start_date}' AND {date_column} <= '{todays_date}';"
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
        ]
        try:
            game_data = self.load_table("game_data", "game_datetime", features)
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
                        f"***{info['rows_removed']} Rows Removed from {table_name} due to Unknow Team Names: {info['not_found']}"
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
    def standardize_team_names(df, columns, mapping, filter=False):
        df[columns] = df[columns].replace(mapping)

        info = {"not_found": [], "rows_removed": 0}

        # Find team names that are in the DataFrame but not in the mapping
        for column in columns:
            unique_teams = df[column].unique()
            for team in unique_teams:
                if team not in mapping:
                    info["not_found"].append(team)

        info["not_found"] = list(set(info["not_found"]))

        # Store the initial number of rows
        initial_rows = df.shape[0]

        if filter:
            # Remove rows where the team names are not found in the mapping
            for column in columns:
                df = df[df[column].isin(mapping.keys())]

            # Calculate the number of rows removed
            info["rows_removed"] = initial_rows - df.shape[0]

        return df, info

    @staticmethod
    def check_duplicates(df, primary_key, filter=False):
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

        return df, info

    @staticmethod
    def downcast_data_types(df, downcast_floats=True):
        info = {}
        mem_used_before = round(df.memory_usage(deep=True).sum() / 1024**2, 2)
        info["Before"] = {
            "Total (MB)": mem_used_before,
            "By Type (MB)": round(
                df.memory_usage(deep=True).groupby([df.dtypes]).sum() / 1024**2, 2
            ),
        }
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
        savings_pct = round((savings / mem_used_before) * 100)
        info["Savings"] = f"{savings} MB ({savings_pct}%)"
        return df, info


if __name__ == "__main__":
    start_date = "2020-09-01"
    ETL = ETLPipeline(start_date)

    ETL.load_features_data(
        [
            "team_fivethirtyeight_games",
            "team_nbastats_general_traditional",
        ]
    )

    ETL.prepare_all_tables()

    ETL.feature_creation_pre_merge()
