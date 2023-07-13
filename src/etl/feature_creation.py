import os
import sys

import pandas as pd
from scipy.stats import zscore

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))
import config


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
    pass
