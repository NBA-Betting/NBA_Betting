import pandas as pd
from sqlalchemy import create_engine


class FeatureCreation:
    """
    Data pipeline class for transforming inbound unclean dataset
    into cleaned and fully featurized model ready dataset.
    Use this area to create new feature sets
    from new and existing data sources
    in order to add predictive and informative power to downstream models.
    """

    def __init__(self, df):
        self.inbound_df = df
        self.wdf = df.copy()

    def create_target(self):
        """
        Current target is the Home Margin of the game.
        - Regression
        - Home Score minus Away Score
        Possible future targets include:
        - Classification of Home Result vs. Spread
        - Classification of Binned Home Margins
        """
        self.wdf.insert(
            1,
            "TARGET_actual_home_margin",
            self.wdf["home_score"] - self.wdf["away_score"],
        )

    def create_base_feature_set(self):
        """
        Model prep for original data sources including:
        - Historic team and odds data from Covers
        - Live team and odds data from Covers
        - Point in time standings and team/opponent stats
          from Basketball Reference
        """
        always_drop_features = [
            "game_date",
            "home_score",
            "away_score",
            "home_result",
            "covers_game_url",
            "home_spread_result",
            "pred_date",
            "fd_line_price_home",
            "fd_line_price_away",
            "dk_line_price_home",
            "dk_line_price_away",
            "open_line_away",
            "fd_line_away",
            "dk_line_away",
        ]
        self.wdf = self.wdf.drop(columns=always_drop_features)

        team_abbrv_list = sorted(list(self.wdf["home_team"].unique()))
        self.wdf.insert(
            2,
            "home_team_num",
            [
                team_abbrv_list.index(x) + 1 if isinstance(x, str) else 0
                for x in self.wdf["home_team"]
            ],
        )
        self.wdf.insert(
            3,
            "away_team_num",
            [team_abbrv_list.index(x) + 1 for x in self.wdf["away_team"]],
        )
        self.wdf.insert(
            4,
            "league_year_end",
            [
                int(x[-2:]) if isinstance(x, str) else 0
                for x in self.wdf["league_year"]
            ],
        )

        self.wdf.insert(6, "fd_line_home", self.wdf.pop("fd_line_home"))
        self.wdf.insert(7, "dk_line_home", self.wdf.pop("dk_line_home"))
        self.wdf.insert(
            8, "covers_consenses_home", self.wdf.pop("covers_consenses_home")
        )

        self.wdf.insert(
            5,
            "home_line",
            self.wdf["home_spread"].fillna(self.wdf["open_line_home"]),
        )

        self.wdf = self.wdf.drop(
            columns=[
                "home_team",
                "away_team",
                "league_year",
                "covers_consenses_away",
                "open_line_home",
                "home_spread",
            ]
        )

    # Define new feature set methods here.

    def finalize_data(self):
        """
        Used to implement dataset ease of use improvements like:
        - Setting Datatypes
        - Renaming Features
        - Reordering Features
        """
        pass

    def run_all_steps(self):
        self.create_target()
        self.create_base_feature_set()
        # Add new feature set methods here.
        self.finalize_data()


if __name__ == "__main__":
    username = "postgres"
    # password = ""
    # endpoint = ""
    database = "nba_betting"
    port = "5432"

    with create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    ).connect() as connection:

        df = pd.read_sql_table("combined_data_inbound", connection)

        feature_pipeline = FeatureCreation(df)
        feature_pipeline.run_all_steps()
        model_ready_df = feature_pipeline.wdf

        # print(model_ready_df.head(50).iloc[:, :10])

        # model_ready_df.to_sql(
        #     "model_ready", connection, index=False, if_exists="replace"
        # )
