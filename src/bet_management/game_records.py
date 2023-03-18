import datetime
import sys

import autokeras as ak
import numpy as np
import pandas as pd
import pytz
import tensorflow as tf
from pycaret import classification as pyc_cls
from pycaret import regression as pyc_reg
from scipy import stats
from sqlalchemy import create_engine
from tensorflow import keras

sys.path.append("../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 100)


class Game_Record:
    """
    Pipeline to create game_records and save to database.
    Inbound - Trained ML and DL models,
              Data from combined_data_inbound and model_ready tables

    """

    def __init__(self):
        self.inbound_data = None
        self.game_records = None
        self.ml_cls_model = None
        self.dl_cls_model = None
        self.ml_reg_model = None
        self.dl_reg_model = None
        self.ml_cls_predictions = None
        self.dl_cls_predictions = None
        self.ml_reg_predictions = None
        self.dl_reg_predictions = None

    def load_data(self, connection, game_dates="daily"):
        """
        Wrapper for SQL query to load data.
        """
        if game_dates == "daily":
            todays_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
            yesterdays_datetime = todays_datetime - datetime.timedelta(days=1)
            yesterdays_date_str = yesterdays_datetime.strftime("%Y%m%d")
            todays_date_str = todays_datetime.strftime("%Y%m%d")

            stmt = f"""
                    SELECT cid.game_id,
                    cid.game_date,
                    cid.home_team,
                    cid.away_team,
                    cid.home_score,
                    cid.away_score,
                    cid.home_result,
                    cid.covers_game_url,
                    cid.league_year,
                    cid.home_spread,
                    cid.home_spread_result,
                    cid.pred_date,
                    cid.fd_line_home,
                    cid.fd_line_price_home,
                    cid.fd_line_away,
                    cid.fd_line_price_away,
                    cid.dk_line_home,
                    cid.dk_line_price_home,
                    cid.dk_line_away,
                    cid.dk_line_price_away,
                    cid.covers_consensus_home,
                    cid.covers_consensus_away,
                    cid.raptor_prob1,
                    cid.raptor_prob2,
                    mtd.*
                    FROM combined_inbound_data AS cid
                    LEFT OUTER JOIN model_training_data AS mtd
                    ON cid.game_id = mtd.game_id
                    WHERE cid.game_id LIKE '20230312%%'
                    OR cid.game_id LIKE '20230311%%'
                    """

        elif game_dates == "all":
            stmt = f"""
                    SELECT cid.game_id,
                    cid.game_date,
                    cid.home_team,
                    cid.away_team,
                    cid.home_score,
                    cid.away_score,
                    cid.home_result,
                    cid.covers_game_url,
                    cid.league_year,
                    cid.home_spread,
                    cid.home_spread_result,
                    cid.pred_date,
                    cid.fd_line_home,
                    cid.fd_line_price_home,
                    cid.fd_line_away,
                    cid.fd_line_price_away,
                    cid.dk_line_home,
                    cid.dk_line_price_home,
                    cid.dk_line_away,
                    cid.dk_line_price_away,
                    cid.covers_consensus_home,
                    cid.covers_consensus_away,
                    cid.raptor_prob1,
                    cid.raptor_prob2,
                    mtd.*
                    FROM combined_inbound_data AS cid
                    LEFT OUTER JOIN model_training_data AS mtd
                    ON cid.game_id = mtd.game_id
                    """
        else:
            raise ValueError(
                "Invalid game_dates parameter. Use either 'daily' or 'all'."
            )

        self.inbound_data = pd.read_sql(sql=stmt, con=connection)
        self.inbound_data = self.inbound_data.loc[
            :, ~self.inbound_data.columns.duplicated()
        ]
        for col in self.inbound_data.columns:
            if col not in [
                "game_id",
                "game_date",
                "home_team",
                "away_team",
                "home_score",
                "away_score",
                "home_result",
                "covers_game_url",
                "league_year",
                "home_spread",
                "home_spread_result",
                "pred_date",
                "fd_line_home",
                "fd_line_price_home",
                "fd_line_away",
                "fd_line_price_away",
                "dk_line_home",
                "dk_line_price_home",
                "dk_line_away",
                "dk_line_price_away",
                "covers_consensus_home",
                "covers_consensus_away",
                "raptor_prob1",
                "raptor_prob2",
                "REG_TARGET_actual_home_margin",
                "CLS_TARGET_home_margin_GT_home_spread",
            ]:
                nan_count = self.inbound_data[col].isna().sum()
                print(f"Column '{col}' has {nan_count} NaN values")

        self.inbound_data = self.inbound_data.dropna(
            subset=[
                feature
                for feature in self.inbound_data.columns
                if feature
                not in [
                    "game_id",
                    "game_date",
                    "home_team",
                    "away_team",
                    "home_score",
                    "away_score",
                    "home_result",
                    "covers_game_url",
                    "league_year",
                    "home_spread",
                    "home_spread_result",
                    "pred_date",
                    "fd_line_home",
                    "fd_line_price_home",
                    "fd_line_away",
                    "fd_line_price_away",
                    "dk_line_home",
                    "dk_line_price_home",
                    "dk_line_away",
                    "dk_line_price_away",
                    "covers_consensus_home",
                    "covers_consensus_away",
                    "raptor_prob1",
                    "raptor_prob2",
                    "REG_TARGET_actual_home_margin",
                    "CLS_TARGET_home_margin_GT_home_spread",
                ]
            ]
        )

    def load_models(
        self, ml_cls_model_path, dl_cls_model_path, ml_reg_model_path, dl_reg_model_path
    ):
        """
        Choosing which models to use for predictions.
        Make sure feature set matches model.
        """
        self.ml_cls_model = pyc_cls.load_model(ml_cls_model_path)
        self.dl_cls_model = keras.models.load_model(
            dl_cls_model_path, custom_objects=ak.CUSTOM_OBJECTS
        )
        self.ml_reg_model = pyc_reg.load_model(ml_reg_model_path)
        self.dl_reg_model = keras.models.load_model(
            dl_reg_model_path, custom_objects=ak.CUSTOM_OBJECTS
        )

    def create_predictions(self):
        """
        Choosing feature set that matches models to be used.
        """
        drop_features = [
            "fd_line_home",
            "dk_line_home",
            "covers_consensus_home",
            "game_id",
            "game_date",
            "REG_TARGET_actual_home_margin",
            "CLS_TARGET_home_margin_GT_home_spread",
        ]
        main_features = [
            "home_team_num",
            "away_team_num",
            "home_spread",
            "league_year_end",
            "day_of_season",
            "elo1_pre",
            "elo2_pre",
            "elo_prob1",
            "elo_prob2",
        ]
        rank_features = [
            feature for feature in list(self.inbound_data) if "rank" in feature
        ]
        zscore_features = [
            feature for feature in list(self.inbound_data) if "zscore" in feature
        ]

        other_features = [
            feature
            for feature in list(self.inbound_data)
            if feature not in main_features + drop_features
        ]

        ml_pred_feature_set = main_features + rank_features + zscore_features
        dl_pred_feature_set = main_features + rank_features + zscore_features

        self.ml_cls_predictions = pyc_cls.predict_model(
            self.ml_cls_model,
            data=self.inbound_data[ml_pred_feature_set].astype("float32"),
        )
        self.ml_reg_predictions = pyc_reg.predict_model(
            self.ml_reg_model,
            data=self.inbound_data[ml_pred_feature_set].astype("float32"),
        )
        self.dl_cls_predictions = self.dl_cls_model.predict(
            self.inbound_data[dl_pred_feature_set].astype("float32")
        ).flatten()
        self.dl_reg_predictions = self.dl_reg_model.predict(
            self.inbound_data[dl_pred_feature_set].astype("float32")
        ).flatten()

    def set_up_table(self):
        self.game_records = pd.DataFrame(
            {
                "line_hv": 0 - self.inbound_data["home_spread"],
                "home_line": self.inbound_data["home_spread"],
                "ml_cls_prediction": self.ml_cls_predictions["prediction_label"],
                "ml_cls_pred_proba": self.ml_cls_predictions["prediction_score"],
                "dl_cls_prediction": self.dl_cls_predictions,
                "ml_reg_prediction": self.ml_reg_predictions["prediction_label"],
                "dl_reg_prediction": self.dl_reg_predictions,
                "covers_consensus_home": self.inbound_data["covers_consensus_home"],
                "raptor_prob1": self.inbound_data["raptor_prob1"],
                "home_score": self.inbound_data["home_score"],
                "away_score": self.inbound_data["away_score"],
            }
        )

    def _raptor_pred_direction(self, x):
        """Helper function to convert raptor probabilities
        to home/away direction based on line."""
        intercept = 0.5
        coef = 0.0324
        implied_win_margin = (x["raptor_prob1"] - intercept) / coef
        if implied_win_margin > x["line_hv"]:
            return "Home"
        else:
            return "Away"

    def bet_direction(self):
        """
        Determines if predictions are for home or away team.
        """
        # Individual predictions
        self.game_records["ml_cls_pred_direction"] = self.game_records[
            "ml_cls_prediction"
        ].apply(lambda x: "Home" if x == "True" else "Away")
        self.game_records["dl_cls_pred_direction"] = self.game_records[
            "dl_cls_prediction"
        ].apply(lambda x: "Home" if x > 0.5 else "Away")
        self.game_records["covers_consensus_direction"] = self.game_records[
            "covers_consensus_home"
        ].apply(lambda x: "Home" if x > 0.5 else "Away")
        self.game_records["raptor_pred_direction"] = self.game_records.apply(
            self._raptor_pred_direction, axis=1
        )

        # Cumulative predictions
        def _cumulative_pred_direction(x):
            home_count = sum(
                [
                    1 if y == "Home" else 0
                    for y in [
                        x["ml_cls_pred_direction"],
                        x["dl_cls_pred_direction"],
                        x["covers_consensus_direction"],
                        x["raptor_pred_direction"],
                    ]
                ]
            )
            away_count = sum(
                [
                    1 if y == "Away" else 0
                    for y in [
                        x["ml_cls_pred_direction"],
                        x["dl_cls_pred_direction"],
                        x["covers_consensus_direction"],
                        x["raptor_pred_direction"],
                    ]
                ]
            )

            # Display String
            if home_count > away_count:
                bet_direction_vote = f"Home {home_count}-{away_count}"
            elif away_count > home_count:
                bet_direction_vote = f"Away {away_count}-{home_count}"
            else:
                bet_direction_vote = f"Tie {home_count}-{away_count}"

            return bet_direction_vote

        self.game_records["bet_direction_vote"] = self.game_records.apply(
            _cumulative_pred_direction, axis=1
        )

    def _raptor_score_calc(self, x):
        intercept = 0.5
        coef = 0.0324
        line_hv_to_prob = (x["line_hv"] * coef) + intercept
        raptor_home_prob_diff = x["raptor_prob1"] - line_hv_to_prob
        raptor_home_score = 50 + (raptor_home_prob_diff * 100)
        return raptor_home_score

    def _game_score_calc(self, x):
        if pd.isnull(x["covers_home_score"]) and pd.isnull(x["raptor_home_score"]):
            home_score = (x["ml_home_score"] + x["dl_home_score"]) / 2
        elif pd.isnull(x["covers_home_score"]):
            home_score = (
                x["ml_home_score"] + x["dl_home_score"] + x["raptor_home_score"]
            ) / 3
        else:
            home_score = (
                x["ml_home_score"]
                + x["dl_home_score"]
                + x["raptor_home_score"]
                + x["covers_home_score"]
            ) / 4
        away_score = 100 - home_score
        if home_score >= 50:
            return home_score, "Home"
        else:
            return away_score, "Away"

    def game_score(self):
        """
        Overall score for the desirability of betting on a game.
        Incorporates model predictions with outside knowledge.
        0 to 100 scale.
        """
        self.game_records["ml_home_score"] = self.game_records.apply(
            lambda x: x["ml_cls_pred_proba"] * 100
            if x["ml_cls_pred_direction"] == "Home"
            else 100 - (x["ml_cls_pred_proba"] * 100),
            axis=1,
        )
        self.game_records["dl_home_score"] = self.game_records["dl_cls_prediction"] * 100
        self.game_records["covers_home_score"] = (
            self.game_records["covers_consensus_home"] * 100
        )
        self.game_records["raptor_home_score"] = self.game_records.apply(
            self._raptor_score_calc, axis=1
        )

        self.game_records["game_score"] = self.game_records.apply(
            lambda x: self._game_score_calc(x)[0], axis=1
        )
        self.game_records["game_score_direction"] = self.game_records.apply(
            lambda x: self._game_score_calc(x)[1], axis=1
        )

    def game_details_and_cleanup(self):
        """
        Adding extra game info for display and ordering columns.

        """
        self.game_records["game_id"] = self.inbound_data["game_id"]
        self.game_records["game_info"] = self.inbound_data["covers_game_url"]
        self.game_records["date"] = self.inbound_data["game_date"].dt.date
        self.game_records["home"] = self.inbound_data["home_team"]
        self.game_records["away"] = self.inbound_data["away_team"]
        self.game_records["game_result"] = self.game_records.apply(
            lambda x: x["home_score"] - x["away_score"] if x["home_score"] else "-",
            axis=1,
        )

        ordered_cols = [
            "game_id",
            "game_info",
            "date",
            "home",
            "away",
            "home_line",
            "ml_home_score",
            "dl_home_score",
            "covers_home_score",
            "raptor_home_score",
            "game_score",
            "game_score_direction",
            "bet_direction_vote",
            "home_score",
            "away_score",
            "game_result",
            "ml_reg_prediction",
            "dl_reg_prediction",
        ]

        self.game_records = self.game_records[ordered_cols]

    def save_records(self, connection, game_dates="daily"):
        """
        Save finalized game records.
        """
        if game_dates == "daily":
            self.game_records.to_sql(
                name="game_records",
                con=connection,
                index=False,
                if_exists="append",
            )
        elif game_dates == "all":
            self.game_records.to_sql(
                name="game_records",
                con=connection,
                index=False,
                if_exists="replace",
            )


if __name__ == "__main__":
    ml_cls_model_path = "../../models/AutoML/PyCaret_LogReg_Main_Rank_ZScore"
    dl_cls_model_path = "../../models/AutoKeras/AutoKeras_CLS_Main_Rank_ZScore"
    ml_reg_model_path = "../../models/AutoML/PyCaret_Lasso_main_rank_zscore"
    dl_reg_model_path = "../../models/AutoKeras/AutoKeras_REG_Main_Rank_ZScore"

    username = "postgres"
    password = RDS_PASSWORD
    endpoint = RDS_ENDPOINT
    database = "nba_betting"
    port = "5432"

    with create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    ).connect() as connection:

        todays_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
        yesterdays_datetime = todays_datetime - datetime.timedelta(days=1)
        yesterdays_date_str = yesterdays_datetime.strftime("%Y%m%d")
        todays_date_str = todays_datetime.strftime("%Y%m%d")

        try:
            games = Game_Record()
            games.load_data(connection, "daily")
            games.load_models(
                ml_cls_model_path,
                dl_cls_model_path,
                ml_reg_model_path,
                dl_reg_model_path,
            )
            games.create_predictions()
            games.set_up_table()
            games.bet_direction()
            games.game_score()
            games.game_details_and_cleanup()
            print(games.game_records.info())
            print(games.game_records.sort_values("game_id", ascending=False).head(10))

            # stmt = f"""
            #         DELETE FROM game_records
            #         WHERE game_id LIKE '20230311%%'
            #         ;
            #         """

            # connection.execute(stmt)

            # games.save_records(connection, "daily")

            print("----- Game Records Update Successful -----")
        except Exception as e:
            print(f"----- Game Records Update Failed -----")
            raise e
