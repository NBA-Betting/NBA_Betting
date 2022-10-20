import sys
import datetime
import pytz
import numpy as np
import pandas as pd
import tensorflow as tf
from scipy import stats
from tensorflow import keras
from sqlalchemy import create_engine
from pycaret import regression as pyc_reg
from pycaret import classification as pyc_cls
import autokeras as ak

sys.path.append('../../')
from ...passkeys import RDS_ENDPOINT, RDS_PASSWORD

pd.set_option("display.width", 200)


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

    def load_data(self, connection, query_date):
        """
        Wrapper for SQL query to load data.
        """
        query = f"""
        SELECT cnc.game_id,
        cnc.game_date,
        cnc.home_team,
        cnc.away_team,
        cnc.home_score,
        cnc.away_score,
        cnc.home_result,
        cnc.covers_game_url,
        cnc.league_year,
        cnc.home_spread,
        cnc.home_spread_result,
        cnc.pred_date,
        cnc.fd_line_home,
        cnc.fd_line_price_home,
        cnc.fd_line_away,
        cnc.fd_line_price_away,
        cnc.dk_line_home,
        cnc.dk_line_price_home,
        cnc.dk_line_away,
        cnc.dk_line_price_away,
        cnc.covers_consenses_home,
        cnc.covers_consenses_away,
        mr.*
        FROM combined_nba_covers AS cnc
        LEFT OUTER JOIN nba_model_ready AS mr
        ON cnc.game_id = mr.game_id
        WHERE cnc.game_id LIKE '{query_date}%%'
        """

        self.inbound_data = pd.read_sql(sql=query, con=connection)
        self.inbound_data = self.inbound_data.loc[:, ~self.inbound_data.
                                                  columns.duplicated()].copy()

    def load_models(self, ml_cls_model_path, dl_cls_model_path,
                    ml_reg_model_path, dl_reg_model_path):
        """
        Choosing which models to use for predictions.
        Make sure feature set matches model.
        """
        self.ml_cls_model = pyc_cls.load_model(ml_cls_model_path)
        self.dl_cls_model = keras.models.load_model(
            dl_cls_model_path, custom_objects=ak.CUSTOM_OBJECTS)
        self.ml_reg_model = pyc_reg.load_model(ml_reg_model_path)
        self.dl_reg_model = keras.models.load_model(
            dl_reg_model_path, custom_objects=ak.CUSTOM_OBJECTS)

    def create_predictions(self):
        """
        Choosing feature set that matches models to be used.
        """
        main_features = [
            'home_team_num', 'away_team_num', 'league_year_end', 'home_spread'
        ]
        nba_stats_features_vla = [
            feature for feature in list(self.inbound_data)
            if feature[-3:] == 'vla'
        ]

        ml_pred_feature_set = main_features + nba_stats_features_vla
        dl_pred_feature_set = nba_stats_features_vla + main_features

        self.ml_cls_predictions = pyc_cls.predict_model(
            self.ml_cls_model, data=self.inbound_data[ml_pred_feature_set])
        self.ml_reg_predictions = pyc_reg.predict_model(
            self.ml_reg_model, data=self.inbound_data[ml_pred_feature_set])
        self.dl_cls_predictions = self.dl_cls_model.predict(
            self.inbound_data[dl_pred_feature_set]).flatten()
        self.dl_reg_predictions = self.dl_reg_model.predict(
            self.inbound_data[dl_pred_feature_set]).flatten()

    def set_up_table(self):
        self.game_records = pd.DataFrame({
            "line_hv":
            0 - self.inbound_data["home_spread"],
            "ml_cls_prediction":
            self.ml_cls_predictions["Label"],
            "ml_cls_pred_proba":
            self.ml_cls_predictions["Score"],
            "dl_cls_prediction":
            self.dl_cls_predictions,
            "ml_reg_prediction":
            self.ml_reg_predictions["Label"],
            "dl_reg_prediction":
            self.dl_reg_predictions,
            "home_score":
            self.inbound_data["home_score"],
            "away_score":
            self.inbound_data["away_score"],
        })

    def model_pred_bet_direction(self):
        """
        Determines if the model prediction is for the home or away team.
        """
        self.game_records['ml_cls_pred_direction'] = self.game_records[
            'ml_cls_prediction'].apply(lambda x: 'Home'
                                       if x == 'True' else 'Away')
        self.game_records['dl_cls_pred_direction'] = self.game_records[
            'dl_cls_prediction'].apply(lambda x: 'Home' if x > 0.5 else 'Away')

    def line_and_line_price(self):
        """
        Currently using -110 as a typical line price.
        TODO: Bring in actual line prices. Low Priority
        """
        self.game_records["home_line"] = self.inbound_data["home_spread"]
        self.game_records["away_line"] = -self.inbound_data["home_spread"]
        self.game_records["home_line_price"] = -110
        self.game_records["away_line_price"] = -110
        self.game_records["ml_pred_vig"] = self.game_records.apply(
            lambda x: x["home_line_price"]
            if x["ml_cls_pred_direction"] == "Home" else x["away_line_price"],
            axis=1,
        )

        self.game_records["dl_pred_vig"] = self.game_records.apply(
            lambda x: x["home_line_price"]
            if x["dl_cls_pred_direction"] == "Home" else x["away_line_price"],
            axis=1,
        )

    def bet_direction(self):
        """
        Logic for choosing between models with differing reccommended bet directions.
        """
        self.game_records["rec_bet_direction"] = self.game_records.apply(
            lambda x: x["ml_cls_pred_direction"] if x["ml_cls_pred_direction"]
            == x["dl_cls_pred_direction"] else "Unknown",
            axis=1,
        )

    def game_score(self):
        """
        Overall score for the desirability of betting on a game.
        Incorporates model predictions with outside knowledge.
        0 to 100 scale.
        TODO: Combine ML and DL EVs once models become more effective
              and generally agree on recommended bet direction.
        TODO: Incorporate outside factors and personal opinion
              once modeling becomes more effective.
        """
        def game_score_calc(x):
            ml_score = x["ml_cls_pred_proba"]
            if x['dl_cls_prediction'] >= 0.5:
                dl_score = x['dl_cls_prediction']
            elif x['dl_cls_prediction'] < 0.5:
                dl_score = 1 - x['dl_cls_prediction']

            if x['rec_bet_direction'] == 'Unknown':
                score = 0
            else:
                score = ((ml_score + dl_score) / 2) * 100

            return score

        self.game_records["game_score"] = self.game_records.apply(
            game_score_calc, axis=1)

    def bet_amount(self, connection):
        """
        How much should be bet on each game based on
        game scores, available funds, and overall bankroll management plan.
        TODO: Update formula to be more effective based on simulations and testing.
        """

        available_funds_query = """SELECT balance
                                   FROM bank_account
                                   WHERE datetime = (SELECT MAX(datetime)
                                   FROM bank_account);"""

        current_available_funds = pd.read_sql(sql=available_funds_query,
                                              con=connection)["balance"]

        unit_size = 0.01
        unit_amount = current_available_funds * unit_size

        def calc_amount_to_bet(x, unit_size):
            if x["game_score"] > 75:
                units_to_bet = 5
            elif x["game_score"] > 65:
                units_to_bet = 3
            elif x["game_score"] > 55:
                units_to_bet = 1
            else:
                units_to_bet = 0
            amount_to_bet = unit_size * units_to_bet
            return amount_to_bet

        self.game_records["rec_bet_amount"] = self.game_records.apply(
            calc_amount_to_bet, unit_size=unit_amount, axis=1)

    def game_info_and_cleanup(self):
        """
        Adding extra game info for display and ordering columns.

        """
        self.game_records["game_id"] = self.inbound_data["game_id"]
        self.game_records["game_info"] = self.inbound_data["covers_game_url"]
        self.game_records["date"] = self.inbound_data["game_date"].dt.date
        self.game_records["home"] = self.inbound_data["home_team"]
        self.game_records["away"] = self.inbound_data["away_team"]
        self.game_records["game_result"] = self.game_records.apply(
            lambda x: x["home_score"] - x["away_score"]
            if x["home_score"] else "Pending",
            axis=1,
        )

        ordered_cols = [
            'game_id', 'game_info', 'date', 'home', 'away', 'home_line',
            'home_line_price', 'away_line', 'away_line_price',
            'ml_cls_prediction', 'ml_cls_pred_proba', 'ml_cls_pred_direction',
            'ml_reg_prediction', 'dl_cls_prediction', 'dl_cls_pred_direction',
            'dl_reg_prediction', 'game_score', 'rec_bet_direction',
            'rec_bet_amount', 'game_result'
        ]

        self.game_records = self.game_records[ordered_cols]

    def save_records(self, connection):
        """
        Save finalized game records.
        """
        self.game_records.to_sql(
            name="game_records",
            con=connection,
            index=False,
            if_exists="append",
        )


if __name__ == "__main__":
    todays_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
    todays_date_str = todays_datetime.strftime("%Y%m%d")
    date_str = '20220410'

    username = "postgres"
    password = RDS_PASSWORD
    endpoint = RDS_ENDPOINT
    database = "nba_betting"
    port = "5432"

    connection = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    ).connect()

    ml_cls_model_path = "../../models/AutoML/Baseline_LR_CLS_PyCaret"
    dl_cls_model_path = ("../../models/AutoKeras/Baseline_AK_CLS")
    ml_reg_model_path = "../../models/AutoML/Baseline_Lasso_Reg_PyCaret"
    dl_reg_model_path = ("../../models/AutoKeras/Baseline_AK_REG")

    games = Game_Record()
    games.load_data(connection, date_str)
    games.load_models(ml_cls_model_path, dl_cls_model_path, ml_reg_model_path,
                      dl_reg_model_path)
    games.create_predictions()
    games.set_up_table()
    games.model_pred_bet_direction()
    games.line_and_line_price()
    games.bet_direction()
    games.game_score()
    games.bet_amount(connection)
    games.game_info_and_cleanup()
    print(games.game_records.info())
    print(games.game_records.head())
    # games.save_records(connection)
