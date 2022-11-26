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
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

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

    def load_data(self, connection, todays_date, yesterdays_date):
        """
        Wrapper for SQL query to load data.
        """
        query = f"""
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
        WHERE cid.game_id LIKE '{todays_date}%%'
        OR cid.game_id LIKE '{yesterdays_date}%%'
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
        drop_features = [
            'fd_line_home', 'dk_line_home', 'covers_consensus_home', 'game_id',
            'REG_TARGET_actual_home_margin',
            'CLS_TARGET_home_margin_GT_home_spread'
        ]
        main_features = [
            'home_team_num', 'away_team_num', 'home_spread', 'league_year_end',
            'day_of_season', 'elo1_pre', 'elo2_pre', 'elo_prob1', 'elo_prob2'
        ]
        rank_features = [
            feature for feature in list(self.inbound_data) if 'rank' in feature
        ]
        vla_std_features = [
            feature for feature in list(self.inbound_data)
            if 'vla_std' in feature
        ]

        other_features = [
            feature for feature in list(self.inbound_data)
            if feature not in main_features + drop_features
        ]

        ml_pred_feature_set = main_features + rank_features + vla_std_features
        dl_pred_feature_set = main_features + rank_features + vla_std_features

        self.ml_cls_predictions = pyc_cls.predict_model(
            self.ml_cls_model, data=self.inbound_data[ml_pred_feature_set])
        self.ml_reg_predictions = pyc_reg.predict_model(
            self.ml_reg_model, data=self.inbound_data[ml_pred_feature_set])
        self.dl_cls_predictions = self.dl_cls_model.predict(
            self.inbound_data[dl_pred_feature_set].astype(
                'float64')).flatten()
        self.dl_reg_predictions = self.dl_reg_model.predict(
            self.inbound_data[dl_pred_feature_set].astype(
                'float64')).flatten()

    def set_up_table(self):
        self.game_records = pd.DataFrame({
            "line_hv":
            0 - self.inbound_data["home_spread"],
            "home_line":
            self.inbound_data["home_spread"],
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
            "covers_consensus_home":
            self.inbound_data["covers_consensus_home"],
            "raptor_prob1":
            self.inbound_data["raptor_prob1"],
            "home_score":
            self.inbound_data["home_score"],
            "away_score":
            self.inbound_data["away_score"],
        })

    def _raptor_pred_direction(self, x):
        """Helper function to convert raptor probabilities
           to home/away direction based on line."""
        intercept = 0.5
        coef = 0.0324
        implied_win_margin = (x['raptor_prob1'] - intercept) / coef
        if implied_win_margin > x['line_hv']:
            return 'Home'
        else:
            return 'Away'

    def bet_direction(self):
        """
        Determines if predictions are for home or away team.
        """
        # Individual predictions
        self.game_records['ml_cls_pred_direction'] = self.game_records[
            'ml_cls_prediction'].apply(lambda x: 'Home'
                                       if x == 'True' else 'Away')
        self.game_records['dl_cls_pred_direction'] = self.game_records[
            'dl_cls_prediction'].apply(lambda x: 'Home' if x > 0.5 else 'Away')
        self.game_records['covers_consensus_direction'] = self.game_records[
            'covers_consensus_home'].apply(lambda x: 'Home'
                                           if x > 0.5 else 'Away')
        self.game_records['raptor_pred_direction'] = self.game_records.apply(
            self._raptor_pred_direction, axis=1)

        # Cumulative predictions
        def _cumulative_pred_direction(x):
            home_count = sum([
                1 if y == 'Home' else 0 for y in [
                    x['ml_cls_pred_direction'], x['dl_cls_pred_direction'],
                    x['covers_consensus_direction'], x['raptor_pred_direction']
                ]
            ])
            away_count = sum([
                1 if y == 'Away' else 0 for y in [
                    x['ml_cls_pred_direction'], x['dl_cls_pred_direction'],
                    x['covers_consensus_direction'], x['raptor_pred_direction']
                ]
            ])

            # Display String
            if home_count > away_count:
                bet_direction_vote = f'Home {home_count}-{away_count}'
            elif away_count > home_count:
                bet_direction_vote = f'Away {away_count}-{home_count}'
            else:
                bet_direction_vote = f'Tie {home_count}-{away_count}'

            return bet_direction_vote

        self.game_records["bet_direction_vote"] = self.game_records.apply(
            _cumulative_pred_direction, axis=1)

    def _raptor_score_calc(self, x):
        intercept = 0.5
        coef = 0.0324
        line_hv_to_prob = (x['line_hv'] * coef) + intercept
        raptor_home_prob_diff = x['raptor_prob1'] - line_hv_to_prob
        raptor_home_score = 50 + (raptor_home_prob_diff * 100)
        return raptor_home_score

    def _game_score_calc(self, x):
        if pd.isnull(x['covers_home_score']) and pd.isnull(
                x['raptor_home_score']):
            home_score = (x['ml_home_score'] + x['dl_home_score']) / 2
        elif pd.isnull(x['covers_home_score']):
            home_score = (x['ml_home_score'] + x['dl_home_score'] +
                          x['raptor_home_score']) / 3
        else:
            home_score = (x['ml_home_score'] + x['dl_home_score'] +
                          x['raptor_home_score'] + x['covers_home_score']) / 4
        away_score = 100 - home_score
        if home_score >= 50:
            return home_score, 'Home'
        else:
            return away_score, 'Away'

    def game_score(self):
        """
        Overall score for the desirability of betting on a game.
        Incorporates model predictions with outside knowledge.
        0 to 100 scale.
        """
        self.game_records['ml_home_score'] = self.game_records.apply(
            lambda x: x['ml_cls_pred_proba'] * 100
            if x['ml_cls_pred_direction'] == 'Home' else 100 -
            (x['ml_cls_pred_proba'] * 100),
            axis=1)
        self.game_records[
            'dl_home_score'] = self.game_records['dl_cls_prediction'] * 100
        self.game_records['covers_home_score'] = self.game_records[
            'covers_consensus_home'] * 100
        self.game_records['raptor_home_score'] = self.game_records.apply(
            self._raptor_score_calc, axis=1)

        self.game_records["game_score"] = self.game_records.apply(
            lambda x: self._game_score_calc(x)[0], axis=1)
        self.game_records['game_score_direction'] = self.game_records.apply(
            lambda x: self._game_score_calc(x)[1], axis=1)

    def bet_amount(self, connection):
        """
        How much should be bet on each game based on
        game scores, available funds, and overall bankroll management plan.
        TODO: EDA-Does higher game score lead to higher win%?
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
            lambda x: x["home_score"] - x["away_score"]
            if x["home_score"] else 0,
            axis=1,
        )

        ordered_cols = [
            'game_id', 'game_info', 'date', 'home', 'away', 'home_line',
            'ml_home_score', 'dl_home_score', 'covers_home_score',
            'raptor_home_score', 'game_score', 'game_score_direction',
            'bet_direction_vote', 'rec_bet_amount', 'home_score', 'away_score',
            'game_result', 'ml_reg_prediction', 'dl_reg_prediction'
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
    ml_cls_model_path = "../../models/AutoML/vlastd_rank_LR_CLS_PyCaret"
    dl_cls_model_path = ("../../models/AutoKeras/vlastd_rank_AK_CLS")
    ml_reg_model_path = "../../models/AutoML/vlastd_Rank_Lasso_Reg_PyCaret"
    dl_reg_model_path = ("../../models/AutoKeras/vlastd_rank_AK_REG")

    username = "postgres"
    password = RDS_PASSWORD
    endpoint = RDS_ENDPOINT
    database = "nba_betting"
    port = "5432"

    with create_engine(
            f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    ).connect() as connection:

        todays_datetime = datetime.datetime.now(
            pytz.timezone("America/Denver"))
        yesterdays_datetime = todays_datetime - datetime.timedelta(days=1)
        yesterdays_date_str = yesterdays_datetime.strftime("%Y%m%d")
        todays_date_str = todays_datetime.strftime("%Y%m%d")

        games = Game_Record()
        games.load_data(connection, todays_date_str, yesterdays_date_str)
        games.load_models(ml_cls_model_path, dl_cls_model_path,
                          ml_reg_model_path, dl_reg_model_path)
        games.create_predictions()
        games.set_up_table()
        games.bet_direction()
        games.game_score()
        games.bet_amount(connection)
        games.game_details_and_cleanup()
        print(games.game_records.info())
        print(games.game_records.head(15))

        # stmt = f"""
        #         DELETE FROM game_records
        #         WHERE game_id LIKE '{yesterdays_date_str}%%'
        #         ;
        #         """

        # connection.execute(stmt)

        # games.save_records(connection)
