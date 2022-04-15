import datetime
import pytz
import numpy as np
import pandas as pd
import tensorflow as tf
from scipy import stats
from tensorflow import keras
from sqlalchemy import create_engine
from pycaret.regression import *


class Game_Record:
    """
    Pipeline to create game_records and save to database.
    Inbound - Trained ML and DL models,
              Data from combined_data_inbound and model_ready tables

    """

    def __init__(self):
        self.inbound_data = None
        self.game_records = None
        self.ml_model = None
        self.dl_model = None
        self.ml_predictions = None
        self.dl_predictions = None

    def load_data(self, connection, date):
        """
        Wrapper for SQL query to load data.
        """
        query = f"""
        SELECT cdi.game_id,
        cdi.game_date,
        cdi.home_team,
        cdi.away_team,
        cdi.home_score,
        cdi.away_score,
        cdi.home_result,
        cdi.covers_game_url,
        cdi.league_year,
        cdi.home_spread,
        cdi.home_spread_result,
        cdi.pred_date,
        cdi.open_line_home,
        cdi.open_line_away,
        cdi.fd_line_price_home,
        cdi.fd_line_away,
        cdi.fd_line_price_away,
        cdi.dk_line_price_home,
        cdi.dk_line_away,
        cdi.dk_line_price_away,
        cdi.covers_consenses_away,
        mr.home_team_num,
        mr.away_team_num,
        mr.league_year_end,
        mr.home_line,
        mr.fd_line_home,
        mr.dk_line_home,
        mr.covers_consenses_home,
        mr.wins,
        mr.losses,
        mr.win_pct,
        mr.expected_wins,
        mr.expected_losses,
        mr.home_ppg,
        mr.home_papg,
        mr.away_wins,
        mr.away_losses,
        mr.away_win_pct,
        mr.away_expected_wins,
        mr.away_expected_losses,
        mr.away_ppg,
        mr.away_papg,
        mr.g,
        mr.mp,
        mr.pts,
        mr.ast,
        mr.trb,
        mr.blk,
        mr.stl,
        mr.tov,
        mr.pf,
        mr.drb,
        mr.orb,
        mr.fg,
        mr.fga,
        mr.fg_pct,
        mr.fg2,
        mr.fg2a,
        mr.fg2_pct,
        mr.fg3,
        mr.fg3a,
        mr.fg3_pct,
        mr.ft,
        mr.fta,
        mr.ft_pct,
        mr.away_g,
        mr.away_mp,
        mr.away_pts,
        mr.away_ast,
        mr.away_trb,
        mr.away_blk,
        mr.away_stl,
        mr.away_tov,
        mr.away_pf,
        mr.away_drb,
        mr.away_orb,
        mr.away_fg,
        mr.away_fga,
        mr.away_fg_pct,
        mr.away_fg2,
        mr.away_fg2a,
        mr.away_fg2_pct,
        mr.away_fg3,
        mr.away_fg3a,
        mr.away_fg3_pct,
        mr.away_ft,
        mr.away_fta,
        mr.away_fta_pct
        FROM combined_data_inbound AS cdi
        LEFT OUTER JOIN model_ready AS mr
        ON cdi.game_id = mr.game_id
        WHERE cdi.game_id LIKE '{date}%%'"""

        self.inbound_data = pd.read_sql(sql=query, con=connection)

    def load_models(self, ml_model_path, dl_model_path):
        self.ml_model = load_model(ml_model_path)
        self.dl_model = keras.models.load_model(dl_model_path)

    def create_predictions(self):
        """
        TODO: Improve choice of inbound features. (iloc[:, 21:])
        TODO: Functionality for diff models with diff features. (with or without fd and dk lines)
        """
        self.ml_predictions = predict_model(
            self.ml_model, data=self.inbound_data.iloc[:, 21:]
        )
        self.dl_predictions = self.dl_model.predict(
            self.inbound_data.iloc[:, 21:].drop(
                columns=[
                    "fd_line_home",
                    "dk_line_home",
                    "covers_consenses_home",
                ]
            )
        ).flatten()

    def set_up_table(self):
        self.game_records = pd.DataFrame(
            {
                "line_hv": 0 - self.inbound_data["home_line"],
                "ml_prediction": self.ml_predictions["Label"],
                "dl_prediction": self.dl_predictions,
            }
        )

    def model_pred_bet_direction(self):
        """
        Determines if the model prediction is for the home or away team.
        """
        self.game_records["is_home_pred_greater_ML"] = (
            self.game_records["ml_prediction"] > self.game_records["line_hv"]
        )
        self.game_records["is_home_pred_greater_DL"] = (
            self.game_records["dl_prediction"] > self.game_records["line_hv"]
        )
        self.game_records["ml_pred_direction"] = self.game_records[
            "is_home_pred_greater_ML"
        ].apply(lambda x: "Home" if x is True else "Away")
        self.game_records["dl_pred_direction"] = self.game_records[
            "is_home_pred_greater_DL"
        ].apply(lambda x: "Home" if x is True else "Away")

    def line_v_pred_margin(self):
        """
        Difference between opening line and model prediction.
        """
        self.game_records["ml_pred_line_margin"] = self.game_records.apply(
            lambda x: x["ml_prediction"] - x["line_hv"]
            if x["ml_pred_direction"] == "Home"
            else x["line_hv"] - x["ml_prediction"],
            axis=1,
        )
        self.game_records["dl_pred_line_margin"] = self.game_records.apply(
            lambda x: x["dl_prediction"] - x["line_hv"]
            if x["dl_pred_direction"] == "Home"
            else x["line_hv"] - x["dl_prediction"],
            axis=1,
        )

    def win_pct(self):
        """
        Converts model vs. line margin into a win percentage value.
        TODO: Wording on calculation. What am I actually calculating here?
        TODO: Research which distribution to use. Empirical or calculated from data?
        """
        std_dev_ML = np.std(self.game_records["ml_pred_line_margin"])
        std_dev_DL = np.std(self.game_records["dl_pred_line_margin"])
        norm_dist = stats.norm
        self.game_records["ml_win_prob"] = self.game_records[
            "ml_pred_line_margin"
        ].apply(lambda x: norm_dist.cdf(x, loc=0, scale=std_dev_ML))
        self.game_records["dl_win_prob"] = self.game_records[
            "dl_pred_line_margin"
        ].apply(lambda x: norm_dist.cdf(x, loc=0, scale=std_dev_DL))

    def line_and_line_price(self):
        """
        Currently using -110 as a typical line price.
        TODO: Bring in actual line prices. Lower Priority
        """
        self.game_records["home_line"] = self.inbound_data[
            "open_line_home"
        ].fillna(self.inbound_data["home_spread"])
        self.game_records["home_line_price"] = -110
        self.game_records["away_line"] = self.inbound_data[
            "open_line_away"
        ].fillna(-self.inbound_data["home_spread"])
        self.game_records["away_line_price"] = -110

    def expected_value(self):
        """
        Long-term expected value on $100 bets given win_pct and vig.
        TODO: Use formula actual line price from above instead of hard coded "91" for vig.
        """
        self.game_records["ml_ev"] = self.game_records["ml_win_prob"].apply(
            lambda x: 100 * x - 100 * (1 - x)
        )
        self.game_records["dl_ev"] = self.game_records["dl_win_prob"].apply(
            lambda x: 100 * x - 100 * (1 - x)
        )
        self.game_records["ml_ev_vig"] = self.game_records[
            "ml_win_prob"
        ].apply(lambda x: 91 * x - 100 * (1 - x))
        self.game_records["dl_ev_vig"] = self.game_records[
            "dl_win_prob"
        ].apply(lambda x: 91 * x - 100 * (1 - x))

    def bet_direction(self):
        """
        Logic for choosing between models with differing reccommended bet directions.
        TODO: Determine optimal formula and implement.
        """
        self.game_records["rec_bet_direction"] = self.game_records[
            "ml_pred_direction"
        ]

    def game_score(self):
        """
        Overall score for the desirability of betting on a game.
        Incorporates model predictions with outside knowledge.
        0 to 100 scale.
        TODO: Determine optimal formula and implement.
        """
        self.game_records["game_score"] = self.game_records["ml_ev"]

    def bet_amount(self):
        """
        How much should be bet on each game based on
        game scores, available funds, and overall bankroll management plan.
        TODO: Determine optimal formula and implement.
        """
        self.game_records["rec_bet_amount"] = 100

    def pred_win_loss(self):
        """
        Bet expected value scaled for bet amount.
        TODO: Add logic to incorporate both ML and DL EV and EV with Vig
        """
        self.game_records["predicted_win_loss"] = (
            self.game_records["ml_ev"] / 100
        ) * self.game_records["rec_bet_amount"]

    def game_info_and_cleanup(self):
        """
        Adding extra game info for display and ordering columns.

        """
        self.game_records["game_id"] = self.inbound_data["game_id"]
        self.game_records["game_info"] = self.inbound_data["covers_game_url"]
        self.game_records["date"] = self.inbound_data["game_date"].dt.date
        self.game_records["time"] = self.inbound_data["game_date"].dt.time
        self.game_records["home"] = self.inbound_data["home_team"]
        self.game_records["away"] = self.inbound_data["away_team"]
        self.game_records["bet_amount"] = None  # Updates on user input
        self.game_records["bet_direction"] = None  # Updates on user input
        self.game_records["bet_price"] = None  # Updates on user input
        self.game_records["bet_location"] = None  # Updates on user input
        self.game_records["game_result"] = 0  # Updates after game end
        self.game_records["bet_result"] = "No Bet"  # Updates after game end
        self.game_records["bet_win_loss"] = None  # Updates after game end

        ordered_cols = [
            "game_id",
            "game_info",
            "date",
            "time",
            "home",
            "away",
            "home_line",
            "home_line_price",
            "away_line",
            "away_line_price",
            "ml_prediction",
            "ml_pred_direction",
            "ml_pred_line_margin",
            "ml_win_prob",
            "ml_ev",
            "ml_ev_vig",
            "dl_prediction",
            "dl_pred_direction",
            "dl_pred_line_margin",
            "dl_win_prob",
            "dl_ev",
            "dl_ev_vig",
            "game_score",
            "rec_bet_direction",
            "rec_bet_amount",
            "predicted_win_loss",
            "game_result",
            "bet_result",
            "bet_amount",
            "bet_direction",
            "bet_price",
            "bet_location",
            "bet_win_loss",
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

    username = "postgres"
    password = ""
    endpoint = ""
    database = "nba_betting"
    port = "5432"

    connection = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    ).connect()

    ml_model_path = "../../models/AutoML/Baseline_Ridge_Reg_PyCaret"
    dl_model_path = (
        "../../models/Deep_Learning/original_baseline_2022_01_20_01_59_27"
    )

    game_records = Game_Record()
    game_records.load_data(connection, todays_date_str)
    game_records.load_models(ml_model_path, dl_model_path)
    game_records.create_predictions()
    game_records.set_up_table()
    game_records.model_pred_bet_direction()
    game_records.line_v_pred_margin()
    game_records.win_pct()
    game_records.line_and_line_price()
    game_records.expected_value()
    game_records.bet_direction()
    game_records.game_score()
    game_records.bet_amount()
    game_records.pred_win_loss()
    game_records.game_info_and_cleanup()
    # print(game_records.game_records.sample(n=100))
    game_records.save_records(connection)
