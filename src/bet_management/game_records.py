import datetime
import pytz
import numpy as np
import pandas as pd
import tensorflow as tf
from scipy import stats
from tensorflow import keras
from sqlalchemy import create_engine
from pycaret.regression import *

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
        self.ml_model = None
        self.dl_model = None
        self.ml_predictions = None
        self.dl_predictions = None

    def load_data(self, connection):
        """
        Wrapper for SQL query to load data.
        """
        query = """
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
        """

        self.inbound_data = pd.read_sql(sql=query, con=connection)

    def load_models(self, ml_model_path, dl_model_path):
        """
        Choosing which models to use for predictions.
        Make sure feature set matches model.
        """
        self.ml_model = load_model(ml_model_path)
        self.dl_model = keras.models.load_model(dl_model_path)

    def create_predictions(self):
        """
        Choosing feature set that matches models to be used.
        """

        ml_pred_feature_set = [
            "home_team_num",
            "away_team_num",
            "league_year_end",
            "home_line",
            "fd_line_home",
            "dk_line_home",
            "covers_consenses_home",
            "wins",
            "losses",
            "win_pct",
            "expected_wins",
            "expected_losses",
            "home_ppg",
            "home_papg",
            "away_wins",
            "away_losses",
            "away_win_pct",
            "away_expected_wins",
            "away_expected_losses",
            "away_ppg",
            "away_papg",
            "g",
            "mp",
            "pts",
            "ast",
            "trb",
            "blk",
            "stl",
            "tov",
            "pf",
            "drb",
            "orb",
            "fg",
            "fga",
            "fg_pct",
            "fg2",
            "fg2a",
            "fg2_pct",
            "fg3",
            "fg3a",
            "fg3_pct",
            "ft",
            "fta",
            "ft_pct",
            "away_g",
            "away_mp",
            "away_pts",
            "away_ast",
            "away_trb",
            "away_blk",
            "away_stl",
            "away_tov",
            "away_pf",
            "away_drb",
            "away_orb",
            "away_fg",
            "away_fga",
            "away_fg_pct",
            "away_fg2",
            "away_fg2a",
            "away_fg2_pct",
            "away_fg3",
            "away_fg3a",
            "away_fg3_pct",
            "away_ft",
            "away_fta",
            "away_fta_pct",
        ]

        dl_pred_feature_set = [
            "home_team_num",
            "away_team_num",
            "league_year_end",
            "home_line",
            "wins",
            "losses",
            "win_pct",
            "expected_wins",
            "expected_losses",
            "home_ppg",
            "home_papg",
            "away_wins",
            "away_losses",
            "away_win_pct",
            "away_expected_wins",
            "away_expected_losses",
            "away_ppg",
            "away_papg",
            "g",
            "mp",
            "pts",
            "ast",
            "trb",
            "blk",
            "stl",
            "tov",
            "pf",
            "drb",
            "orb",
            "fg",
            "fga",
            "fg_pct",
            "fg2",
            "fg2a",
            "fg2_pct",
            "fg3",
            "fg3a",
            "fg3_pct",
            "ft",
            "fta",
            "ft_pct",
            "away_g",
            "away_mp",
            "away_pts",
            "away_ast",
            "away_trb",
            "away_blk",
            "away_stl",
            "away_tov",
            "away_pf",
            "away_drb",
            "away_orb",
            "away_fg",
            "away_fga",
            "away_fg_pct",
            "away_fg2",
            "away_fg2a",
            "away_fg2_pct",
            "away_fg3",
            "away_fg3a",
            "away_fg3_pct",
            "away_ft",
            "away_fta",
            "away_fta_pct",
        ]

        self.ml_predictions = predict_model(
            self.ml_model, data=self.inbound_data[ml_pred_feature_set]
        )
        self.dl_predictions = self.dl_model.predict(
            self.inbound_data[dl_pred_feature_set]
        ).flatten()

    def set_up_table(self):
        self.game_records = pd.DataFrame(
            {
                "line_hv": 0 - self.inbound_data["home_line"],
                "ml_prediction": self.ml_predictions["Label"],
                "dl_prediction": self.dl_predictions,
                "home_score": self.inbound_data["home_score"],
                "away_score": self.inbound_data["away_score"],
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
        TODO: Bring in actual line prices. Low Priority
        """
        self.game_records["home_line"] = self.inbound_data[
            "open_line_home"
        ].fillna(self.inbound_data["home_spread"])
        self.game_records["home_line_price"] = -110
        self.game_records["away_line"] = self.inbound_data[
            "open_line_away"
        ].fillna(-self.inbound_data["home_spread"])
        self.game_records["away_line_price"] = -110
        self.game_records["ml_pred_vig"] = self.game_records.apply(
            lambda x: x["home_line_price"]
            if x["ml_pred_direction"] == "Home"
            else x["away_line_price"],
            axis=1,
        )

        self.game_records["dl_pred_vig"] = self.game_records.apply(
            lambda x: x["home_line_price"]
            if x["dl_pred_direction"] == "Home"
            else x["away_line_price"],
            axis=1,
        )

    def expected_value(self):
        """
        Long-term expected value on $100 bets given win_pct and vig.
        """

        def vig_ev_calc(vig):
            return round(1 / (-vig) * 100, 2) * 100

        self.game_records["ml_ev"] = self.game_records["ml_win_prob"].apply(
            lambda x: 100 * x - 100 * (1 - x)
        )
        self.game_records["dl_ev"] = self.game_records["dl_win_prob"].apply(
            lambda x: 100 * x - 100 * (1 - x)
        )
        self.game_records["ml_ev_vig"] = self.game_records.apply(
            lambda x: vig_ev_calc(x["ml_pred_vig"]) * x["ml_win_prob"]
            - 100 * (1 - x["ml_win_prob"]),
            axis=1,
        )
        self.game_records["dl_ev_vig"] = self.game_records.apply(
            lambda x: vig_ev_calc(x["dl_pred_vig"]) * x["dl_win_prob"]
            - 100 * (1 - x["dl_win_prob"]),
            axis=1,
        )

    def bet_direction(self):
        """
        Logic for choosing between models with differing reccommended bet directions.
        """
        self.game_records["rec_bet_direction"] = self.game_records.apply(
            lambda x: x["ml_pred_direction"]
            if x["ml_pred_direction"] == x["dl_pred_direction"]
            else "Warning-Models Differ",
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
            ml_ev = x["ml_ev"]
            dl_ev = x["dl_ev"]

            return ml_ev

        self.game_records["game_score"] = self.game_records.apply(
            game_score_calc, axis=1
        )

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

        current_available_funds = pd.read_sql(
            sql=available_funds_query, con=connection
        )["balance"]

        unit_size = 0.01
        unit_amount = current_available_funds * unit_size

        def calc_amount_to_bet(x, unit_size):
            if x["game_score"] > 95:
                units_to_bet = 5
            elif x["game_score"] > 90:
                units_to_bet = 3
            elif x["game_score"] > 85:
                units_to_bet = 1
            else:
                units_to_bet = 0
            amount_to_bet = unit_size * units_to_bet
            return amount_to_bet

        self.game_records["rec_bet_amount"] = self.game_records.apply(
            calc_amount_to_bet, unit_size=unit_amount, axis=1
        )

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
        self.game_records["bet_line"] = None  # Updates on user input
        self.game_records["bet_direction"] = None  # Updates on user input
        self.game_records["bet_price"] = None  # Updates on user input
        self.game_records["bet_location"] = None  # Updates on user input
        self.game_records["game_result"] = self.game_records.apply(
            lambda x: x["home_score"] - x["away_score"]
            if x["home_score"]
            else "Pending",
            axis=1,
        )
        self.game_records["bet_result"] = "No Bet"  # Updates after game end

        # def calc_bet_result(x):
        #     home_facing_game_result = x['game_result']
        #     home_facing_bet_line = -x['bet_line'] if x['bet_direction'] == 'Home' else x['bet_line']

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
            "game_result",
            "bet_result",
            "bet_amount",
            "bet_line",
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
            if_exists="replace",
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

    games = Game_Record()
    games.load_data(connection)
    games.load_models(ml_model_path, dl_model_path)
    games.create_predictions()
    games.set_up_table()
    games.model_pred_bet_direction()
    games.line_v_pred_margin()
    games.win_pct()
    games.line_and_line_price()
    games.expected_value()
    games.bet_direction()
    games.game_score()
    games.bet_amount(connection)
    games.game_info_and_cleanup()
    # print(games.game_records)
    games.save_records(connection)
