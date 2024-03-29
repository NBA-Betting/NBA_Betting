import datetime
import os

import autokeras as ak
import pandas as pd
import pytz
from dotenv import load_dotenv
from pycaret import classification as pyc_cls
from pycaret import regression as pyc_reg
from sqlalchemy import create_engine
from tensorflow.keras.models import load_model

load_dotenv()
DB_ENDPOINT = os.getenv("DB_ENDPOINT")
DB_PASSWORD = os.getenv("DB_PASSWORD")
NBA_BETTING_BASE_DIR = os.getenv("NBA_BETTING_BASE_DIR")

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 100)


class Predictions:
    def __init__(
        self,
        line_type,
        line_source="fanduel",
        DB_ENDPOINT=DB_ENDPOINT,
        DB_PASSWORD=DB_PASSWORD,
    ):
        """Initialize Game_Record with a database engine."""

        self.database_engine = create_engine(
            f"postgresql://postgres:{DB_PASSWORD}@{DB_ENDPOINT}/nba_betting"
        )
        self.line_type = line_type
        self.line_source = line_source
        self.df = None
        self.prediction_df = None
        self.ml_cls_model_1 = None
        self.ml_reg_model_1 = None
        self.dl_cls_model_1 = None
        self.dl_reg_model_1 = None

    def load_data(self, current_date=True, start_date=None, end_date=None):
        """Load data from database based on date filters."""
        try:
            start_datetime, end_datetime = self._get_date_range(
                current_date, start_date, end_date
            )
            query = f"""
                    SELECT
                        games.game_id,
                        games.game_datetime,
                        games.home_team,
                        games.away_team,
                        games.open_line AS open_line_copy,
                        features.data,
                        lines.{self.line_source}_home_line
                    FROM games
                    LEFT JOIN all_features_json AS features 
                        ON games.game_id = features.game_id
                    LEFT JOIN (
                        SELECT game_id, {self.line_source}_home_line
                        FROM (
                            SELECT game_id, {self.line_source}_home_line, 
                                ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY line_datetime DESC) AS rn
                            FROM lines
                        ) AS sub
                        WHERE sub.rn = 1
                    ) AS lines ON games.game_id = lines.game_id
                    WHERE games.game_datetime BETWEEN %s AND %s;
                    """
            self.df = pd.read_sql(
                query, self.database_engine, params=(start_datetime, end_datetime)
            )
        except Exception as e:
            print(f"An error occurred while loading data: {e}")

    def _get_date_range(self, current_date, start_date, end_date):
        """Internal method to get date range based on input flags."""
        if current_date:
            today_datetime = datetime.datetime.now(pytz.timezone(self.TIME_ZONE))
            today_str = today_datetime.strftime("%Y%m%d")
            return f"{today_str} 00:00:00", f"{today_str} 23:59:59"
        elif start_date and end_date:
            return f"{start_date} 00:00:00", f"{end_date} 23:59:59"
        else:
            raise ValueError("Invalid date parameters.")

    def load_models(
        self,
        ml_cls_model_1_path=None,
        ml_reg_model_1_path=None,
        dl_cls_model_1_path=None,
        dl_reg_model_1_path=None,
    ):
        """Load machine learning models."""
        try:
            self.ml_cls_model_1 = pyc_cls.load_model(ml_cls_model_1_path)
            self.ml_reg_model_1 = pyc_reg.load_model(ml_reg_model_1_path)
            self.dl_cls_model_1 = load_model(
                dl_cls_model_1_path, custom_objects=ak.CUSTOM_OBJECTS
            )
            self.dl_reg_model_1 = load_model(
                dl_reg_model_1_path, custom_objects=ak.CUSTOM_OBJECTS
            )

        except Exception as e:
            print(f"An error occurred while loading models: {e}")

    def create_predictions(self, df, features):
        """Create predictions using loaded models and return a new DataFrame."""

        # Flatten the 'data' column to extract features
        data_df = df["data"].apply(pd.Series)

        # Select only the relevant features
        selected_features = data_df[features]

        # Add the line to the selected features
        if self.line_type == "open":
            line_data = df["open_line_copy"]
        elif self.line_type == "current":
            line_data = df[f"{self.line_source}_home_line"]

        selected_features.insert(0, "open_line", line_data)

        # Remove rows where any of the selected features is null
        non_null_indices = selected_features.dropna().index
        selected_features = selected_features.loc[non_null_indices]
        df = df.loc[non_null_indices]

        # Create a new DataFrame by concatenating only the relevant feature columns
        new_df = pd.concat([df, selected_features], axis=1)

        # Make predictions using the selected features
        ml_cls_predictions_1 = pyc_cls.predict_model(
            self.ml_cls_model_1, data=selected_features
        )
        ml_reg_predictions_1 = pyc_reg.predict_model(
            self.ml_reg_model_1, data=selected_features
        )
        dl_cls_predictions_1 = self.dl_cls_model_1.predict(selected_features)
        dl_reg_predictions_1 = self.dl_reg_model_1.predict(selected_features)

        # Add the predictions to the DataFrame
        new_df["ml_reg_pred_1"] = ml_reg_predictions_1["prediction_label"]
        new_df["ml_cls_pred_1"] = ml_cls_predictions_1["prediction_label"]
        new_df["ml_cls_prob_1"] = ml_cls_predictions_1["prediction_score"]

        new_df["dl_reg_pred_1"] = dl_reg_predictions_1
        new_df["dl_cls_prob_1"] = dl_cls_predictions_1

        # Remove the temporary feature columns used for prediction
        new_df.drop(features, axis=1, inplace=True)

        return new_df

    def set_up_table(self):
        # Get the current time in 'America/Denver' timezone
        denver_time = datetime.datetime.now(pytz.timezone("America/Denver"))

        # Remove the timezone information and microseconds
        naive_denver_time = denver_time.replace(tzinfo=None, microsecond=0)

        if self.line_type == "open":
            prediction_line_hv = 0 - self.df["open_line_copy"]
        elif self.line_type == "current":
            prediction_line_hv = 0 - self.df[f"{self.line_source}_home_line"]

        # Setting up your DataFrame
        self.prediction_df = pd.DataFrame(
            {
                "game_id": self.df["game_id"],
                "prediction_datetime": naive_denver_time,
                "open_line_hv": 0 - self.df["open_line_copy"],
                "prediction_line_hv": prediction_line_hv,
                "ml_reg_pred_1": self.df["ml_reg_pred_1"],
                "ml_cls_pred_1": self.df["ml_cls_pred_1"],
                "ml_cls_prob_1": self.df["ml_cls_prob_1"],
                "dl_reg_pred_1": self.df["dl_reg_pred_1"],
                "dl_cls_prob_1": self.df["dl_cls_prob_1"],
            }
        )

    def _ml_cls_rating_hv(self, x):
        if x["ml_cls_pred_1"] == True:
            return x["ml_cls_prob_1"]
        elif x["ml_cls_pred_1"] == False:
            return 1 - x["ml_cls_prob_1"]
        else:
            return 0

    def _game_rating_hv(self, x):
        # Components
        ml_cls_rating_hv = x["ml_cls_rating_hv"]
        dl_cls_rating_hv = x["dl_cls_rating_hv"]

        # Weighted Average
        return ((ml_cls_rating_hv * 1) + (dl_cls_rating_hv * 1)) / 2

    def _prediction_direction(self, x):
        if x["game_rating_hv"] > 0.5:
            return "Home"
        elif x["game_rating_hv"] < 0.5:
            return "Away"
        else:
            return None

    def _directional_game_rating(self, x):
        if x["prediction_direction"] == "Home":
            directional_game_rating = x["game_rating_hv"]
        elif x["prediction_direction"] == "Away":
            directional_game_rating = 1 - x["game_rating_hv"]
        else:
            return None
        return directional_game_rating * 100

    def game_ratings(self):
        """
        Determines the rating of the game based on the model predictions.
        """
        # Individual predictions
        self.prediction_df["ml_cls_rating_hv"] = self.prediction_df.apply(
            self._ml_cls_rating_hv, axis=1
        )
        self.prediction_df["dl_cls_rating_hv"] = self.prediction_df["dl_cls_prob_1"]
        # Cumulative prediction
        self.prediction_df["game_rating_hv"] = self.prediction_df.apply(
            self._game_rating_hv, axis=1
        )
        # Bet Direction
        self.prediction_df["prediction_direction"] = self.prediction_df.apply(
            self._prediction_direction, axis=1
        )
        # Directional Game Rating
        self.prediction_df["directional_game_rating"] = self.prediction_df.apply(
            self._directional_game_rating, axis=1
        )

    def save_records(self):
        """Save records to the database."""
        try:
            self.prediction_df.to_sql(
                "predictions",
                self.database_engine,
                if_exists="append",
                index=False,
            )
        except Exception as e:
            print(f"An error occurred while saving records: {e}")


def main_predictions(current_date, start_date, end_date, line_type="open"):
    ml_cls_model_path = (
        NBA_BETTING_BASE_DIR + "/models/AutoML/pycaret_cls_nb_2023_10_30_06_26_58"
    )
    dl_cls_model_path = (
        NBA_BETTING_BASE_DIR + "/models/AutoDL/autokeras_cls_dl_2023_10_30_06_36_54"
    )
    ml_reg_model_path = (
        NBA_BETTING_BASE_DIR + "/models/AutoML/pycaret_reg_lasso_2023_10_30_06_31_04"
    )
    dl_reg_model_path = (
        NBA_BETTING_BASE_DIR + "/models/AutoDL/autokeras_reg_dl_2023_10_30_06_40_04"
    )

    feature_set = [
        "rest_diff_hv",
        "day_of_season",
        "last_5_hv",
        "streak_hv",
        "point_diff_last_5_hv",
        "point_diff_hv",
        "win_pct_hv",
        "pie_percentile_away_all_advanced",
        "home_team_avg_point_diff",
        "net_rating_away_all_advanced",
        "net_rating_home_all_advanced",
        "plus_minus_home_all_traditional",
        "e_net_rating_zscore_away_all_advanced",
        "net_rating_zscore_away_all_advanced",
        "plus_minus_away_all_opponent",
        "away_team_avg_point_diff",
        "plus_minus_away_all_traditional",
        "pie_zscore_away_all_advanced",
        "e_net_rating_away_all_advanced",
        "plus_minus_percentile_away_all_traditional",
        "net_rating_zscore_home_l2w_advanced",
        "e_net_rating_home_all_advanced",
        "w_zscore_away_all_traditional",
        "pie_away_all_advanced",
        "w_pct_zscore_away_all_traditional",
        "e_net_rating_percentile_away_l2w_advanced",
    ]

    try:
        predictions = Predictions(line_type=line_type)
        predictions.load_models(
            ml_cls_model_1_path=ml_cls_model_path,
            ml_reg_model_1_path=ml_reg_model_path,
            dl_cls_model_1_path=dl_cls_model_path,
            dl_reg_model_1_path=dl_reg_model_path,
        )
        predictions.load_data(
            current_date=current_date, start_date=start_date, end_date=end_date
        )
        predictions.df = predictions.create_predictions(
            predictions.df, features=feature_set
        )
        predictions.set_up_table()
        predictions.game_ratings()

        print(predictions.prediction_df.info())
        print(
            predictions.prediction_df.sort_values("game_id", ascending=False).head(10)
        )

        predictions.save_records()

        print("----- Predictions Update Successful -----")
    except Exception as e:
        print(f"----- Predictions Update Failed -----")
        raise e


def on_demand_predictions(
    current_date, start_date=None, end_date=None, line_type="current"
):
    ml_cls_model_path = (
        NBA_BETTING_BASE_DIR + "/models/AutoML/pycaret_cls_nb_2023_10_30_06_26_58"
    )
    dl_cls_model_path = (
        NBA_BETTING_BASE_DIR + "/models/AutoDL/autokeras_cls_dl_2023_10_30_06_36_54"
    )
    ml_reg_model_path = (
        NBA_BETTING_BASE_DIR + "/models/AutoML/pycaret_reg_lasso_2023_10_30_06_31_04"
    )
    dl_reg_model_path = (
        NBA_BETTING_BASE_DIR + "/models/AutoDL/autokeras_reg_dl_2023_10_30_06_40_04"
    )

    feature_set = [
        "rest_diff_hv",
        "day_of_season",
        "last_5_hv",
        "streak_hv",
        "point_diff_last_5_hv",
        "point_diff_hv",
        "win_pct_hv",
        "pie_percentile_away_all_advanced",
        "home_team_avg_point_diff",
        "net_rating_away_all_advanced",
        "net_rating_home_all_advanced",
        "plus_minus_home_all_traditional",
        "e_net_rating_zscore_away_all_advanced",
        "net_rating_zscore_away_all_advanced",
        "plus_minus_away_all_opponent",
        "away_team_avg_point_diff",
        "plus_minus_away_all_traditional",
        "pie_zscore_away_all_advanced",
        "e_net_rating_away_all_advanced",
        "plus_minus_percentile_away_all_traditional",
        "net_rating_zscore_home_l2w_advanced",
        "e_net_rating_home_all_advanced",
        "w_zscore_away_all_traditional",
        "pie_away_all_advanced",
        "w_pct_zscore_away_all_traditional",
        "e_net_rating_percentile_away_l2w_advanced",
    ]

    try:
        predictions = Predictions(line_type=line_type)
        predictions.load_models(
            ml_cls_model_1_path=ml_cls_model_path,
            ml_reg_model_1_path=ml_reg_model_path,
            dl_cls_model_1_path=dl_cls_model_path,
            dl_reg_model_1_path=dl_reg_model_path,
        )
        predictions.load_data(
            current_date=current_date, start_date=start_date, end_date=end_date
        )
        predictions.df = predictions.create_predictions(
            predictions.df, features=feature_set
        )
        predictions.set_up_table()
        predictions.game_ratings()

        print(predictions.prediction_df.info())
        print(
            predictions.prediction_df.sort_values("game_id", ascending=False).head(10)
        )

        # predictions.save_records()

        print("----- Predictions Update Successful -----")
    except Exception as e:
        print(f"----- Predictions Update Failed -----")
        raise e


if __name__ == "__main__":
    # main_predictions(current_date=False, start_date="2020-09-01", end_date="2023-10-29")
    # on_demand_predictions(
    #     current_date=False, start_date="2023-09-01", end_date="2023-10-29"
    # )
    pass
