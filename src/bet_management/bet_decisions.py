import datetime
import os

import pandas as pd
import pytz
from dotenv import load_dotenv
from pycaret import classification as pyc_cls
from pycaret import regression as pyc_reg
from sqlalchemy import create_engine

load_dotenv()
RDS_ENDPOINT = os.getenv("RDS_ENDPOINT")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 100)


class Predictions:
    TIME_ZONE = "America/Denver"
    MODEL_FILE_LOCATION = "../../models/"

    def __init__(self, database_engine, line_source="fanduel"):
        """Initialize Game_Record with a database engine."""
        self.database_engine = database_engine
        self.line_source = line_source
        self.df = None
        self.prediction_df = None
        self.ml_cls_model_1 = None
        self.ml_reg_model_1 = None
        self.dl_cls_model = None
        self.dl_reg_model = None

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
                        games.open_line AS spread,
                        features.data,
                        lines.{self.line_source}_home_line
                    FROM games
                    LEFT JOIN all_features_json AS features 
                        ON games.game_id = features.game_id
                    LEFT JOIN (
                        SELECT game_id, fanduel_home_line
                        FROM (
                            SELECT game_id, fanduel_home_line, 
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

    def load_models(self, ml_cls_model_1_name=None, ml_reg_model_1_name=None):
        """Load machine learning models."""
        try:
            ml_cls_model_1_path = self.MODEL_FILE_LOCATION + ml_cls_model_1_name
            ml_reg_model_1_path = self.MODEL_FILE_LOCATION + ml_reg_model_1_name
            self.ml_cls_model_1 = pyc_cls.load_model(ml_cls_model_1_path)
            self.ml_reg_model_1 = pyc_reg.load_model(ml_reg_model_1_path)
        except Exception as e:
            print(f"An error occurred while loading models: {e}")

    def create_predictions(self, df, features):
        """Create predictions using loaded models and return a new DataFrame."""

        # Flatten the 'data' column to extract features
        data_df = df["data"].apply(pd.Series)

        # Select only the relevant features
        selected_features = data_df[features]

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

        # Add the predictions to the DataFrame
        new_df["ml_reg_pred_1"] = ml_reg_predictions_1["prediction_label"]
        new_df["ml_cls_pred_1"] = ml_cls_predictions_1["prediction_label"]
        new_df["ml_cls_prob_1"] = ml_cls_predictions_1["prediction_score"]

        # Remove the temporary feature columns used for prediction
        new_df.drop(features, axis=1, inplace=True)

        return new_df

    def set_up_table(self):
        # ! Need to update this if using a line that is not the open line
        self.prediction_df = pd.DataFrame(
            {
                "game_id": self.df["game_id"],
                "prediction_datetime": datetime.datetime.now(
                    pytz.timezone(self.TIME_ZONE)
                ),
                "open_line_hv": 0 - self.df["spread"],
                "prediction_line_hv": 0 - self.df["spread"],
                # "prediction_line_hv": 0 - self.df[f"{self.line_source}_home_line"],
                "ml_reg_pred_1": self.df["ml_reg_pred_1"],
                "ml_cls_pred_1": self.df["ml_cls_pred_1"],
                "ml_cls_prob_1": self.df["ml_cls_prob_1"],
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

        # Weighted Average
        return (ml_cls_rating_hv * 1) / 1

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


if __name__ == "__main__":
    database_engine = create_engine(
        f"postgresql+psycopg2://postgres:{RDS_PASSWORD}@{RDS_ENDPOINT}/nba_betting"
    )

    ml_cls_model_name = "AutoML/pycaret_cls_lr_2023_09_06_00_22_00"
    dl_cls_model_name = None
    ml_reg_model_name = "AutoML/pycaret_reg_linreg_2023_09_06_00_28_16"
    dl_reg_model_name = None

    feature_set = [
        "open_line",
        "rest_diff_hv",
        "day_of_season",
        "last_5_hv",
        "538_prob1",
        "elo_prob1",
        "streak_hv",
        "point_diff_last_5_hv",
        "point_diff_hv",
        "win_pct_hv",
        "plus_minus_home_l2w_traditional",
        "net_rating_home_l2w_advanced",
        "plus_minus_home_l2w_opponent",
        "plus_minus_zscore_home_l2w_traditional",
        "net_rating_zscore_home_l2w_advanced",
        "plus_minus_zscore_home_l2w_opponent",
        "e_net_rating_home_l2w_advanced",
        "e_net_rating_zscore_home_l2w_advanced",
        "plus_minus_percentile_home_l2w_opponent",
        "plus_minus_percentile_home_l2w_traditional",
        "net_rating_percentile_home_l2w_advanced",
        "plus_minus_away_l2w_traditional",
        "plus_minus_away_l2w_opponent",
        "w_pct_zscore_home_l2w_traditional",
        "e_net_rating_percentile_home_l2w_advanced",
        "e_net_rating_away_l2w_advanced",
        "pie_percentile_home_l2w_advanced",
        "e_net_rating_zscore_away_l2w_advanced",
        "net_rating_zscore_away_l2w_advanced",
        "pie_home_l2w_advanced",
    ]

    try:
        predictions = Predictions(database_engine=database_engine)
        predictions.load_models(
            ml_cls_model_1_name=ml_cls_model_name,
            ml_reg_model_1_name=ml_reg_model_name,
        )
        predictions.load_data(
            current_date=False, start_date="2010-09-01", end_date="2023-09-01"
        )
        predictions.df = predictions.create_predictions(
            predictions.df, features=feature_set
        )
        predictions.set_up_table()
        predictions.game_ratings()

        print(predictions.prediction_df.info())
        print(predictions.prediction_df.head(10))

        predictions.save_records()

        print("----- Predictions Update Successful -----")
    except Exception as e:
        print(f"----- Predictions Update Failed -----")
        raise e
