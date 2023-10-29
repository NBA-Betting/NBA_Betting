import json
import os
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, mean_absolute_error

from config import NBA_IMPORTANT_DATES


class ModelSetup:
    def __init__(self):
        pass

    @staticmethod
    def add_targets(df):
        df_copy = df.copy()

        actual_score_diff_hv = df_copy["home_score"] - df_copy["away_score"]
        vegas_score_diff_hv = -df_copy["open_line"]

        df_copy["REG_TARGET"] = actual_score_diff_hv
        df_copy["CLS_TARGET"] = actual_score_diff_hv > vegas_score_diff_hv

        return df_copy

    @staticmethod
    def choose_dates(training_seasons, testing_seasons, season_type):
        training_dates = []
        testing_dates = []

        for season in training_seasons:
            year_str = f"{season}-{season+1}"
            if year_str not in NBA_IMPORTANT_DATES:
                raise ValueError(f"Season {year_str} not in NBA_IMPORTANT_DATES.")

            if season_type.lower() == "reg":
                training_dates.append(
                    (
                        NBA_IMPORTANT_DATES[year_str]["reg_season_start_date"],
                        NBA_IMPORTANT_DATES[year_str]["reg_season_end_date"],
                    )
                )
            elif season_type.lower() == "post":
                training_dates.append(
                    (
                        NBA_IMPORTANT_DATES[year_str]["postseason_start_date"],
                        NBA_IMPORTANT_DATES[year_str]["postseason_end_date"],
                    )
                )
            elif season_type.lower() == "both":
                training_dates.append(
                    (
                        NBA_IMPORTANT_DATES[year_str]["reg_season_start_date"],
                        NBA_IMPORTANT_DATES[year_str]["postseason_end_date"],
                    )
                )
            else:
                raise ValueError(
                    'Invalid season_type. Must be "Reg", "Post", or "Both".'
                )

        for season in testing_seasons:
            year_str = f"{season}-{season+1}"
            if year_str not in NBA_IMPORTANT_DATES:
                raise ValueError(f"Season {year_str} not in NBA_IMPORTANT_DATES.")

            if season_type.lower() == "reg":
                testing_dates.append(
                    (
                        NBA_IMPORTANT_DATES[year_str]["reg_season_start_date"],
                        NBA_IMPORTANT_DATES[year_str]["reg_season_end_date"],
                    )
                )
            elif season_type.lower() == "post":
                testing_dates.append(
                    (
                        NBA_IMPORTANT_DATES[year_str]["postseason_start_date"],
                        NBA_IMPORTANT_DATES[year_str]["postseason_end_date"],
                    )
                )
            elif season_type.lower() == "both":
                testing_dates.append(
                    (
                        NBA_IMPORTANT_DATES[year_str]["reg_season_start_date"],
                        NBA_IMPORTANT_DATES[year_str]["postseason_end_date"],
                    )
                )
            else:
                raise ValueError(
                    'Invalid season_type. Must be "Reg", "Post", or "Both".'
                )

        # Flatten the list of tuples into single tuple for training and testing
        training_start_date = min(date[0] for date in training_dates)
        training_end_date = max(date[1] for date in training_dates)
        testing_start_date = min(date[0] for date in testing_dates)
        testing_end_date = max(date[1] for date in testing_dates)

        return (training_start_date, training_end_date), (
            testing_start_date,
            testing_end_date,
        )

    @staticmethod
    def create_datasets(
        df, cls_or_reg, features, training_dates, testing_dates, create_report=False
    ):
        # Problem Type
        if cls_or_reg.lower() == "cls":
            problem_type = "cls"
            target = "CLS_TARGET"
        elif cls_or_reg.lower() == "reg":
            problem_type = "reg"
            target = "REG_TARGET"
        else:
            raise ValueError('cls_or_reg must be either "cls" or "reg"')

        # Convert date strings to Timestamps for easier comparison
        training_start_date, training_end_date = [
            pd.Timestamp(date) for date in training_dates
        ]
        testing_start_date, testing_end_date = [
            pd.Timestamp(date) for date in testing_dates
        ]

        # Check for overlapping date ranges
        if not (
            training_end_date < testing_start_date
            or testing_end_date < training_start_date
        ):
            warnings.warn("Warning: Training and testing date ranges overlap.")

        # Extract YYYYMMDD part from game_id for date filtering
        df["game_date_part"] = df["game_id"].str[:8].astype(int)

        # Convert YYYYMMDD to Timestamp
        df["game_date_part"] = pd.to_datetime(df["game_date_part"], format="%Y%m%d")

        # Dates for training and testing
        training_df = df[
            (df["game_date_part"] >= training_start_date)
            & (df["game_date_part"] <= training_end_date)
        ].copy()

        testing_df = df[
            (df["game_date_part"] >= testing_start_date)
            & (df["game_date_part"] <= testing_end_date)
        ].copy()

        # Remove the temporary 'game_date_part' column
        df.drop("game_date_part", axis=1, inplace=True)

        # Create Baselines
        (
            ind_baseline_train,
            ind_baseline_test,
            dep_baseline_train,
            dep_baseline_test,
        ) = ModelSetup._create_baselines(training_df, testing_df, problem_type)

        # Feature Selection
        if problem_type == "reg":
            columns = ["game_id", "vegas_open_hv", target] + features
        elif problem_type == "cls":
            columns = ["game_id", target] + features
        training_df = training_df[columns]
        testing_df = testing_df[columns]

        # Create Report
        if create_report:
            report = {
                "datetime": pd.Timestamp.now(),
                "problem_type": problem_type,
                "target": target,
                "features": features,
                "training_start_date": training_start_date.strftime("%Y-%m-%d"),
                "training_end_date": training_end_date.strftime("%Y-%m-%d"),
                "testing_start_date": testing_start_date.strftime("%Y-%m-%d"),
                "testing_end_date": testing_end_date.strftime("%Y-%m-%d"),
                "training_df_shape": training_df.shape,
                "testing_df_shape": testing_df.shape,
                "ind_baseline_train": ind_baseline_train,
                "ind_baseline_test": ind_baseline_test,
                "dep_baseline_train": dep_baseline_train,
                "dep_baseline_test": dep_baseline_test,
            }
            return training_df, testing_df, report
        else:
            return training_df, testing_df

    @staticmethod
    def _create_baselines(training_df, testing_df, problem_type):
        # Baseline Creation
        if problem_type == "cls":
            # Independent Baseline: Always picking the home side
            ind_baseline_predictions_train = [True] * len(training_df)
            ind_baseline_train = accuracy_score(
                training_df["CLS_TARGET"], ind_baseline_predictions_train
            )

            ind_baseline_predictions_test = [True] * len(testing_df)
            ind_baseline_test = accuracy_score(
                testing_df["CLS_TARGET"], ind_baseline_predictions_test
            )

            # Dependent Baseline: Majority Class from Training Set
            dep_baseline_train_prediction = training_df["CLS_TARGET"].mode()[0]
            dep_baseline_train = accuracy_score(
                training_df["CLS_TARGET"],
                [dep_baseline_train_prediction] * len(training_df),
            )
            dep_baseline_test = accuracy_score(
                testing_df["CLS_TARGET"],
                [dep_baseline_train_prediction] * len(testing_df),
            )

        elif problem_type == "reg":
            # Independent Baseline: MAE based on Vegas miss
            training_df.loc[:, "actual_score_diff_hv"] = (
                training_df["home_score"] - training_df["away_score"]
            )
            testing_df.loc[:, "actual_score_diff_hv"] = (
                testing_df["home_score"] - testing_df["away_score"]
            )
            training_df.loc[:, "vegas_open_hv"] = -training_df["open_line"]
            testing_df.loc[:, "vegas_open_hv"] = -testing_df["open_line"]

            ind_baseline_train = mean_absolute_error(
                training_df["actual_score_diff_hv"], training_df["vegas_open_hv"]
            )
            ind_baseline_test = mean_absolute_error(
                testing_df["actual_score_diff_hv"], testing_df["vegas_open_hv"]
            )

            # Dependent Baseline: Mean of Training Set
            dep_baseline_train_prediction = training_df["REG_TARGET"].mean()
            dep_baseline_train = mean_absolute_error(
                training_df["REG_TARGET"],
                [dep_baseline_train_prediction] * len(training_df),
            )
            dep_baseline_test = mean_absolute_error(
                testing_df["REG_TARGET"],
                [dep_baseline_train_prediction] * len(testing_df),
            )

        else:
            raise ValueError('Invalid problem_type. Must be "cls" or "reg".')

        return (
            ind_baseline_train,
            ind_baseline_test,
            dep_baseline_train,
            dep_baseline_test,
        )


def evaluate_reg_model(df, vegas_column, actual_column, prediction_column, display=True):
    df = df.copy()
    # create prediction side column
    df["pred_side"] = df.apply(
        lambda row: "away" if row[prediction_column] < row[vegas_column] else "home",
        axis=1,
    )
    # create actual side column
    df["actual_side"] = df.apply(
        lambda row: "away" if row[actual_column] < row[vegas_column] else "home", axis=1
    )
    # create which is closer column
    df["closer_to_target"] = df.apply(
        lambda row: abs(row[actual_column] - row[prediction_column])
        < abs(row[actual_column] - row[vegas_column]),
        axis=1,
    )

    closer_to_target_mean = df["closer_to_target"].mean()
    accuracy = accuracy_score(df["actual_side"], df["pred_side"])

    if display:
        # print % of Trues in the which is closer column
        closer_to_target_percent = closer_to_target_mean * 100
        print(
            f"Prediction is closer to target in {closer_to_target_percent:.2f}% of cases"
        )
        print(f"Accuracy: {accuracy:.4f}")

    return accuracy, closer_to_target_mean, df


def calculate_roi(df, actual_column, pred_column, pred_prob=None):
    df = df.copy()

    # Adding new columns to the DataFrame to track if the prediction was correct or not
    df["even_win"] = df.apply(
        lambda row: 100 if row[actual_column] == row[pred_column] else -100, axis=1
    )
    df["typical_win"] = df.apply(
        lambda row: 91 if row[actual_column] == row[pred_column] else -100, axis=1
    )

    # Calculate total ROIs
    total_roi_even = df["even_win"].sum()
    total_roi_typical = df["typical_win"].sum()

    # Calculate average ROIs per bet
    average_roi_even = round(total_roi_even / df.shape[0], 2)
    average_roi_typical = round(total_roi_typical / df.shape[0], 2)

    # Prepare result as a DataFrame
    result = pd.DataFrame(
        {
            "Label": ["All Bets, Even Amount", "All Bets, Typical Odds"],
            "Total ROI": [total_roi_even, total_roi_typical],
            "Average ROI per Bet": [average_roi_even, average_roi_typical],
        }
    )

    # Add extra rows if pred_prob is not None
    if pred_prob is not None:
        cutoffs = [0.50, 0.55, 0.60, 0.65, 0.70]
        for cutoff in cutoffs:
            filtered_df = df[df[pred_prob] > cutoff]

            # Calculate total ROIs for filtered DataFrame
            total_roi_even_filtered = filtered_df["even_win"].sum()
            total_roi_typical_filtered = filtered_df["typical_win"].sum()

            # Calculate average ROIs per bet for filtered DataFrame
            average_roi_even_filtered = (
                round(total_roi_even_filtered / filtered_df.shape[0], 2)
                if not filtered_df.empty
                else 0
            )
            average_roi_typical_filtered = (
                round(total_roi_typical_filtered / filtered_df.shape[0], 2)
                if not filtered_df.empty
                else 0
            )

            # Add new rows to result
            new_rows = pd.DataFrame(
                {
                    "Label": [
                        f"Cutoff {int(cutoff*100)}% Bets, Even Odds",
                        f"Cutoff {int(cutoff*100)}% Bets, Typical Odds",
                    ],
                    "Total ROI": [total_roi_even_filtered, total_roi_typical_filtered],
                    "Average ROI per Bet": [
                        average_roi_even_filtered,
                        average_roi_typical_filtered,
                    ],
                }
            )
            result = pd.concat([result, new_rows], ignore_index=True)

        # Calculate ROI using Kelly Criterion
        df["bet_fraction"] = df[pred_prob].map(lambda p: 2 * p - 1 if p > 0.5 else 0)
        total_roi_even_kelly = 0
        total_roi_typical_kelly = 0

        # Group the dataframe by date
        # Extract the date from the game_id column and create a new column for it
        df["extracted_date"] = df["game_id"].apply(lambda x: str(x)[:8])

        # Group the dataframe by the extracted date
        grouped = df.groupby("extracted_date")

        # Initialize an empty list to store modified groups
        modified_groups = []

        # Initialize variables to hold the total ROI using Kelly Criterion
        total_roi_even_kelly = 0
        total_roi_typical_kelly = 0

        # Process each group (i.e., each date's games) separately
        for _, group in grouped:
            # Make a copy of the group to avoid in-place modification
            group_copy = group.copy()

            total_fraction = group_copy["bet_fraction"].sum()
            group_copy["bet_fraction"] /= total_fraction
            group_copy["bet_size"] = (
                group_copy["bet_fraction"] * 100 * group_copy.shape[0]
            )

            group_copy["even_win_kelly"] = group_copy.apply(
                lambda row: row["bet_size"]
                if row[actual_column] == row[pred_column]
                else -row["bet_size"],
                axis=1,
            )
            group_copy["typical_win_kelly"] = group_copy.apply(
                lambda row: 0.91 * row["bet_size"]
                if row[actual_column] == row[pred_column]
                else -row["bet_size"],
                axis=1,
            )

            # Update the total ROI based on this group's results
            total_roi_even_kelly += group_copy["even_win_kelly"].sum()
            total_roi_typical_kelly += group_copy["typical_win_kelly"].sum()

            # Append the modified group to the list
            modified_groups.append(group_copy)

        # Concatenate all modified groups back together
        df_modified = pd.concat(modified_groups, ignore_index=True)

        # Calculate average ROI using Kelly Criterion
        average_roi_even_kelly = round(total_roi_even_kelly / df_modified.shape[0], 2)
        average_roi_typical_kelly = round(
            total_roi_typical_kelly / df_modified.shape[0], 2
        )

        new_rows_kelly = pd.DataFrame(
            {
                "Label": [
                    "All Bets, Even Amount, Kelly Criterion",
                    "All Bets, Typical Odds, Kelly Criterion",
                ],
                "Total ROI": [total_roi_even_kelly, total_roi_typical_kelly],
                "Average ROI per Bet": [
                    average_roi_even_kelly,
                    average_roi_typical_kelly,
                ],
            }
        )
        result = pd.concat([result, new_rows_kelly], ignore_index=True)

    return result


def default_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()  # for datetime objects
    elif isinstance(obj, (np.float32, np.float64)):
        return float(obj)  # convert NumPy float32 to Python native float
    elif isinstance(obj, np.integer):
        return int(obj)  # convert NumPy integer to Python native int
    elif isinstance(obj, np.ndarray):
        return obj.tolist()  # convert NumPy array to Python list
    # Add any other types as needed here
    raise TypeError(f"Type {type(obj)} not serializable")


def save_model_report(model_report, file_path="../models/model_reports.json"):
    # Check if the file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    # Convert the model_report dictionary to a JSON string
    new_report_json = json.dumps(model_report, default=default_serializer)

    # Append the new report to the file with a newline character for 'lines' demarcation
    with open(file_path, "a") as file:  # 'a' indicates append mode
        file.write(
            new_report_json + "\n"
        )  # append a newline character to separate records
