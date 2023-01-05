import sys
import pandas as pd
import xgboost as xgb
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    mean_absolute_error,
    r2_score,
)

sys.path.append("../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD


def load_data(print_data_summary=False):
    username = "postgres"
    password = RDS_PASSWORD
    endpoint = RDS_ENDPOINT
    database = "nba_betting"

    connection = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    ).connect()

    df = pd.read_sql_table("model_training_data", connection)
    df = df[df["league_year_end"] != 23]

    if print_data_summary:
        print(df.info(verbose=True, show_counts=True))
        print(df.describe())
        print(df.sort_values("game_id", ascending=False).head())

    return df


def split_data(df, target, test_size=0.3, random_state=0):
    """Deep Learning specific wrapper for normal sklearn train/test split.

    Args:
        df (Pandas Dataframe)
        target (str): Name of target column.
        test_size (float, optional): Defaults to 0.3.
        random_state (int, optional): Defaults to 0.

    Returns:
        Tuple of Dataframes: X_train, X_test, y_train, y_test
    """
    y = df[target]
    X = df.drop(columns=target)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    print(f"X_train Shape: {X_train.shape}")
    print(f"X_test Shape: {X_test.shape}")
    print(f"y_train Shape: {y_train.shape}")
    print(f"y_test Shape: {y_test.shape}")
    return X_train, X_test, y_train, y_test


if __name__ == "__main__":
    df = load_data()

    # IMPORTANT: Change this to "CLS" or "REG" to run the model for classification or regression
    cls_or_reg = "REG"  # "CLS" or "REG"

    # =============================================================================

    if cls_or_reg == "CLS":
        # FEATURE SELECTION
        target = ["CLS_TARGET_home_margin_GT_home_spread"]
        drop_features = [
            "fd_line_home",
            "dk_line_home",
            "covers_consensus_home",
            "game_id",
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
        rank_features = [feature for feature in list(df) if "rank" in feature]
        vla_std_features = [
            feature for feature in list(df) if "vla_std" in feature
        ]

        other_features = [
            feature
            for feature in list(df)
            if feature not in target + main_features + drop_features
        ]

        features_to_use = target + main_features

        model_ready_df = df[features_to_use]

        # TRAIN TEST SPLIT
        X_train, X_test, y_train, y_test = split_data(
            model_ready_df, target[0]
        )

        # MODEL TRAINING
        cls = xgb.XGBClassifier(eval_metric="error")

        cls.fit(
            X_train,
            y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=True,
        )

        # MODEL EVALUATION

        # Predict on the test set
        y_pred = cls.predict(X_test)

        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        print("\nAccuracy: %.3f" % accuracy)
        print("Precision: %.3f" % precision)

        # best_iteration = cls.best_iteration
        # best_score = cls.best_score
        # print("Best Iteration: %d" % best_iteration)
        # print("Best Score: %.2f" % best_score)

        # Get the feature importances
        importances = cls.get_booster().get_score(importance_type="weight")
        importances = {
            k: v
            for k, v in sorted(
                importances.items(), key=lambda item: item[1], reverse=True
            )
        }

        # Print the feature importances
        print("\nFeature Importances:")
        for feature, importance in importances.items():
            print(feature, importance)

        # evals_result = cls.evals_result()
        # print(evals_result)
        # xgb.plot_importance(cls)
        # xgb.plot_tree(cls, num_trees=2)
        # plt.show()

        # SAVE MODEL
        cls.save_model("../../models/XGB_CLS_main_features_baseline.model")

    # =============================================================================

    elif cls_or_reg == "REG":
        # FEATURE SELECTION
        target = ["REG_TARGET_actual_home_margin"]
        drop_features = [
            "fd_line_home",
            "dk_line_home",
            "covers_consensus_home",
            "game_id",
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
        rank_features = [feature for feature in list(df) if "rank" in feature]
        vla_std_features = [
            feature for feature in list(df) if "vla_std" in feature
        ]

        other_features = [
            feature
            for feature in list(df)
            if feature not in target + main_features + drop_features
        ]

        features_to_use = target + main_features

        model_ready_df = df[features_to_use]

        # TRAIN TEST SPLIT
        X_train, X_test, y_train, y_test = split_data(
            model_ready_df, target[0]
        )

        # MODEL TRAINING
        reg = xgb.XGBRegressor(eval_metric="mae")

        reg.fit(
            X_train,
            y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=True,
        )

        # MODEL EVALUATION

        # Predict on the test set
        y_pred = reg.predict(X_test)

        mae = mean_absolute_error(y_test, y_pred)
        r2_score_ = r2_score(y_test, y_pred)
        print("\nMean Absolute Error: %.2f" % mae)
        print("R2_Score: %.2f" % r2_score_)

        # best_iteration = reg.best_iteration
        # best_score = reg.best_score
        # print("Best Iteration: %d" % best_iteration)
        # print("Best Score: %.2f" % best_score)

        # Get the feature importances
        importances = reg.get_booster().get_score(importance_type="weight")
        importances = {
            k: v
            for k, v in sorted(
                importances.items(), key=lambda item: item[1], reverse=True
            )
        }

        # Print the feature importances
        print("\nFeature Importances:")
        for feature, importance in importances.items():
            print(feature, importance)

        # evals_result = reg.evals_result()
        # print(evals_result)
        # xgb.plot_importance(reg)
        # xgb.plot_tree(reg, num_trees=2)
        # plt.show()

        # SAVE MODEL
        reg.save_model("../../models/XGB_REG_main_features_baseline.model")
