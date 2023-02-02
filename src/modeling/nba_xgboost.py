import sys

import matplotlib.pyplot as plt
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    accuracy_score,
    mean_absolute_error,
    precision_score,
    r2_score,
)
from sklearn.model_selection import GridSearchCV, train_test_split
from sqlalchemy import create_engine

sys.path.append("../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD


def load_data(print_data_summary=False):
    """Load data from the database and return a Pandas dataframe.

    Args:
    - print_data_summary: bool, whether to print summary of the loaded data. Default is False.

    Returns:
    - df: Pandas dataframe, containing the loaded data from the database.
    """
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
        print("Data Information:")
        print(df.info(verbose=True, show_counts=True))
        print("\nData Description:")
        print(df.describe())
        print("\nHead of the Data:")
        print(df.sort_values("game_id", ascending=False).head())

    return df


def split_data(df, target, test_size=0.3, random_state=0, print_shape=False):
    """Split the data into training and testing sets.

    Args:
    - df: Pandas DataFrame, the data to be split.
    - target: str, the name of the target column.
    - test_size: float, optional, the proportion of the data to be used for testing. Default is 0.3.
    - random_state: int, optional, the random seed for reproducibility. Default is 0.
    - print_shape: bool, optional, whether to print the shape of the output data. Default is False.

    Returns:
    - X_train: Pandas DataFrame, the training set features.
    - X_test: Pandas DataFrame, the testing set features.
    - y_train: Pandas Series, the training set targets.
    - y_test: Pandas Series, the testing set targets.
    """
    y = df[target]
    X = df.drop(columns=target)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    if print_shape:
        print(f"X_train Shape: {X_train.shape}")
        print(f"X_test Shape: {X_test.shape}")
        print(f"y_train Shape: {y_train.shape}")
        print(f"y_test Shape: {y_test.shape}")
    return X_train, X_test, y_train, y_test


def prepare_data(df, model_type, feature_types=["main"]):
    """
    This function prepares the data for the desired model type
    by selecting the specified features and returning the resulting dataframe.

    Parameters:
    df (DataFrame): The input dataframe.
    model_type (str): The type of model to prepare the data for, either "CLS" or "REG".
    feature_types (list, optional): The types of features to include in the data preparation.
                                    Defaults to ["main"].

    Returns:
    DataFrame: The prepared dataframe with only the selected features.

    Raises:
    ValueError: If the model_type argument is not "CLS" or "REG".
    ValueError: If the feature_types argument contains "other" and either "rank" or "vla_std".
    ValueError: If the feature_types argument is not one of "all", "main", "rank", "vla_std", "other".
    """
    if model_type == "CLS":
        target_column = "CLS_TARGET_home_margin_GT_home_spread"
    elif model_type == "REG":
        target_column = "REG_TARGET_actual_home_margin"
    else:
        raise ValueError("model_type must be either 'CLS' or 'REG'")

    drop_columns = [
        "fd_line_home",
        "dk_line_home",
        "cover_consensus_home",
        "game_id",
        "REG_TARGET_actual_home_margin",
        "CLS_TARGET_home_margin_GT_home_spread",
    ]

    # FEATURE OPTIONS (main, rank, vla_std, other, all)
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

    vla_std_features = [feature for feature in list(df) if "vla_std" in feature]

    other_features = [
        feature
        for feature in list(df)
        if feature not in [target_column] + main_features + drop_columns
    ]

    all_features = main_features + other_features

    features_to_use = []

    if "all" in feature_types:
        features_to_use = all_features
    elif "main" in feature_types:
        features_to_use = main_features
        if "other" in feature_types and (
            "rank" in feature_types or "vla_std" in feature_types
        ):
            raise ValueError("other_features can only be added to main features.")
        elif "other" in feature_types:
            features_to_use += other_features
        elif "other" not in feature_types:
            if "rank" in feature_types:
                features_to_use += rank_features
            if "vla_std" in feature_types:
                features_to_use += vla_std_features
    else:
        raise ValueError(
            "Feature_types must be one of 'all', 'main', 'rank', 'vla_std', 'other'."
        )

    model_ready_df = df[[target_column] + features_to_use].dropna()

    return model_ready_df, target_column


def nba_xgboost_cls(
    X_train,
    y_train,
    X_test,
    y_test,
    use_gridsearch=True,
    show_importances=False,
    save_model=False,
    filename_end=None,
    params={},
):
    """Train and evaluate an XGBoost classifier.

    Parameters
    ----------
    X_train : pd.DataFrame
        Dataframe of training features
    y_train : pd.Series
        Series of training labels
    X_test : pd.DataFrame
        Dataframe of testing features
    y_test : pd.Series
        Series of testing labels
    use_gridsearch : bool, optional
        Whether to use GridSearchCV to tune hyperparameters, by default True
    show_importances : bool, optional
        Whether to show feature importances, by default False
    save_model : bool, optional
        Whether to save the trained model, by default False
    filename_end : str, optional
        End of the filename to save the model as, by default None
    params : dict, optional
        Dictionary of hyperparameters to use if not using GridSearchCV, by default None

    Returns
    -------
    None
        The function prints out accuracy and precision of the trained classifier
        and saves the model if save_model is True."""

    if use_gridsearch:
        param_grid = {
            "max_depth": [3, 5, 7],
            "learning_rate": [0.1, 0.3, 0.5],
            "n_estimators": [5, 10, 50, 100, 300],
        }
        cls = xgb.XGBClassifier(eval_metric="error")
        grid_search_cls = GridSearchCV(cls, param_grid, cv=5, verbose=3)
        grid_search_cls.fit(X_train, y_train)
        y_pred = grid_search_cls.predict(X_test)
        best_estimator = grid_search_cls.best_estimator_
    else:
        cls = xgb.XGBClassifier(eval_metric="error", **params)
        cls.fit(
            X_train,
            y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=True,
        )
        y_pred = cls.predict(X_test)
        best_estimator = cls
    if show_importances:
        importances = best_estimator.get_booster().get_score(importance_type="weight")
        importances = {
            k: v
            for k, v in sorted(
                importances.items(), key=lambda item: item[1], reverse=False
            )
        }
        print("\nFeature Importances:")
        for feature, importance in importances.items():
            print(feature, importance)
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    print("\nAccuracy: %.3f" % accuracy)
    print("Precision: %.3f" % precision)
    if save_model:
        filename = "../../models/XGB_CLS_" + filename_end
        best_estimator.save_model(filename)


def nba_xgboost_reg(
    X_train,
    y_train,
    X_test,
    y_test,
    use_gridsearch=True,
    show_importances=False,
    save_model=False,
    filename_end=None,
    params={},
):
    """Train and evaluate an XGBoost regressor.

    Parameters
    ----------
    X_train : pd.DataFrame
        Dataframe of training features
    y_train : pd.Series
        Series of training labels
    X_test : pd.DataFrame
        Dataframe of testing features
    y_test : pd.Series
        Series of testing labels
    use_gridsearch : bool, optional
        Whether to use GridSearchCV to tune hyperparameters, by default True
    show_importances : bool, optional
        Whether to show feature importances, by default False
    save_model : bool, optional
        Whether to save the trained model, by default False
    filename_end : str, optional
        End of the filename to save the model as, by default None
    params : dict, optional
        Dictionary of hyperparameters to use if not using GridSearchCV, by default None

    Returns
    -------
    None
        The function prints MAE and R2 Score of the trained regressor
        and saves the model if save_model is True."""

    if use_gridsearch:
        param_grid = {
            "max_depth": [3, 5, 7],
            "learning_rate": [0.1, 0.3, 0.5],
            "n_estimators": [5, 10, 50, 100, 300],
        }
        reg = xgb.XGBRegressor(eval_metric="mae")
        grid_search_reg = GridSearchCV(reg, param_grid, cv=5, verbose=3)
        grid_search_reg.fit(X_train, y_train)
        y_pred = grid_search_reg.predict(X_test)
        best_estimator = grid_search_reg.best_estimator_
    else:
        reg = xgb.XGBRegressor(eval_metric="mae", **params)
        reg.fit(
            X_train,
            y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=True,
        )
        y_pred = reg.predict(X_test)
        best_estimator = reg
    if show_importances:
        importances = best_estimator.get_booster().get_score(importance_type="weight")
        importances = {
            k: v
            for k, v in sorted(
                importances.items(), key=lambda item: item[1], reverse=False
            )
        }
        print("\nFeature Importances:")
        for feature, importance in importances.items():
            print(feature, importance)
    mae = mean_absolute_error(y_test, y_pred)
    r2_score_ = r2_score(y_test, y_pred)
    print("\nMean Absolute Error: %.2f" % mae)
    print("R2_Score: %.2f" % r2_score_)
    if save_model:
        filename = "../../models/XGB_REG_" + filename_end
        best_estimator.save_model(filename)


if __name__ == "__main__":
    df = load_data()
    df, target = prepare_data(df, "REG", feature_types=["main"])
    X_train, X_test, y_train, y_test = split_data(df, target)
    nba_xgboost_reg(
        X_train,
        y_train,
        X_test,
        y_test,
        save_model=False,
        show_importances=True,
        use_gridsearch=False,
    )
