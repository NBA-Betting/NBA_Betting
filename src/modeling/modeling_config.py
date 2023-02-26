import datetime
import sys

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
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


def prepare_data(
    df,
    model_type,
    feature_types=["main"],
    random_state=17,
    test_size=0.2,
    scale_data=True,
    time_based_split=False,
    print_shape=False,
):
    """
    Preprocesses the given data frame for use in a machine learning model.

    Args:
        df (pandas DataFrame): The data to preprocess.
        model_type (str): The type of model to train, either "CLS" for classification or "REG" for regression.
        feature_types (list of str, optional): The types of features to include in the training data. Can be any combination of "main", "rank", "zscore", "other", or "all". Defaults to ["main"].
        random_state (int, optional): The random state to use for splitting the data into train and test sets. Defaults to 17.
        test_size (float, optional): The proportion of the data to use for the test set when splitting. Defaults to 0.2.
        scale_data (bool, optional): Whether to scale the features using the StandardScaler. Defaults to True.
        time_based_split (bool, optional): Whether to split the data into train and test sets based on a time-based split (i.e. training data comes before a certain date and test data comes after that date). Defaults to False.
        print_shape (bool, optional): Whether to print the shapes of the resulting train and test sets. Defaults to False.

    Returns:
        tuple of numpy ndarrays: The resulting train and test sets, in the order (X_train, X_test, y_train, y_test). If `scale_data` is True, the scaler used for feature scaling is also returned as a second item in the tuple.
    """

    # Restrict Dates to Regular Season minus First and Last 2 Weeks
    df["game_date"] = pd.to_datetime(df["game_id"].str[:8], format="%Y%m%d")
    mask = pd.Series(False, index=df.index)
    for season, (start_date, end_date) in REGULAR_SEASON_DATES_ADJUSTED.items():
        season_mask = (df["game_date"] >= pd.to_datetime(start_date)) & (
            df["game_date"] <= pd.to_datetime(end_date)
        )
        mask |= season_mask
    df = df[mask]

    # Determine the target column
    target = None
    if model_type == "CLS":
        target = "CLS_TARGET_home_margin_GT_home_spread"
    elif model_type == "REG":
        target = "REG_TARGET_actual_home_margin"
    else:
        raise ValueError("model_type must be either 'CLS' or 'REG'")

    # Determine Features to Use
    drop_columns = [
        "fd_line_home",
        "dk_line_home",
        "cover_consensus_home",
        "game_id",
        "game_date",
        "REG_TARGET_actual_home_margin",
        "CLS_TARGET_home_margin_GT_home_spread",
    ]

    # FEATURE OPTIONS (main, rank, zscore, other, all)
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
    zscore_features = [feature for feature in list(df) if "zscore" in feature]
    other_features = [
        feature
        for feature in list(df)
        if feature not in [target] + main_features + drop_columns
    ]
    all_features = main_features + other_features

    features_to_use = []
    if "all" in feature_types:
        features_to_use = all_features
    elif "main" in feature_types:
        features_to_use = main_features
        if "other" in feature_types and (
            "rank" in feature_types or "zscore" in feature_types
        ):
            raise ValueError("other_features can only be added to main features.")
        elif "other" in feature_types:
            features_to_use += other_features
        elif "other" not in feature_types:
            if "rank" in feature_types:
                features_to_use += rank_features
            if "zscore" in feature_types:
                features_to_use += zscore_features
    else:
        raise ValueError(
            "Feature_types must be one of 'all', 'main', 'rank', 'zscore', 'other'."
        )
    df = df[[target] + features_to_use]

    # Convert Data Types
    float_cols = [
        col
        for col in df.columns
        if col
        not in [
            "game_id",
            "CLS_TARGET_home_margin_GT_home_spread",
            "REG_TARGET_actual_home_margin",
        ]
    ]
    df[float_cols] = df[float_cols].astype("float32")

    # Drop NA Values
    df = df.dropna()

    # Split Data
    X_train, X_test, y_train, y_test = None, None, None, None
    if time_based_split:
        test_data = df[df["league_year_end"] == 22]
        train_data = df[df["league_year_end"] < 22]
        X_train = train_data.drop(target, axis=1)
        X_test = test_data.drop(target, axis=1)
        y_train = train_data[target]
        y_test = test_data[target]
    else:
        X_train, X_test, y_train, y_test = train_test_split(
            df.drop(target, axis=1),
            df[target],
            test_size=test_size,
            random_state=random_state,
        )

    # Feature Scaling
    scaler = None
    if scale_data:
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)

    if print_shape:
        print(f"X_train shape: {X_train.shape}")
        print(f"X_test shape: {X_test.shape}")
        print(f"y_train shape: {y_train.shape}")
        print(f"y_test shape: {y_test.shape}")
    return (X_train, X_test, y_train, y_test), scaler


REGULAR_SEASON_DATES_ADJUSTED = {
    "2013-2014": (datetime.date(2013, 11, 12), datetime.date(2014, 4, 2)),
    "2014-2015": (datetime.date(2014, 11, 11), datetime.date(2015, 4, 1)),
    "2015-2016": (datetime.date(2015, 11, 10), datetime.date(2016, 3, 30)),
    "2016-2017": (datetime.date(2016, 11, 8), datetime.date(2017, 3, 29)),
    "2017-2018": (datetime.date(2017, 10, 31), datetime.date(2018, 3, 28)),
    "2018-2019": (datetime.date(2018, 10, 30), datetime.date(2019, 3, 27)),
    "2019-2020": (datetime.date(2019, 11, 5), datetime.date(2020, 2, 26)),
    "2020-2021": (datetime.date(2021, 1, 5), datetime.date(2021, 5, 2)),
    "2021-2022": (datetime.date(2021, 11, 2), datetime.date(2022, 3, 27)),
    "2022-2023": (datetime.date(2022, 11, 1), datetime.date(2023, 3, 26)),
}

REGULAR_SEASON_DATES = {
    "2013-2014": (datetime.date(2013, 10, 29), datetime.date(2014, 4, 16)),
    "2014-2015": (datetime.date(2014, 10, 28), datetime.date(2015, 4, 15)),
    "2015-2016": (datetime.date(2015, 10, 27), datetime.date(2016, 4, 13)),
    "2016-2017": (datetime.date(2016, 10, 25), datetime.date(2017, 4, 12)),
    "2017-2018": (datetime.date(2017, 10, 17), datetime.date(2018, 4, 11)),
    "2018-2019": (datetime.date(2018, 10, 16), datetime.date(2019, 4, 10)),
    "2019-2020": (datetime.date(2019, 10, 22), datetime.date(2020, 3, 11)),
    "2020-2021": (datetime.date(2020, 12, 22), datetime.date(2021, 5, 16)),
    "2021-2022": (datetime.date(2021, 10, 19), datetime.date(2022, 4, 10)),
    "2022-2023": (datetime.date(2022, 10, 18), datetime.date(2023, 4, 9)),
}
PLAYOFF_DATES = {
    "2013-2014": (datetime.date(2014, 4, 19), datetime.date(2014, 6, 15)),
    "2014-2015": (datetime.date(2015, 4, 18), datetime.date(2015, 6, 16)),
    "2015-2016": (datetime.date(2016, 4, 16), datetime.date(2016, 6, 19)),
    "2016-2017": (datetime.date(2017, 4, 15), datetime.date(2017, 6, 12)),
    "2017-2018": (datetime.date(2018, 4, 14), datetime.date(2018, 6, 8)),
    "2018-2019": (datetime.date(2019, 4, 13), datetime.date(2019, 6, 13)),
    "2019-2020": (datetime.date(2020, 8, 15), datetime.date(2020, 10, 11)),
    "2020-2021": (datetime.date(2021, 5, 18), datetime.date(2021, 7, 22)),
    "2021-2022": (datetime.date(2022, 4, 12), datetime.date(2022, 6, 19)),
    "2022-2023": (datetime.date(2023, 4, 11), datetime.date(2023, 6, 18)),
}

OFFSEASON_DATES = {
    "2013-2014": (datetime.date(2014, 6, 15), datetime.date(2014, 10, 27)),
    "2014-2015": (datetime.date(2015, 6, 16), datetime.date(2015, 10, 26)),
    "2015-2016": (datetime.date(2016, 6, 19), datetime.date(2016, 10, 24)),
    "2016-2017": (datetime.date(2017, 6, 12), datetime.date(2017, 10, 16)),
    "2017-2018": (datetime.date(2018, 6, 8), datetime.date(2018, 10, 15)),
    "2018-2019": (datetime.date(2019, 6, 13), datetime.date(2019, 10, 21)),
    "2019-2020": (datetime.date(2020, 10, 11), datetime.date(2020, 12, 21)),
    "2020-2021": (datetime.date(2021, 7, 20), datetime.date(2021, 10, 18)),
    "2021-2022": (datetime.date(2022, 6, 19), datetime.date(2022, 10, 24)),
    "2022-2023": (datetime.date(2023, 6, 18), datetime.date(2023, 10, 23)),
}

BUBBLE_DATES = (datetime.date(2020, 7, 30), datetime.date(2020, 8, 14))

FIRST_TWO_WEEKS_DATES = {
    "2013-2014": (datetime.date(2013, 10, 29), datetime.date(2013, 11, 11)),
    "2014-2015": (datetime.date(2014, 10, 28), datetime.date(2014, 11, 10)),
    "2015-2016": (datetime.date(2015, 10, 27), datetime.date(2015, 11, 9)),
    "2016-2017": (datetime.date(2016, 10, 25), datetime.date(2016, 11, 7)),
    "2017-2018": (datetime.date(2017, 10, 17), datetime.date(2017, 10, 30)),
    "2018-2019": (datetime.date(2018, 10, 16), datetime.date(2018, 10, 29)),
    "2019-2020": (datetime.date(2019, 10, 22), datetime.date(2019, 11, 4)),
    "2020-2021": (datetime.date(2020, 12, 22), datetime.date(2021, 1, 4)),
    "2021-2022": (datetime.date(2021, 10, 19), datetime.date(2021, 11, 1)),
    "2022-2023": (datetime.date(2022, 10, 18), datetime.date(2022, 10, 31)),
}

LAST_TWO_WEEKS_DATES = {
    "2013-2014": (datetime.date(2014, 4, 3), datetime.date(2014, 4, 16)),
    "2014-2015": (datetime.date(2015, 4, 2), datetime.date(2015, 4, 15)),
    "2015-2016": (datetime.date(2016, 3, 31), datetime.date(2016, 4, 13)),
    "2016-2017": (datetime.date(2017, 3, 30), datetime.date(2017, 4, 12)),
    "2017-2018": (datetime.date(2018, 3, 29), datetime.date(2018, 4, 11)),
    "2018-2019": (datetime.date(2019, 3, 28), datetime.date(2019, 4, 10)),
    "2019-2020": (datetime.date(2020, 2, 25), datetime.date(2020, 3, 11)),
    "2020-2021": (datetime.date(2021, 5, 3), datetime.date(2021, 5, 16)),
    "2021-2022": (datetime.date(2022, 3, 27), datetime.date(2022, 4, 10)),
    "2022-2023": (datetime.date(2023, 3, 26), datetime.date(2023, 4, 9)),
}
