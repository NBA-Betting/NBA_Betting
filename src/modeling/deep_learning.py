import datetime
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from tensorflow import keras
from tensorflow.keras.layers import Normalization, Dense


def nba_betting_rds_connection(endpoint, password):
    """
    Args:
        endpoint (str): AWS RDS Database Endpoint
        password (str)

    Returns:
        Database Connection Object
    """
    username = "postgres"
    database = "nba_betting"
    connection = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    ).connect()
    return connection


def deep_learning_data_prep(df, target):
    """Final data prep for nba_betting "model_ready" table data.

    Args:
        df (Pandas Dataframe): From "model_ready" data table.
        target (str): Name of target column. Regression or Classification.

    Returns:
        Pandas Dataframe
    """
    print(f"Inbound Data Shape: {df.shape}")
    print(df.info())
    keep_features = [
        'home_team_num', 'away_team_num', 'league_year_end', 'home_spread'
    ]
    features_to_use = [
        feature for feature in list(df) if feature[-3:] == 'vla'
    ] + keep_features + [target]

    df = df[features_to_use]
    df = df.dropna()
    df = df.astype("float32")
    print(f"Outbound Data Shape: {df.shape}")
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
    X = df.drop(columns=target)
    y = df.pop(target)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state)
    print(f"X_train Shape: {X_train.shape}")
    print(f"X_test Shape: {X_test.shape}")
    print(f"y_train Shape: {y_train.shape}")
    print(f"y_test Shape: {y_test.shape}")
    return X_train, X_test, y_train, y_test


if __name__ == "__main__":
    cls_target = 'CLS_TARGET_home_margin_GT_home_spread'
    reg_target = 'REG_TARGET_actual_home_margin'

    nba_betting_password = ""
    nba_betting_endpoint = ""

    # Load Data
    database_connection = nba_betting_rds_connection(nba_betting_endpoint,
                                                     nba_betting_password)
    data = pd.read_sql_table("nba_model_ready", database_connection)

    # Prep Data
    data = deep_learning_data_prep(data, reg_target)

    # Split Data
    X_train, X_test, y_train, y_test = split_data(data, reg_target)

    # Tensorboard Setup
    tensorboard_callback = keras.callbacks.TensorBoard(
        log_dir=
        "/home/jeff/Documents/Data_Science_Projects/NBA_Betting/models/Deep_Learning/tensorboard_logs/"
        + datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S"),
        embeddings_freq=10,
        histogram_freq=1,
    )

    # Normalizer
    normalizer = Normalization(axis=-1)
    normalizer.adapt(X_train)

    # Model Creation - Regression
    # inputs = keras.Input(shape=(155, ))
    # norm = normalizer(inputs)
    # x = Dense(128, activation="relu")(norm)
    # x = Dense(64, activation="relu")(x)
    # outputs = Dense(1, activation="linear")(x)

    # model = keras.Model(inputs=inputs, outputs=outputs)
    # model.compile(optimizer="adam", loss="mse", metrics=["mae"])
    # model.summary()

    # Classification
    inputs = keras.Input(shape=(155, ))
    norm = normalizer(inputs)
    x = Dense(128, activation="relu")(norm)
    x = Dense(64, activation="relu")(x)
    outputs = Dense(1, activation="sigmoid")(x)

    model = keras.Model(inputs=inputs, outputs=outputs)
    model.compile(optimizer="adam",
                  loss="binary_crossentropy",
                  metrics=["accuracy"])
    model.summary()

    # Model Training
    history = model.fit(
        X_train,
        y_train,
        epochs=200,
        callbacks=[tensorboard_callback],
        validation_split=0.2,
    )

    # Model Testing
    print("\nTest Data")
    results = model.evaluate(X_test, y_test, callbacks=[tensorboard_callback])

    # Save Model
    model_save_location = "/home/jeff/Documents/Data_Science_Projects/NBA_Betting/models/Deep_Learning/"
    model_name = "NBA_Stats_CLS_Baseline"
    model_creation = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    model.save(f"{model_save_location}{model_name}_{model_creation}")
