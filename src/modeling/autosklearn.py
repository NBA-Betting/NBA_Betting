import pickle
import sys

import pandas as pd
from autosklearn import classification as autosklearn_cls
from autosklearn import regression as autosklearn_reg
from nba_xgboost import load_data, prepare_data, split_data
from sklearn.metrics import (
    accuracy_score,
    mean_absolute_error,
    precision_score,
    r2_score,
)

sys.path.append("../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD


import pickle
from sklearn.metrics import accuracy_score, precision_score
from autosklearn import classification as autosklearn_cls


def autosklearn_cls(X_train, X_test, y_train, y_test, print_info=False, save_model=False, filepath_postfix=None):
    """
    This function trains an AutoSklearnClassifier model on the input training data, makes predictions on the test data,
    and returns the accuracy and precision scores of the model. Optionally, it can also print model training information,
    and save the model for future use.

    Parameters:
    - X_train (pandas DataFrame): Training data features
    - X_test (pandas DataFrame): Test data features
    - y_train (pandas Series): Training data target
    - y_test (pandas Series): Test data target
    - print_info (bool): Whether to print the model training information and configuration (default False)
    - save_model (bool): Whether to save the model (default False)
    - filepath_postfix (str): If saving the model, a postfix to append to the file name (default None)

    Returns:
    None
    """
    automl = autosklearn_cls.AutoSklearnClassifier()
    automl.fit(X_train, y_train)

    y_pred = automl.predict(X_test)

    if print_info:
        print("Model training information:")
        print(automl.sprint_statistics())

        print("Model configuration:")
        print(automl.show_models())

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, average="weighted")

    print("Accuracy: ", accuracy)
    print("Precision: ", precision)

    if save_model:
        if filepath_postfix is None:
            raise ValueError("filepath_postfix must be provided if saving the model")
        with open("../../models/AutoSkLearn_REG_" + filepath_postfix, "wb") as file:
            pickle.dump(automl, file)


def autosklearn_reg(X_train, X_test, y_train, y_test, print_info=False, save_model=False, filepath_postfix=None):
    """
    This function trains an AutoSklearnRegressor model on the input training data, makes predictions on the test data,
    and returns the MAE and R2 Score of the model. Optionally, it can also print model training information,
    and save the model for future use.

    Parameters:
    - X_train (pandas DataFrame): Training data features
    - X_test (pandas DataFrame): Test data features
    - y_train (pandas Series): Training data target
    - y_test (pandas Series): Test data target
    - print_info (bool): Whether to print the model training information and configuration (default False)
    - save_model (bool): Whether to save the model (default False)
    - filepath_postfix (str): If saving the model, a postfix to append to the file name (default None)

    Returns:
    None
    """
    automl = autosklearn_cls.AutoSklearnRegressor()
    automl.fit(X_train, y_train)

    y_pred = automl.predict(X_test)

    if print_info:
        print("Model training information:")
        print(automl.sprint_statistics())

        print("Model configuration:")
        print(automl.show_models())

    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print("MAE: ", mae)
    print("R2 Score: ", r2)

    if save_model:
        if filepath_postfix is None:
            raise ValueError("filepath_postfix must be provided if saving the model")
        with open("../../models/AutoSkLearn_REG_" + filepath_postfix, "wb") as file:
            pickle.dump(automl, file)


if __name__ == "__main__":
    df = load_data()
    df, target = prepare_data(df, "CLS")
    X_train, X_test, y_train, y_test = split_data(df, target)

    autosklearn_cls(
        X_train,
        X_test,
        y_train,
        y_test,
        print_info=True
        save_model=False,
        model_filepath=None,
    )
