import xgboost as xgb
from modeling_config import load_data, prepare_data
from sklearn.metrics import (
    accuracy_score,
    mean_absolute_error,
    precision_score,
    r2_score,
)
from sklearn.model_selection import GridSearchCV


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

    # Classification
    (X_train, X_test, y_train, y_test), scalar = prepare_data(
        df,
        "CLS",
        feature_types=["main"],
        random_state=17,
        test_size=0.2,
        scale_data=False,
        time_based_split=False,
        print_shape=True,
    )

    nba_xgboost_cls(
        X_train,
        y_train,
        X_test,
        y_test,
        save_model=False,
        filename_end=None,
        show_importances=True,
        use_gridsearch=False,
    )

    # Regression
    (X_train, X_test, y_train, y_test), scalar = prepare_data(
        df,
        "REG",
        feature_types=["main"],
        random_state=17,
        test_size=0.2,
        scale_data=False,
        time_based_split=False,
        print_shape=True,
    )

    nba_xgboost_reg(
        X_train,
        y_train,
        X_test,
        y_test,
        save_model=False,
        filename_end=None,
        show_importances=True,
        use_gridsearch=False,
    )
