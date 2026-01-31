"""
NBA Betting - AutoGluon Model Trainer

Provides a clean interface for training and evaluating NBA game prediction models
using AutoGluon's state-of-the-art AutoML capabilities.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd

from src.config import NBA_IMPORTANT_DATES
from src.database import get_engine
from src.utils.modeling_utils import ModelSetup, calculate_roi


class NBAPredictor:
    """
    AutoGluon-based predictor for NBA game outcomes.

    Supports both classification (beat the spread) and regression (predict score differential).

    Example usage:
        >>> predictor = NBAPredictor(problem_type="classification")
        >>> predictor.load_data(training_seasons=[2022, 2023], testing_seasons=[2024])
        >>> predictor.train(time_limit=300)  # 5 minutes
        >>> results = predictor.evaluate()
        >>> predictor.save("models/nba_cls_2024")
    """

    def __init__(
        self,
        problem_type: Literal["classification", "regression"] = "classification",
        season_type: Literal["reg", "post", "both"] = "reg",
        use_vegas_line: bool = False,
    ):
        """
        Initialize the NBA Predictor.

        Args:
            problem_type: "classification" (beat the spread) or "regression" (predict margin)
            season_type: "reg" (regular season), "post" (playoffs), or "both"
            use_vegas_line: Whether to include Vegas line as a feature
        """
        self.problem_type = problem_type
        self.season_type = season_type
        self.use_vegas_line = use_vegas_line
        self.predictor = None
        self.train_data = None
        self.test_data = None
        self.features = None
        self.target = "CLS_TARGET" if problem_type == "classification" else "REG_TARGET"
        self.report = {}

    def load_data(
        self,
        training_seasons: list[int],
        testing_seasons: list[int],
        features: list[str] | None = None,
        start_date: str = "2020-09-01",
    ) -> "NBAPredictor":
        """
        Load and prepare training/testing data from the database.

        Args:
            training_seasons: List of seasons for training (e.g., [2022, 2023])
            testing_seasons: List of seasons for testing (e.g., [2024])
            features: Optional list of feature columns. If None, uses default feature set.
            start_date: Earliest date to include in data loading

        Returns:
            self for method chaining
        """
        engine = get_engine()

        # Load games
        games_query = f"""
            SELECT game_id, game_datetime, home_team, away_team,
                   open_line, home_score, away_score
            FROM games
            WHERE game_datetime >= '{start_date}'
        """
        games = pd.read_sql_query(games_query, engine)
        games["game_datetime"] = pd.to_datetime(games["game_datetime"], format="mixed")

        # Load features
        start_date_int = int(start_date.replace("-", ""))
        features_query = f"""
            SELECT game_id, data
            FROM all_features_json
            WHERE CAST(SUBSTR(game_id, 1, 8) AS INTEGER) >= {start_date_int}
        """
        all_features = pd.read_sql_query(features_query, engine)

        # Parse JSON data column
        all_features["data"] = all_features["data"].apply(self._parse_json)
        expanded_data = pd.json_normalize(all_features["data"])
        all_features = pd.concat([all_features.drop(columns=["data"]), expanded_data], axis=1)

        # Merge games and features
        # Features table may have overlapping columns - keep games columns as primary
        df = pd.merge(games, all_features, on="game_id", how="inner", suffixes=("", "_feat"))

        # Drop duplicate columns from features (keep games version)
        drop_cols = [c for c in df.columns if c.endswith("_feat")]
        df = df.drop(columns=drop_cols)

        # Filter to completed games with lines
        df = df[df["home_score"].notna() & df["open_line"].notna()]

        # Add targets
        df = ModelSetup.add_targets(df)

        # Get date ranges for train/test split
        training_dates, testing_dates = ModelSetup.choose_dates(
            training_seasons, testing_seasons, self.season_type
        )

        # Use default features if none provided
        if features is None:
            features = self._get_default_features(df)
        self.features = features

        # Create train/test datasets
        self.train_data, self.test_data, self.report = ModelSetup.create_datasets(
            df,
            "cls" if self.problem_type == "classification" else "reg",
            features,
            training_dates,
            testing_dates,
            create_report=True,
            use_vegas_line=self.use_vegas_line,
        )

        # Track whether vegas line is included
        self.report["use_vegas_line"] = self.use_vegas_line

        # Drop game_id for training (keep for reference)
        self._train_game_ids = self.train_data["game_id"].copy()
        self._test_game_ids = self.test_data["game_id"].copy()

        print(f"Training data: {len(self.train_data)} games")
        print(f"Testing data: {len(self.test_data)} games")
        print(f"Features: {len(features)}" + (" (+ vegas_open_hv)" if self.use_vegas_line else ""))

        return self

    def load_data_by_dates(
        self,
        training_start: str,
        training_end: str,
        testing_start: str,
        testing_end: str,
        features: list[str] | None = None,
    ) -> "NBAPredictor":
        """
        Load and prepare training/testing data using explicit date ranges.

        Args:
            training_start: Training period start date (YYYY-MM-DD)
            training_end: Training period end date (YYYY-MM-DD)
            testing_start: Testing period start date (YYYY-MM-DD)
            testing_end: Testing period end date (YYYY-MM-DD)
            features: Optional list of feature columns. If None, uses default feature set.

        Returns:
            self for method chaining
        """
        engine = get_engine()

        # Use earliest date as start for data loading
        start_date = min(training_start, testing_start)

        # Load games
        games_query = f"""
            SELECT game_id, game_datetime, home_team, away_team,
                   open_line, home_score, away_score
            FROM games
            WHERE game_datetime >= '{start_date}'
        """
        games = pd.read_sql_query(games_query, engine)
        games["game_datetime"] = pd.to_datetime(games["game_datetime"], format="mixed")

        # Load features
        start_date_int = int(start_date.replace("-", ""))
        features_query = f"""
            SELECT game_id, data
            FROM all_features_json
            WHERE CAST(SUBSTR(game_id, 1, 8) AS INTEGER) >= {start_date_int}
        """
        all_features = pd.read_sql_query(features_query, engine)

        # Parse JSON data column
        all_features["data"] = all_features["data"].apply(self._parse_json)
        expanded_data = pd.json_normalize(all_features["data"])
        all_features = pd.concat([all_features.drop(columns=["data"]), expanded_data], axis=1)

        # Merge games and features
        df = pd.merge(games, all_features, on="game_id", how="inner", suffixes=("", "_feat"))

        # Drop duplicate columns from features (keep games version)
        drop_cols = [c for c in df.columns if c.endswith("_feat")]
        df = df.drop(columns=drop_cols)

        # Filter to completed games with lines
        df = df[df["home_score"].notna() & df["open_line"].notna()]

        # Add targets
        df = ModelSetup.add_targets(df)

        # Use default features if none provided
        if features is None:
            features = self._get_default_features(df)
        self.features = features

        # Create train/test datasets using explicit date ranges
        training_dates = (training_start, training_end)
        testing_dates = (testing_start, testing_end)

        self.train_data, self.test_data, self.report = ModelSetup.create_datasets(
            df,
            "cls" if self.problem_type == "classification" else "reg",
            features,
            training_dates,
            testing_dates,
            create_report=True,
            use_vegas_line=self.use_vegas_line,
        )

        # Track whether vegas line is included
        self.report["use_vegas_line"] = self.use_vegas_line

        # Drop game_id for training (keep for reference)
        self._train_game_ids = self.train_data["game_id"].copy()
        self._test_game_ids = self.test_data["game_id"].copy()

        print(f"Training data: {len(self.train_data)} games ({training_start} to {training_end})")
        print(f"Testing data: {len(self.test_data)} games ({testing_start} to {testing_end})")
        print(f"Features: {len(features)}" + (" (+ vegas_open_hv)" if self.use_vegas_line else ""))

        return self

    def train(
        self,
        time_limit: int = 300,
        presets: str = "medium_quality",
        verbosity: int = 2,
        excluded_model_types: list[str] | None = None,
    ) -> "NBAPredictor":
        """
        Train the AutoGluon model.

        Args:
            time_limit: Training time limit in seconds (default 5 minutes)
            presets: AutoGluon presets - "best_quality", "high_quality", "medium_quality", "fast"
            verbosity: 0 (silent) to 4 (debug)
            excluded_model_types: Model types to exclude (e.g., ["RF", "XT"] to skip Random Forest)

        Returns:
            self for method chaining
        """
        try:
            from autogluon.tabular import TabularPredictor
        except ImportError:
            raise ImportError(
                "AutoGluon is not installed. Install with: pip install 'nba-betting[ml]'"
            )

        if self.train_data is None:
            raise ValueError("No training data loaded. Call load_data() first.")

        # Prepare training data (drop game_id)
        train_df = self.train_data.drop(columns=["game_id"])

        # Configure problem type
        if self.problem_type == "classification":
            problem_type = "binary"
            eval_metric = "accuracy"
        else:
            problem_type = "regression"
            eval_metric = "mean_absolute_error"

        print(f"Training {self.problem_type} model with {presets} presets...")
        print(f"Time limit: {time_limit} seconds")
        if excluded_model_types:
            print(f"Excluding model types: {excluded_model_types}")

        # Create a temp directory for training (will be moved on save)
        import tempfile

        self._temp_model_dir = tempfile.mkdtemp(prefix="autogluon_")

        self.predictor = TabularPredictor(
            label=self.target,
            problem_type=problem_type,
            eval_metric=eval_metric,
            verbosity=verbosity,
            path=self._temp_model_dir,
        )

        self.predictor.fit(
            train_data=train_df,
            time_limit=time_limit,
            presets=presets,
            excluded_model_types=excluded_model_types,
        )

        # Clean up training data from model to reduce storage (saves ~90% space)
        self.predictor.save_space()

        # Store training info in report
        self.report["training_time_limit"] = time_limit
        self.report["presets"] = presets
        self.report["trained_at"] = datetime.now().isoformat()

        return self

    def evaluate(self, detailed: bool = True) -> dict:
        """
        Evaluate the model on test data.

        Args:
            detailed: If True, include ROI calculations for betting analysis

        Returns:
            Dictionary with evaluation metrics
        """
        if self.predictor is None:
            raise ValueError("No model trained. Call train() first.")

        # Prepare test data
        test_df = self.test_data.drop(columns=["game_id"])

        # Get predictions
        predictions = self.predictor.predict(test_df)

        # Get prediction probabilities for classification
        if self.problem_type == "classification":
            pred_proba = self.predictor.predict_proba(test_df)
            # Get probability of positive class (True = bet on home)
            if True in pred_proba.columns:
                pred_scores = pred_proba[True]
            else:
                pred_scores = pred_proba.iloc[:, 1]

        # Calculate metrics
        results = {}

        if self.problem_type == "classification":
            from sklearn.metrics import accuracy_score, precision_score, recall_score

            y_true = test_df[self.target]
            y_pred = predictions

            results["accuracy"] = accuracy_score(y_true, y_pred)
            results["precision"] = precision_score(y_true, y_pred, zero_division=0)
            results["recall"] = recall_score(y_true, y_pred, zero_division=0)

            # Baselines
            results["baseline_home"] = self.report.get("ind_baseline_test", 0.5)
            results["baseline_majority"] = self.report.get("dep_baseline_test", 0.5)

            print(f"\n{'='*50}")
            print("CLASSIFICATION RESULTS")
            print(f"{'='*50}")
            print(f"Accuracy:          {results['accuracy']:.4f}")
            print(f"Baseline (home):   {results['baseline_home']:.4f}")
            print(f"Baseline (majority): {results['baseline_majority']:.4f}")
            print(f"Lift over random:  {(results['accuracy'] - 0.5) * 100:.2f}%")

            # ROI analysis
            if detailed:
                test_with_preds = self.test_data.copy()
                test_with_preds["prediction_label"] = predictions.values
                test_with_preds["prediction_score"] = pred_scores.values

                roi_df = calculate_roi(
                    test_with_preds,
                    self.target,
                    "prediction_label",
                    pred_prob="prediction_score",
                )

                results["roi_analysis"] = roi_df.to_dict("records")

                print(f"\n{'='*50}")
                print("ROI ANALYSIS")
                print(f"{'='*50}")
                for _, row in roi_df.iterrows():
                    print(f"{row['Label']}: ${row['Average ROI per Bet']:.2f} per bet")

        else:  # regression
            from sklearn.metrics import mean_absolute_error, root_mean_squared_error

            y_true = test_df[self.target]
            y_pred = predictions

            results["mae"] = mean_absolute_error(y_true, y_pred)
            results["rmse"] = root_mean_squared_error(y_true, y_pred)
            results["baseline_vegas_mae"] = self.report.get("ind_baseline_test", 0)

            print(f"\n{'='*50}")
            print("REGRESSION RESULTS")
            print(f"{'='*50}")
            print(f"MAE:               {results['mae']:.4f}")
            print(f"RMSE:              {results['rmse']:.4f}")
            print(f"Vegas MAE:         {results['baseline_vegas_mae']:.4f}")

        # Get model leaderboard
        leaderboard = self.predictor.leaderboard(test_df, silent=True)
        results["leaderboard"] = leaderboard.to_dict("records")

        print(f"\n{'='*50}")
        print("MODEL LEADERBOARD (Top 5)")
        print(f"{'='*50}")
        print(leaderboard.head().to_string())

        # Update report
        self.report["evaluation"] = results

        return results

    def predict(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Make predictions on new data.

        Args:
            data: DataFrame with same features as training data

        Returns:
            DataFrame with predictions
        """
        if self.predictor is None:
            raise ValueError("No model trained. Call train() first.")

        predictions = self.predictor.predict(data)

        if self.problem_type == "classification":
            pred_proba = self.predictor.predict_proba(data)
            if True in pred_proba.columns:
                confidence = pred_proba[True]
            else:
                confidence = pred_proba.iloc[:, 1]

            return pd.DataFrame(
                {
                    "prediction": predictions,
                    "confidence": confidence,
                    "recommendation": predictions.map({True: "Bet Home", False: "Bet Away"}),
                }
            )
        else:
            return pd.DataFrame(
                {
                    "predicted_margin": predictions,
                }
            )

    def save(self, path: str) -> None:
        """Save the model and report to disk."""
        import shutil

        if self.predictor is None:
            raise ValueError("No model to save. Call train() first.")

        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        # Copy AutoGluon model from temp directory
        model_dest = path / "model"
        if hasattr(self, "_temp_model_dir") and self._temp_model_dir:
            # Copy the trained model from temp location
            if model_dest.exists():
                shutil.rmtree(model_dest)
            shutil.copytree(self._temp_model_dir, model_dest)
        else:
            # Fallback: save directly (may not work for all cases)
            self.predictor.save(str(model_dest))

        # Save report
        report_path = path / "report.json"
        with open(report_path, "w") as f:
            json.dump(self.report, f, indent=2, default=str)

        # Save feature list
        features_path = path / "features.json"
        with open(features_path, "w") as f:
            json.dump(self.features, f, indent=2)

        print(f"Model saved to {path}")

    @classmethod
    def load(cls, path: str) -> "NBAPredictor":
        """Load a saved model from disk."""
        try:
            from autogluon.tabular import TabularPredictor
        except ImportError:
            raise ImportError(
                "AutoGluon is not installed. Install with: pip install 'nba-betting[ml]'"
            )

        path = Path(path)

        # Load report to get problem type
        with open(path / "report.json") as f:
            report = json.load(f)

        problem_type = report.get("problem_type", "classification")
        # Normalize problem_type - handle both short ("cls", "reg") and long forms
        if problem_type in ("cls", "classification"):
            problem_type = "classification"
        else:
            problem_type = "regression"

        # Get vegas line setting from report
        use_vegas_line = report.get("use_vegas_line", False)

        # Create instance
        instance = cls(problem_type=problem_type, use_vegas_line=use_vegas_line)
        instance.report = report

        # Load features
        with open(path / "features.json") as f:
            instance.features = json.load(f)

        # Load model
        instance.predictor = TabularPredictor.load(str(path / "model"))

        return instance

    def feature_importance(self, top_n: int = 20) -> pd.DataFrame:
        """Get feature importance from the model."""
        if self.predictor is None:
            raise ValueError("No model trained. Call train() first.")

        importance = self.predictor.feature_importance(self.test_data.drop(columns=["game_id"]))
        return importance.head(top_n)

    def _parse_json(self, data_str):
        """Parse JSON or Python dict string."""
        import ast

        if data_str is None:
            return {}
        try:
            return json.loads(data_str)
        except json.JSONDecodeError:
            try:
                return ast.literal_eval(data_str)
            except (ValueError, SyntaxError):
                return {}

    def _get_default_features(self, df: pd.DataFrame) -> list[str]:
        """Get default feature set based on available columns."""
        # Exclude non-feature columns
        exclude = {
            "game_id",
            "game_datetime",
            "home_team",
            "away_team",
            "open_line",
            "home_score",
            "away_score",
            "game_completed",
            "odds_last_update",
            "scores_last_update",
            "CLS_TARGET",
            "REG_TARGET",
            "season",
            "season_type",
        }

        # Get numeric columns that aren't excluded
        features = [
            col
            for col in df.columns
            if col not in exclude and df[col].dtype in ["float64", "int64", "float32", "int32"]
        ]

        return features
