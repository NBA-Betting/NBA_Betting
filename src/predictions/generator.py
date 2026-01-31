"""
NBA Betting - Prediction Generation

Generates game predictions using the AutoGluon-based NBAPredictor model.
"""

import datetime
import json
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from src.config import MODEL_CUTOFFS, MODEL_VARIANTS
from src.database import get_engine
from src.utils.timezone import APP_TIMEZONE, get_current_time

load_dotenv()
NBA_BETTING_BASE_DIR = os.getenv("NBA_BETTING_BASE_DIR", ".")


def _build_model_path(cutoff: str, variant: str) -> str:
    """Build model path from cutoff date and variant."""
    date_str = cutoff.replace("-", "")
    return f"models/nba_{variant}_thru_{date_str}"


def _build_model_registry():
    """Build the model registry from cutoffs and variants."""
    registry = []
    for cutoff in MODEL_CUTOFFS:
        entry = {"cutoff": cutoff}
        for variant in MODEL_VARIANTS:
            entry[variant] = _build_model_path(cutoff, variant)
        registry.append(entry)
    return registry


MODEL_REGISTRY = _build_model_registry()


def select_model_for_date(target_date, use_vegas=False):
    """
    Select the appropriate model for a given prediction date.

    Returns the most recent model that was trained BEFORE the target date.
    This ensures no data leakage (model never predicts games it was trained on).

    Args:
        target_date: Date of games to predict (datetime.date or string YYYY-MM-DD)
        use_vegas: If True, return vegas variants (cls_vegas, reg_vegas)

    Returns:
        Tuple of (cls_model_path, reg_model_path) or (None, None) if no suitable model
    """
    if isinstance(target_date, str):
        target_date = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()

    cls_key = "cls_vegas" if use_vegas else "cls"
    reg_key = "reg_vegas" if use_vegas else "reg"

    for model in MODEL_REGISTRY:
        cutoff = datetime.datetime.strptime(model["cutoff"], "%Y-%m-%d").date()
        if target_date > cutoff:
            cls_path = model[cls_key]
            reg_path = model[reg_key]
            # Only return paths if the classification model exists
            # (regression model is optional but cls is required)
            if Path(cls_path).exists():
                return cls_path, reg_path

    # No suitable model found (target date is before all training cutoffs or no models exist)
    return None, None


class Predictions:
    """Generate predictions for NBA games using trained AutoGluon models."""

    def __init__(
        self,
        line_type="open",
        cls_model_path=None,
        reg_model_path=None,
        target_date=None,
        use_vegas=False,
    ):
        """
        Initialize the Predictions class.

        Args:
            line_type: "open" or "current" - determines which line is stored in
                       prediction_spread for tracking. Does NOT affect model features
                       (models always use open_line via vegas_open_hv feature).
            cls_model_path: Path to classification model, or None for auto-select
            reg_model_path: Path to regression model, or None for auto-select
            target_date: Date for model selection (YYYY-MM-DD). If None, uses today.
                         Used to auto-select appropriate model from MODEL_REGISTRY.
            use_vegas: If True, use vegas variants (cls_vegas, reg_vegas) that include
                       the vegas line as a feature. Only used when auto-selecting models.
        """
        self.database_engine = get_engine()
        self.line_type = line_type
        self.target_date = target_date
        self.df = None
        self.prediction_df = None
        self.cls_predictor = None
        self.reg_predictor = None
        self.features = None
        self.model_training_end = None  # Will be set when model is loaded
        self.use_vegas = use_vegas

        # Auto-select model from registry if not provided
        if cls_model_path is None:
            if target_date is None:
                target_date = get_current_time().strftime("%Y-%m-%d")
            cls_model_path, reg_model_path = select_model_for_date(target_date, use_vegas=use_vegas)
        else:
            # Detect if provided model paths are vegas variants
            self.use_vegas = "_vegas_" in str(cls_model_path)

        self.cls_model_path = cls_model_path
        self.reg_model_path = reg_model_path

    def load_model(self):
        """Load the trained AutoGluon models (classification required, regression optional)."""
        from src.modeling import NBAPredictor

        # Check if model was found
        if self.cls_model_path is None:
            raise ValueError(
                f"No suitable model found for target date {self.target_date}. "
                f"Train models using: python scripts/train_models.py --all"
            )

        # Load classification model (required)
        cls_full_path = Path(NBA_BETTING_BASE_DIR) / self.cls_model_path
        try:
            self.cls_predictor = NBAPredictor.load(str(cls_full_path))
            self.features = self.cls_predictor.features

            # Extract training end date from model report to prevent predicting on training data
            # Handle both field names: "training_end" (new format) and "training_end_date" (ModelSetup format)
            training_end = self.cls_predictor.report.get(
                "training_end"
            ) or self.cls_predictor.report.get("training_end_date")
            if training_end:
                self.model_training_end = pd.to_datetime(training_end).date()
                # Silent model loading
            else:
                pass  # Silent - no training_end in model report
        except Exception as e:
            print(f"  ERROR: Loading classification model: {e}")
            raise

        # Load regression model (optional)
        # Regression model provides predicted_margin but is not required for picks
        if self.reg_model_path:
            reg_full_path = Path(NBA_BETTING_BASE_DIR) / self.reg_model_path
            if reg_full_path.exists():
                try:
                    self.reg_predictor = NBAPredictor.load(str(reg_full_path))
                except Exception as e:
                    # Log but continue - regression is optional
                    import logging

                    logging.debug(f"Regression model load failed (non-critical): {e}")
                    self.reg_predictor = None
            else:
                self.reg_predictor = None
        else:
            self.reg_predictor = None

    def load_data(self, current_date=True, start_date=None, end_date=None):
        """
        Load game data from database for prediction.

        Args:
            current_date: If True, load today's games only
            start_date: Start date string (YYYY-MM-DD) if not current_date
            end_date: End date string (YYYY-MM-DD) if not current_date
        """
        try:
            start_datetime, end_datetime = self._get_date_range(current_date, start_date, end_date)

            # Query games with features and lines
            # Lines table now has simplified schema: open_line, current_line (consensus across books)
            query = """
                SELECT
                    games.game_id,
                    games.game_datetime,
                    games.home_team,
                    games.away_team,
                    games.open_line,
                    features.data,
                    lines.current_line
                FROM games
                LEFT JOIN all_features_json AS features
                    ON games.game_id = features.game_id
                LEFT JOIN lines
                    ON games.game_id = lines.game_id
                WHERE games.game_datetime BETWEEN :start_dt AND :end_dt;
            """

            self.df = pd.read_sql(
                query,
                self.database_engine,
                params={"start_dt": start_datetime, "end_dt": end_datetime},
            )
            # Silent data loading

        except Exception as e:
            print(f"  ERROR: Loading data: {e}")
            raise

    def _get_date_range(self, current_date, start_date, end_date):
        """Get date range for query."""
        if current_date:
            today = get_current_time()
            today_str = today.strftime("%Y-%m-%d")
            return f"{today_str} 00:00:00", f"{today_str} 23:59:59"
        elif start_date and end_date:
            return f"{start_date} 00:00:00", f"{end_date} 23:59:59"
        else:
            raise ValueError("Must provide current_date=True or start_date and end_date")

    def _parse_json(self, data_str):
        """Parse JSON or Python dict string.

        Note: Uses ast.literal_eval as fallback for legacy data that may have been
        stored as Python dict literals instead of JSON. This is safe because:
        1. Data comes from our database, not user input
        2. literal_eval only evaluates literals (strings, numbers, tuples, lists, dicts, booleans, None)
        3. It cannot execute arbitrary code like eval() can
        """
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

    def create_predictions(self):
        """Generate predictions using the loaded models."""
        if self.cls_predictor is None:
            raise ValueError("Model not loaded. Call load_model() first.")

        if self.df is None or len(self.df) == 0:
            raise ValueError("No data loaded. Call load_data() first.")

        # Filter out games that fall within the model's training period
        # This prevents predicting on data the model was trained on
        if self.model_training_end is not None:
            # Extract date from game_datetime (handle mixed formats)
            self.df["game_date"] = pd.to_datetime(self.df["game_datetime"], format="mixed").dt.date
            self.df = self.df[self.df["game_date"] > self.model_training_end]
            self.df = self.df.drop(columns=["game_date"])

            if len(self.df) == 0:
                # Silent - no games remaining
                return

        # Parse and expand the JSON features
        self.df["data"] = self.df["data"].apply(self._parse_json)
        expanded_data = pd.json_normalize(self.df["data"])

        # Drop columns from expanded_data that already exist in base df to avoid duplicates
        base_cols = set(self.df.columns) - {"data"}
        expanded_cols_to_keep = [c for c in expanded_data.columns if c not in base_cols]
        expanded_data = expanded_data[expanded_cols_to_keep]

        features_df = pd.concat([self.df.drop(columns=["data"]), expanded_data], axis=1)

        # Use the line based on line_type
        if self.line_type == "open":
            features_df["prediction_line"] = features_df["open_line"].copy()
        else:
            features_df["prediction_line"] = (
                features_df["current_line"].fillna(features_df["open_line"]).copy()
            )

        # Add vegas_open_hv feature (negated open_line for home-visitor perspective)
        # This is required by vegas models and the regression model's feature pipeline
        features_df["vegas_open_hv"] = -pd.to_numeric(features_df["open_line"], errors="coerce")

        # For vegas models, vegas_open_hv is part of the feature set
        # For non-vegas models, it's only needed for the regression prediction step
        model_features = list(self.features)
        if self.use_vegas and "vegas_open_hv" not in model_features:
            model_features.append("vegas_open_hv")

        # Filter to games with required features
        available_features = [f for f in model_features if f in features_df.columns]
        missing_features = [f for f in model_features if f not in features_df.columns]

        # Silent - don't warn about missing features (l2w features often missing early season)

        # Prepare prediction data
        prediction_data = features_df[available_features].copy()

        # Drop rows with too many nulls
        # Note: AutoGluon handles missing values well, so we use a high threshold (80%)
        # Many l2w (last 2 weeks) features are null early in the season
        null_threshold = len(available_features) * 0.8
        valid_rows = prediction_data.isnull().sum(axis=1) < null_threshold
        prediction_data = prediction_data[valid_rows].reset_index(drop=True)
        features_df = features_df[valid_rows].reset_index(drop=True)

        if len(prediction_data) == 0:
            # Silent - no valid games
            return

        # Get classification predictions
        cls_predictions = self.cls_predictor.predict(prediction_data)

        # Get regression predictions (if model is available)
        reg_predictions = None
        if self.reg_predictor is not None:
            try:
                # Regression model may need vegas_open_hv
                # For vegas models, it's already in prediction_data
                # For non-vegas reg models, add it here
                reg_prediction_data = prediction_data.copy()
                if "vegas_open_hv" not in reg_prediction_data.columns:
                    reg_prediction_data["vegas_open_hv"] = features_df["vegas_open_hv"].values
                reg_predictions = self.reg_predictor.predict(reg_prediction_data)
            except Exception as e:
                print(f"Warning: Regression prediction failed: {e}")

        # Build prediction dataframe
        current_time = get_current_time()
        naive_time = current_time.replace(tzinfo=None, microsecond=0)

        # Convert spreads to home perspective (negate, since open_line is away perspective)
        open_spread = pd.to_numeric(features_df["open_line"], errors="coerce") * -1
        prediction_spread = pd.to_numeric(features_df["prediction_line"], errors="coerce") * -1

        # Determine model variant for tracking
        model_variant = "vegas" if self.use_vegas else "standard"

        # Map recommendation to pick
        pick = cls_predictions["recommendation"].map(
            {
                "Bet Home": "Home",
                "Bet Away": "Away",
            }
        )

        # home_cover_prob is the probability home covers
        # If prediction=True, confidence IS P(home covers)
        # If prediction=False, confidence is P(home covers), so we use it directly
        home_cover_prob = cls_predictions["confidence"].values
        predictions = cls_predictions["prediction"].values

        # Confidence in the pick (0-100 scale) - vectorized calculation
        # If picking Home (pred=True): confidence = home_cover_prob * 100
        # If picking Away (pred=False): confidence = (1 - home_cover_prob) * 100
        confidence_values = np.where(predictions, home_cover_prob, 1 - home_cover_prob) * 100

        self.prediction_df = pd.DataFrame(
            {
                "game_id": features_df["game_id"].values,
                "model_variant": model_variant,
                "predicted_at": naive_time,
                "open_spread": open_spread.values,
                "prediction_spread": prediction_spread.values,
                "home_cover_prob": home_cover_prob,
                "home_cover_pred": cls_predictions["prediction"].values,
                "predicted_margin": (
                    reg_predictions["predicted_margin"].values
                    if reg_predictions is not None
                    else None
                ),
                "pick": pick.values,
                "confidence": confidence_values,
            }
        )

    def save_records(self):
        """Save prediction records to database using upsert (INSERT OR REPLACE)."""
        if self.prediction_df is None:
            raise ValueError("No predictions to save. Call create_predictions() first.")

        try:
            from sqlalchemy import text

            # Select columns that match the predictions table schema
            save_cols = [
                "game_id",
                "model_variant",
                "predicted_at",
                "open_spread",
                "prediction_spread",
                "home_cover_prob",
                "home_cover_pred",
                "predicted_margin",
                "pick",
                "confidence",
            ]

            save_df = self.prediction_df[save_cols].copy()

            # Convert Timestamp to string for SQLite compatibility
            save_df["predicted_at"] = save_df["predicted_at"].astype(str)

            # Drop rows with null game_id (shouldn't happen, but safeguard)
            save_df = save_df.dropna(subset=["game_id"])

            if len(save_df) == 0:
                print("No valid predictions to save (all had null game_id)")
                return

            # Use SQLite upsert (INSERT OR REPLACE) to handle existing records
            # Primary key is (game_id, model_variant)
            upsert_sql = text("""
                INSERT OR REPLACE INTO predictions (
                    game_id, model_variant, predicted_at, open_spread,
                    prediction_spread, home_cover_prob, home_cover_pred,
                    predicted_margin, pick, confidence
                ) VALUES (
                    :game_id, :model_variant, :predicted_at, :open_spread,
                    :prediction_spread, :home_cover_prob, :home_cover_pred,
                    :predicted_margin, :pick, :confidence
                )
            """)

            # Convert DataFrame to list of dicts for parameterized insert
            records = save_df.to_dict(orient="records")

            with self.database_engine.connect() as conn:
                for record in records:
                    conn.execute(upsert_sql, record)
                conn.commit()

            # Silent save confirmation

        except Exception as e:
            print(f"  ERROR: Saving predictions: {e}")
            raise


def main_predictions(current_date=True, start_date=None, end_date=None, line_type="open"):
    """
    Generate and save predictions for games using both model variants.

    Runs both standard and vegas models, saving predictions for each variant.
    Automatically selects the appropriate model from MODEL_REGISTRY based on the
    prediction date (today if current_date=True, otherwise start_date).

    Args:
        current_date: If True, predict today's games
        start_date: Start date if not current_date
        end_date: End date if not current_date
        line_type: "open" or "current"
    """
    # Determine target date for model selection
    if current_date:
        target_date = get_current_time().strftime("%Y-%m-%d")
    else:
        target_date = start_date

    # Run both standard and vegas model variants
    total_saved = 0
    for use_vegas in [False, True]:
        variant_name = "vegas" if use_vegas else "standard"

        predictions = Predictions(line_type=line_type, target_date=target_date, use_vegas=use_vegas)

        try:
            predictions.load_model()
        except ValueError as e:
            # Model not found - skip silently
            continue

        predictions.load_data(current_date=current_date, start_date=start_date, end_date=end_date)
        predictions.create_predictions()

        if predictions.prediction_df is not None:
            predictions.save_records()
            total_saved += len(predictions.prediction_df)

    if total_saved > 0:
        print(f"  Generated {total_saved} predictions (standard + vegas)")


def on_demand_predictions(current_date=True, start_date=None, end_date=None, line_type="current"):
    """
    Generate predictions on-demand (without saving to database).

    Automatically selects the appropriate model from MODEL_REGISTRY.
    Used by the web app to show live predictions.
    """
    # Determine target date for model selection
    if current_date:
        target_date = get_current_time().strftime("%Y-%m-%d")
    else:
        target_date = start_date

    predictions = Predictions(line_type=line_type, target_date=target_date)
    predictions.load_model()
    predictions.load_data(current_date=current_date, start_date=start_date, end_date=end_date)
    predictions.create_predictions()

    if predictions.prediction_df is not None:
        print("\nOn-Demand Predictions:")
        print(predictions.prediction_df[["game_id", "pick", "confidence"]].head(10))
        print("----- On-Demand Predictions Successful -----")
    else:
        print("----- No predictions generated -----")

    return predictions.prediction_df


def backtest_predictions(start_date, end_date, line_type="open", save=False):
    """
    Generate predictions for a historical date range using both model variants.

    Runs both standard and vegas models. Automatically selects the correct model
    based on start_date from MODEL_REGISTRY, ensuring no data leakage.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        line_type: "open" or "current"
        save: If True, save predictions to database

    Returns:
        Dict with 'standard' and 'vegas' prediction DataFrames
    """
    results = {}

    for use_vegas in [False, True]:
        variant_name = "vegas" if use_vegas else "standard"
        print(f"\n{'='*50}")
        print(f"Backtesting {variant_name} model ({start_date} to {end_date})...")
        print(f"{'='*50}")

        try:
            predictions = Predictions(
                line_type=line_type, target_date=start_date, use_vegas=use_vegas
            )
            predictions.load_model()
        except ValueError as e:
            print(f"Skipping {variant_name}: {e}")
            continue

        print(f"Selected model: {predictions.cls_model_path}")

        predictions.load_data(current_date=False, start_date=start_date, end_date=end_date)
        predictions.create_predictions()

        if predictions.prediction_df is not None:
            print(f"\n{variant_name.capitalize()} Predictions:")
            print(predictions.prediction_df[["game_id", "pick", "confidence"]].head(10))

            if save:
                predictions.save_records()
                print(f"----- {variant_name.capitalize()} Predictions Saved -----")

            results[variant_name] = predictions.prediction_df
        else:
            print(f"----- No {variant_name} predictions generated -----")

    print("\n----- Backtest Complete -----")
    return results if results else None


if __name__ == "__main__":
    # Example usage:
    # main_predictions(current_date=True)
    # on_demand_predictions(current_date=False, start_date="2023-10-01", end_date="2023-10-31")
    # backtest_predictions("2025-12-01", "2025-12-31", save=True)
    pass
