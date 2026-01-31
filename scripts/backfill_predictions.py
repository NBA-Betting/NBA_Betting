"""
Backfill Predictions for Historical Games (Walk-Forward Backtesting)

Generates historically accurate predictions by using each model only for games
that occurred AFTER its training cutoff. This prevents data leakage and provides
true out-of-sample backtesting results.

Supports all 4 model variants:
- cls: Classification without Vegas line
- reg: Regression without Vegas line
- cls_vegas: Classification with Vegas line as feature
- reg_vegas: Regression with Vegas line as feature

Each model in MODEL_CUTOFFS predicts games from:
  - Start: day after model's training cutoff
  - End: the next model's training cutoff (or today for the latest model)

This is the correct way to backtest time-series predictions.
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.config import MODEL_CUTOFFS, MODEL_VARIANTS
from src.database import get_engine
from src.predictions.generator import _build_model_path, Predictions


def get_model_date_ranges(use_vegas=False):
    """
    Calculate the valid prediction date range for each model.

    Returns list of dicts with model paths and their valid date ranges:
    - start_date: first day model can predict (day after training cutoff)
    - end_date: last day model should predict (day of next model's cutoff)
    """
    ranges = []

    # MODEL_CUTOFFS is sorted newest first, so reverse for chronological order
    cutoffs = list(reversed(MODEL_CUTOFFS))

    cls_variant = "cls_vegas" if use_vegas else "cls"
    reg_variant = "reg_vegas" if use_vegas else "reg"

    for i, cutoff in enumerate(cutoffs):
        cutoff_date = datetime.strptime(cutoff, "%Y-%m-%d").date()

        # Model can predict starting the day after its training cutoff
        start_date = cutoff_date + timedelta(days=1)

        # Model's valid range ends at the next model's cutoff (or today)
        if i + 1 < len(cutoffs):
            next_cutoff = datetime.strptime(cutoffs[i + 1], "%Y-%m-%d").date()
            end_date = next_cutoff
        else:
            # Latest model - predict through today
            end_date = datetime.now().date()

        cls_path = _build_model_path(cutoff, cls_variant)
        reg_path = _build_model_path(cutoff, reg_variant)

        ranges.append({
            "cls": cls_path,
            "reg": reg_path,
            "cutoff": cutoff,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        })

    return ranges


def backfill_predictions_walkforward(use_vegas=False, start_from_cutoff: str = None):
    """
    Generate predictions using walk-forward backtesting.

    Each model predicts only games in its valid time window, ensuring
    no data leakage (model never sees games it's predicting).

    Args:
        use_vegas: If True, use vegas variants (cls_vegas, reg_vegas)
        start_from_cutoff: Optional cutoff date (YYYY-MM-DD) to start from.
                          If None, starts from the earliest model.
    """
    variant_label = "WITH Vegas line" if use_vegas else "WITHOUT Vegas line"

    print("=" * 70)
    print(f"Walk-Forward Backtesting - {variant_label}")
    print("=" * 70)
    print()
    print("Each model will only predict games AFTER its training cutoff.")
    print("This ensures true out-of-sample backtesting with no data leakage.")
    print()

    model_ranges = get_model_date_ranges(use_vegas=use_vegas)
    engine = get_engine()

    # Filter to start from specific cutoff if requested
    if start_from_cutoff:
        model_ranges = [m for m in model_ranges if m["cutoff"] >= start_from_cutoff]

    # Filter to only models that exist
    existing_ranges = []
    for m in model_ranges:
        if Path(m["cls"]).exists() and Path(m["reg"]).exists():
            existing_ranges.append(m)
        else:
            print(f"Skipping {m['cutoff']} - models not found")

    if not existing_ranges:
        print("No models found! Train models first with scripts/train_models.py")
        return 0

    print(f"Processing {len(existing_ranges)} models...")
    print("-" * 70)

    total_predictions = 0
    model_stats = []

    for i, model_info in enumerate(existing_ranges):
        print(f"\n[{i+1}/{len(existing_ranges)}] Model cutoff: {model_info['cutoff']}")
        print(f"    CLS: {model_info['cls'].split('/')[-1]}")
        print(f"    REG: {model_info['reg'].split('/')[-1]}")
        print(f"    Prediction range: {model_info['start_date']} to {model_info['end_date']}")

        # Check how many games are in this range
        count_query = f"""
            SELECT COUNT(*) as cnt
            FROM games g
            INNER JOIN all_features_json f ON g.game_id = f.game_id
            WHERE DATE(g.game_datetime) > '{model_info['cutoff']}'
              AND DATE(g.game_datetime) <= '{model_info['end_date']}'
              AND g.home_score IS NOT NULL
        """
        count_df = pd.read_sql(count_query, engine)
        game_count = count_df.iloc[0]["cnt"]

        if game_count == 0:
            print(f"    No games in this range, skipping...")
            continue

        print(f"    Games to predict: {game_count}")

        try:
            # Initialize predictor with this specific model
            predictor = Predictions(
                line_type="open",
                cls_model_path=model_info["cls"],
                reg_model_path=model_info["reg"],
            )
            predictor.load_model()

            # Load games for this model's valid range
            predictor.load_data(
                current_date=False,
                start_date=model_info["start_date"],
                end_date=model_info["end_date"],
            )

            if predictor.df is not None and len(predictor.df) > 0:
                predictor.create_predictions()

                if predictor.prediction_df is not None and len(predictor.prediction_df) > 0:
                    predictor.save_records()
                    batch_count = len(predictor.prediction_df)
                    total_predictions += batch_count
                    print(f"    Saved {batch_count} predictions")

                    model_stats.append({
                        "model": model_info["cls"].split("/")[-1],
                        "cutoff": model_info["cutoff"],
                        "predictions": batch_count,
                    })
                else:
                    print(f"    No predictions generated")
            else:
                print(f"    No data loaded")

        except Exception as e:
            print(f"    ERROR: {e}")
            import traceback
            traceback.print_exc()

    # Summary
    print()
    print("=" * 70)
    print(f"BACKFILL COMPLETE - {variant_label}")
    print("=" * 70)
    print(f"\nTotal predictions saved: {total_predictions}")

    if model_stats:
        print(f"\nPredictions by model:")
        print("-" * 50)
        for stat in model_stats:
            print(f"  {stat['model']}: {stat['predictions']:,} games")

    print()
    print("=" * 70)

    return total_predictions


def clear_existing_predictions():
    """Clear existing predictions from the database before backfill."""
    from sqlalchemy import text

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM predictions"))
        conn.commit()
    print("Cleared existing predictions from database.")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill predictions using walk-forward backtesting",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--clear", action="store_true",
        help="Clear existing predictions before backfill"
    )
    parser.add_argument(
        "--from", dest="from_cutoff", type=str,
        help="Start from specific model cutoff date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--vegas", action="store_true",
        help="Use vegas variants (cls_vegas, reg_vegas)"
    )
    parser.add_argument(
        "--no-vegas", action="store_true",
        help="Use non-vegas variants (cls, reg) - default"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Run backfill for both vegas and non-vegas variants"
    )

    args = parser.parse_args()

    print()
    print("Walk-Forward Backfill Predictions")
    print("=" * 70)
    print()

    if args.clear:
        clear_existing_predictions()
        print()

    if args.all:
        # Run both variants
        print("Running backfill for BOTH variants...")
        print()
        total = 0
        total += backfill_predictions_walkforward(use_vegas=False, start_from_cutoff=args.from_cutoff)
        print()
        total += backfill_predictions_walkforward(use_vegas=True, start_from_cutoff=args.from_cutoff)
        print()
        print(f"GRAND TOTAL: {total:,} predictions")
    elif args.vegas:
        backfill_predictions_walkforward(use_vegas=True, start_from_cutoff=args.from_cutoff)
    else:
        # Default: non-vegas
        backfill_predictions_walkforward(use_vegas=False, start_from_cutoff=args.from_cutoff)


if __name__ == "__main__":
    main()
