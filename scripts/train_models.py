#!/usr/bin/env python3
"""
Train NBA prediction models for walk-forward backtesting.

This script trains models at various cutoff dates to enable:
1. Historical backtesting without data leakage
2. Walk-forward validation across seasons
3. Monthly granularity for recent years

Model Variants:
- Classification (cls): Predicts if home team beats the spread
- Regression (reg): Predicts point differential
- With Vegas line (_vegas): Includes opening line as a feature
- Without Vegas line: Pure team stats only

Usage:
    # Train all models (all 4 variants)
    python scripts/train_models.py --all

    # Train specific variant
    python scripts/train_models.py --all --cls-only          # Classification without vegas
    python scripts/train_models.py --all --reg-only          # Regression without vegas
    python scripts/train_models.py --all --cls-vegas-only    # Classification with vegas
    python scripts/train_models.py --all --reg-vegas-only    # Regression with vegas

    # Train a specific cutoff
    python scripts/train_models.py --cutoff 2024-04-14

    # Train only yearly or monthly models
    python scripts/train_models.py --yearly
    python scripts/train_models.py --monthly

    # Dry run - show what would be trained
    python scripts/train_models.py --all --dry-run

    # Check status
    python scripts/train_models.py --status
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import (
    MODEL_YEARLY_CUTOFFS,
    MODEL_MONTHLY_CUTOFFS,
    MODEL_TRAINING_START,
    MODEL_EXCLUDED_TYPES,
)

# Model variant configurations (maps variant name to training params)
MODEL_VARIANT_CONFIGS = [
    {"name": "cls", "problem_type": "classification", "use_vegas": False},
    {"name": "reg", "problem_type": "regression", "use_vegas": False},
    {"name": "cls_vegas", "problem_type": "classification", "use_vegas": True},
    {"name": "reg_vegas", "problem_type": "regression", "use_vegas": True},
]


def get_model_path(cutoff_date: str, variant_name: str) -> str:
    """Generate model path from cutoff date and variant."""
    date_str = cutoff_date.replace("-", "")
    return f"models/nba_{variant_name}_thru_{date_str}"


def train_model_variant(
    cutoff_date: str,
    variant: dict,
    time_limit: int = 300,
    dry_run: bool = False
):
    """
    Train a single model variant for a specific cutoff date.

    Args:
        cutoff_date: Training data cutoff (YYYY-MM-DD)
        variant: Dict with name, problem_type, use_vegas
        time_limit: AutoGluon training time in seconds
        dry_run: If True, just print what would be done
    """
    from src.modeling import NBAPredictor

    model_path = get_model_path(cutoff_date, variant["name"])

    # Check if model already exists
    if Path(model_path).exists():
        print(f"    {variant['name']}: already exists, skipping")
        return

    if dry_run:
        print(f"    {variant['name']}: [DRY RUN] would train")
        return

    print(f"    {variant['name']}: training...")

    # For testing period, use the next 30 days after cutoff
    cutoff_dt = datetime.strptime(cutoff_date, "%Y-%m-%d")
    test_end_dt = cutoff_dt + timedelta(days=30)
    test_end = test_end_dt.strftime("%Y-%m-%d")

    predictor = NBAPredictor(
        problem_type=variant["problem_type"],
        use_vegas_line=variant["use_vegas"],
    )
    predictor.load_data_by_dates(
        training_start=MODEL_TRAINING_START,
        training_end=cutoff_date,
        testing_start=cutoff_date,
        testing_end=test_end,
    )
    predictor.train(
        time_limit=time_limit,
        presets="medium_quality",
        excluded_model_types=MODEL_EXCLUDED_TYPES,
    )
    predictor.save(model_path)
    print(f"    {variant['name']}: saved to {model_path}")


def train_models_for_cutoff(
    cutoff_date: str,
    variants: list[dict],
    time_limit: int = 300,
    dry_run: bool = False
):
    """Train all specified variants for a single cutoff date."""
    print(f"\n{'='*60}")
    print(f"Cutoff: {cutoff_date}")
    print(f"{'='*60}")

    for variant in variants:
        train_model_variant(cutoff_date, variant, time_limit, dry_run)


def train_all_models(
    yearly: bool = True,
    monthly: bool = True,
    variants: list[dict] = None,
    time_limit: int = 300,
    dry_run: bool = False
):
    """Train all models for specified cutoffs and variants."""
    if variants is None:
        variants = MODEL_VARIANT_CONFIGS

    cutoffs = []
    if yearly:
        cutoffs.extend(MODEL_YEARLY_CUTOFFS)
    if monthly:
        cutoffs.extend(MODEL_MONTHLY_CUTOFFS)

    # Sort chronologically
    cutoffs = sorted(set(cutoffs))

    total_models = len(cutoffs) * len(variants)
    variant_names = [v["name"] for v in variants]

    print(f"Training {total_models} models total")
    print(f"  Cutoffs: {len(cutoffs)}")
    print(f"  Variants: {variant_names}")
    print(f"  Time limit per model: {time_limit} seconds")
    print(f"  Estimated total time: {total_models * time_limit / 60:.0f} minutes")

    for i, cutoff in enumerate(cutoffs, 1):
        print(f"\n[{i}/{len(cutoffs)}] Processing cutoff: {cutoff}")
        train_models_for_cutoff(cutoff, variants, time_limit, dry_run)


def check_model_status():
    """Show which models exist and which are missing."""
    all_cutoffs = sorted(set(MODEL_YEARLY_CUTOFFS + MODEL_MONTHLY_CUTOFFS))

    print("\nModel Status:")
    print("-" * 90)
    header = f"{'Cutoff':<12}"
    for v in MODEL_VARIANT_CONFIGS:
        header += f" {v['name']:<15}"
    print(header)
    print("-" * 90)

    counts = {v["name"]: 0 for v in MODEL_VARIANT_CONFIGS}

    for cutoff in all_cutoffs:
        row = f"{cutoff:<12}"
        for variant in MODEL_VARIANT_CONFIGS:
            model_path = get_model_path(cutoff, variant["name"])
            exists = Path(model_path).exists()
            if exists:
                counts[variant["name"]] += 1
            status = "✓" if exists else "✗"
            row += f" {status:<15}"
        print(row)

    print("-" * 90)
    total_row = "Total:      "
    for variant in MODEL_VARIANT_CONFIGS:
        total_row += f" {counts[variant['name']]}/{len(all_cutoffs):<11}"
    print(total_row)


def main():
    parser = argparse.ArgumentParser(
        description="Train NBA prediction models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Cutoff selection
    parser.add_argument("--all", action="store_true", help="Train all cutoffs")
    parser.add_argument("--yearly", action="store_true", help="Train yearly cutoffs only")
    parser.add_argument("--monthly", action="store_true", help="Train monthly cutoffs only")
    parser.add_argument("--cutoff", type=str, help="Train specific cutoff date (YYYY-MM-DD)")

    # Variant selection (mutually exclusive)
    variant_group = parser.add_mutually_exclusive_group()
    variant_group.add_argument("--cls-only", action="store_true",
                               help="Train classification without vegas only")
    variant_group.add_argument("--reg-only", action="store_true",
                               help="Train regression without vegas only")
    variant_group.add_argument("--cls-vegas-only", action="store_true",
                               help="Train classification with vegas only")
    variant_group.add_argument("--reg-vegas-only", action="store_true",
                               help="Train regression with vegas only")
    variant_group.add_argument("--no-vegas", action="store_true",
                               help="Train both cls and reg without vegas")
    variant_group.add_argument("--with-vegas", action="store_true",
                               help="Train both cls_vegas and reg_vegas")

    # Other options
    parser.add_argument("--time-limit", type=int, default=300,
                        help="Training time per model in seconds (default: 300)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be trained without training")
    parser.add_argument("--status", action="store_true",
                        help="Show which models exist")

    args = parser.parse_args()

    if args.status:
        check_model_status()
        return

    # Determine which variants to train
    if args.cls_only:
        variants = [v for v in MODEL_VARIANT_CONFIGS if v["name"] == "cls"]
    elif args.reg_only:
        variants = [v for v in MODEL_VARIANT_CONFIGS if v["name"] == "reg"]
    elif args.cls_vegas_only:
        variants = [v for v in MODEL_VARIANT_CONFIGS if v["name"] == "cls_vegas"]
    elif args.reg_vegas_only:
        variants = [v for v in MODEL_VARIANT_CONFIGS if v["name"] == "reg_vegas"]
    elif args.no_vegas:
        variants = [v for v in MODEL_VARIANT_CONFIGS if not v["use_vegas"]]
    elif args.with_vegas:
        variants = [v for v in MODEL_VARIANT_CONFIGS if v["use_vegas"]]
    else:
        variants = MODEL_VARIANT_CONFIGS  # All 4 variants

    # Execute training
    if args.cutoff:
        train_models_for_cutoff(args.cutoff, variants, args.time_limit, args.dry_run)
    elif args.all:
        train_all_models(yearly=True, monthly=True, variants=variants,
                         time_limit=args.time_limit, dry_run=args.dry_run)
    elif args.yearly:
        train_all_models(yearly=True, monthly=False, variants=variants,
                         time_limit=args.time_limit, dry_run=args.dry_run)
    elif args.monthly:
        train_all_models(yearly=False, monthly=True, variants=variants,
                         time_limit=args.time_limit, dry_run=args.dry_run)
    else:
        parser.print_help()
        print("\nUse --status to see which models exist")


if __name__ == "__main__":
    main()
