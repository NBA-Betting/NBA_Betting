#!/usr/bin/env python3
"""
NBA Betting Data Pipeline - Quick Start

Usage:
    python update_data.py                      # Daily update (yesterday/today/tomorrow)
    python update_data.py --season 2024        # Full season backfill
    python update_data.py --date 2025-01-15    # Specific date fix

Requirements:
    pip install -e ".[scraping]"               # Install scraping dependencies
    cp .env.example .env                       # Create environment file (optional for Odds API)
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Update NBA Betting data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python update_data.py                       Daily update (default)
  python update_data.py --season 2024         Backfill 2024-25 season
  python update_data.py --season 2023,2024    Backfill multiple seasons
  python update_data.py --date 2025-01-15     Fix specific date
  python update_data.py --date 2025-01-15,2025-01-16  Fix date range
  python update_data.py --collect-only        Skip ETL and predictions
  python update_data.py --skip-odds-api       Skip Odds API (no API key needed)
        """,
    )

    # Date selection (mutually exclusive)
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--season", "-s", type=str,
        help="Backfill full season(s), e.g., '2024' or '2023,2024'"
    )
    date_group.add_argument(
        "--date", "-d", type=str,
        help="Specific date(s), e.g., '2025-01-15' or '2025-01-15,2025-01-16'"
    )

    # Stage selection
    parser.add_argument(
        "--collect-only", "-c", action="store_true",
        help="Only collect data (skip ETL and predictions)"
    )
    parser.add_argument(
        "--etl-only", "-e", action="store_true",
        help="Only run ETL (skip collection and predictions)"
    )
    parser.add_argument(
        "--predict-only", "-p", action="store_true",
        help="Only run predictions (skip collection and ETL)"
    )

    # Skip options
    parser.add_argument(
        "--skip-odds-api", action="store_true",
        help="Skip Odds API collection (if no API key)"
    )
    parser.add_argument(
        "--skip-nbastats", action="store_true",
        help="Skip NBA Stats collection"
    )

    # Advanced options (passed through to pipeline)
    parser.add_argument(
        "--etl-start-date", type=str, default="2023-09-01",
        help="Start date for ETL processing (default: 2023-09-01)"
    )

    args = parser.parse_args()

    # Determine dates string
    if args.season:
        dates = args.season
        date_desc = f"Season {args.season}"
    elif args.date:
        dates = args.date
        date_desc = f"Date {args.date}"
    else:
        dates = "daily_update"
        date_desc = "Daily update"

    # Determine stage
    if args.collect_only:
        stage = "collect"
        stage_desc = "Data collection only"
    elif args.etl_only:
        stage = "etl"
        stage_desc = "ETL only"
    elif args.predict_only:
        stage = "predict"
        stage_desc = "Predictions only"
    else:
        stage = "all"
        stage_desc = "Full pipeline"

    # Print startup banner
    print("\n" + "=" * 60)
    print("  NBA Betting Data Pipeline")
    print("=" * 60)
    print(f"\n  Mode: {date_desc}")
    print(f"  Stage: {stage_desc}")
    print("\n  Press Ctrl+C to stop")
    print("=" * 60 + "\n")

    # Run pipeline
    try:
        from src.pipeline import run_pipeline

        result = run_pipeline(
            dates=dates,
            stage=stage,
            skip_odds_api=args.skip_odds_api,
            skip_nbastats=args.skip_nbastats,
            etl_start_date=args.etl_start_date,
        )
        sys.exit(0 if result else 1)

    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
