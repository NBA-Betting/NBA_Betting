"""
NBA Betting Data Pipeline

Orchestrates data collection, ETL, and predictions.
Primary entry point is update_data.py which calls run_pipeline().

Can also be run directly:
    python -m src.pipeline --dates 2024 --collect
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Project root for subprocess calls (Scrapy)
PROJECT_ROOT = Path(__file__).parent.parent


def log(msg: str, level: str = "INFO"):
    """Simple logging with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")


def run_covers_collection(dates: str) -> bool:
    """Collect game data from Covers.com (PRIMARY source).

    Args:
        dates: Date specification - "daily_update", season year(s), or specific date(s)
    """
    log(f"Starting Covers.com data collection (dates={dates})...")

    try:
        # Stream output directly to console (no capture_output)
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "scrapy",
                "crawl",
                "game_covers_historic_scores_and_odds_spider",
                "-a",
                f"dates={dates}",
                "-a",
                "save_data=True",
                "-a",
                "view_data=True",
            ],
            cwd=PROJECT_ROOT / "src" / "data_sources" / "game",
        )

        if result.returncode != 0:
            log("Covers collection failed", "ERROR")
            return False

        log("Covers.com collection complete")
        return True

    except Exception as e:
        log(f"Covers collection error: {e}", "ERROR")
        return False


def run_odds_api_collection() -> bool:
    """Collect live odds from The Odds API (SECONDARY source).

    Note: Odds API only fetches current/upcoming games - no historical data.
    """
    log("Starting Odds API data collection...")

    try:
        from src.data_sources.game.odds_api import update_game_data

        update_game_data(past_games=True)
        log("Odds API collection complete")
        return True

    except Exception as e:
        log(f"Odds API collection failed (non-critical): {e}", "WARNING")
        return False  # Non-critical, continue pipeline


def run_nbastats_collection(dates: str) -> bool:
    """Collect NBA team statistics.

    Args:
        dates: Date specification - "daily_update", season year(s), or specific date(s)
    """
    log(f"Starting NBA Stats collection (dates={dates})...")

    try:
        from src.data_sources.team.nbastats_fetcher import NBAStatsFetcher

        fetcher = NBAStatsFetcher()
        fetcher.fetch_all_stats(dates=dates, save_data=True)

        log("NBA Stats collection complete")
        return True

    except Exception as e:
        log(f"NBA Stats collection failed: {e}", "ERROR")
        return False


def run_etl(start_date: str = "2023-09-01") -> bool:
    """Run ETL pipeline to create features.

    Args:
        start_date: Start date for ETL processing (YYYY-MM-DD format)
    """
    log(f"Starting ETL pipeline (start_date={start_date})...")

    try:
        from src.etl.main_etl import ETLPipeline

        etl = ETLPipeline(start_date=start_date)

        etl.load_features_data(
            [
                "team_nbastats_general_traditional",
                "team_nbastats_general_advanced",
                "team_nbastats_general_fourfactors",
                "team_nbastats_general_opponent",
            ]
        )

        etl.prepare_all_tables()
        etl.feature_creation_pre_merge()
        etl.merge_features_data()
        etl.feature_creation_post_merge()
        etl.clean_and_save_combined_features()

        log("ETL pipeline complete")
        return True

    except Exception as e:
        log(f"ETL pipeline failed: {e}", "ERROR")
        return False


def parse_dates_to_range(dates: str) -> tuple[str, str]:
    """Convert dates specification to start/end date range.

    Args:
        dates: "daily_update", season year(s), or specific date(s)

    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
    """
    from src.config import NBA_IMPORTANT_DATES
    from src.utils.timezone import get_current_time
    from datetime import timedelta

    if dates == "daily_update":
        current = get_current_time()
        yesterday = (current - timedelta(days=1)).strftime("%Y-%m-%d")
        tomorrow = (current + timedelta(days=1)).strftime("%Y-%m-%d")
        return yesterday, tomorrow

    # Season year(s) like "2024" or "2023,2024"
    if "-" not in dates:
        seasons = [s.strip() for s in dates.split(",")]
        all_starts = []
        all_ends = []
        for season in seasons:
            year = int(season)
            season_key = f"{year}-{year+1}"
            if season_key in NBA_IMPORTANT_DATES:
                all_starts.append(NBA_IMPORTANT_DATES[season_key]["reg_season_start_date"])
                all_ends.append(NBA_IMPORTANT_DATES[season_key]["postseason_end_date"])
        if all_starts and all_ends:
            return min(all_starts), max(all_ends)
        # Fallback if season not found
        return f"{seasons[0]}-10-01", f"{int(seasons[-1])+1}-06-30"

    # Specific date(s) like "2025-01-15" or "2025-01-15,2025-01-16"
    date_list = [d.strip() for d in dates.split(",")]
    return min(date_list), max(date_list)


def run_predictions(
    dates: str = "daily_update", start_date: str | None = None, end_date: str | None = None
) -> bool:
    """Generate predictions for games.

    Args:
        dates: Date specification - "daily_update", season year, or specific date(s)
        start_date: Start date for predictions (overrides dates if provided)
        end_date: End date for predictions (overrides dates if provided)
    """
    try:
        from src.predictions import main_predictions

        # If explicit start/end provided, use those; otherwise parse from dates
        if start_date is None or end_date is None:
            start_date, end_date = parse_dates_to_range(dates)

        log(f"Starting predictions ({start_date} to {end_date})...")
        main_predictions(current_date=False, start_date=start_date, end_date=end_date)

        log("Predictions complete")
        return True

    except Exception as e:
        log(f"Predictions failed: {e}", "ERROR")
        return False


def validate_dates(dates: str) -> bool:
    """Validate the dates argument format."""
    if dates == "daily_update":
        return True

    # Check if it's season year(s) like "2024" or "2023,2024"
    if "-" not in dates:
        try:
            for season in dates.split(","):
                year = int(season.strip())
                if year < 1996 or year > 2030:
                    return False
            return True
        except ValueError:
            return False

    # Check if it's specific date(s) like "2025-01-15" or "2025-01-15,2025-01-16"
    for date_str in dates.split(","):
        try:
            datetime.strptime(date_str.strip(), "%Y-%m-%d")
        except ValueError:
            return False
    return True


def run_pipeline(
    dates: str = "daily_update",
    stage: str = "all",
    skip_odds_api: bool = False,
    skip_nbastats: bool = False,
    etl_start_date: str = "2023-09-01",
) -> bool:
    """
    Run the NBA Betting data pipeline.

    Args:
        dates: Date specification - "daily_update", season year(s), or specific date(s)
        stage: Pipeline stage - "all", "collect", "etl", or "predict"
        skip_odds_api: Skip Odds API collection
        skip_nbastats: Skip NBA Stats collection
        etl_start_date: Start date for ETL processing

    Returns:
        True if pipeline completed successfully, False otherwise
    """
    # Validate dates
    if not validate_dates(dates):
        log(f"Invalid dates format: {dates}", "ERROR")
        log("Expected: 'daily_update', season year (2024), or date (2025-01-15)")
        return False

    run_all = stage == "all"

    log("=" * 60)
    log("NBA Betting Pipeline")
    log(f"Dates: {dates}")
    log("=" * 60)

    success = True

    # Data Collection
    if run_all or stage == "collect":
        log("-" * 40)
        log("STAGE: Data Collection")
        log("-" * 40)

        # Covers is primary - must succeed
        if not run_covers_collection(dates):
            log("Covers collection failed - aborting pipeline", "ERROR")
            return False

        # Odds API is optional (only fetches current/upcoming games)
        if not skip_odds_api:
            if dates == "daily_update":
                run_odds_api_collection()  # Continue even if fails
            else:
                log("Skipping Odds API (only works for current games, not historical)")
        else:
            log("Skipping Odds API (--skip-odds-api flag set)")

        # NBA Stats
        if not skip_nbastats:
            if not run_nbastats_collection(dates):
                log("NBA Stats collection failed - continuing with existing data", "WARNING")
        else:
            log("Skipping NBA Stats (--skip-nbastats flag set)")

    # ETL
    if run_all or stage == "etl":
        log("-" * 40)
        log("STAGE: ETL Pipeline")
        log("-" * 40)

        if not run_etl(etl_start_date):
            log("ETL failed - aborting pipeline", "ERROR")
            return False

    # Predictions
    if run_all or stage == "predict":
        log("-" * 40)
        log("STAGE: Predictions")
        log("-" * 40)

        if not run_predictions(dates):
            log("Predictions failed", "ERROR")
            success = False

    log("=" * 60)
    if success:
        log("Pipeline completed successfully!")
    else:
        log("Pipeline completed with errors", "WARNING")
    log("=" * 60)

    return success


def main():
    """CLI entry point for running pipeline directly."""
    parser = argparse.ArgumentParser(
        description="Run NBA Betting data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Stage selection
    parser.add_argument("--collect", action="store_true", help="Run data collection only")
    parser.add_argument("--etl", action="store_true", help="Run ETL only")
    parser.add_argument("--predict", action="store_true", help="Run predictions only")

    # Date options
    parser.add_argument(
        "--dates",
        type=str,
        default="daily_update",
        help="Date specification: 'daily_update' (default), season year, or date",
    )
    parser.add_argument(
        "--etl-start-date",
        type=str,
        default="2023-09-01",
        help="Start date for ETL processing (default: 2023-09-01)",
    )

    # Optional skips
    parser.add_argument("--skip-odds-api", action="store_true", help="Skip Odds API collection")
    parser.add_argument("--skip-nbastats", action="store_true", help="Skip NBA Stats collection")

    args = parser.parse_args()

    # Determine stage
    if args.collect:
        stage = "collect"
    elif args.etl:
        stage = "etl"
    elif args.predict:
        stage = "predict"
    else:
        stage = "all"

    success = run_pipeline(
        dates=args.dates,
        stage=stage,
        skip_odds_api=args.skip_odds_api,
        skip_nbastats=args.skip_nbastats,
        etl_start_date=args.etl_start_date,
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
