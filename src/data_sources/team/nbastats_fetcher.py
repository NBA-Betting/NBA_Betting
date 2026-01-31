"""
Fetches NBA team stats from NBA.com via nba_api library.

Usage: NBAStatsFetcher().fetch_all_stats(dates="daily_update", save_data=True)
"""

import random
import time
from datetime import datetime, timedelta
from typing import Literal

import pandas as pd
from nba_api.stats.endpoints import leaguedashteamstats
from tqdm import tqdm
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import sessionmaker

from src import config
from src.database import engine
from src.database_orm import (
    NbastatsGeneralAdvancedTable,
    NbastatsGeneralFourfactorsTable,
    NbastatsGeneralOpponentTable,
    NbastatsGeneralTraditionalTable,
)
from src.utils.data_source_utils import convert_season_to_short, convert_season_to_long
from src.utils.timezone import get_current_time, get_current_year
from src.utils.general_utils import find_season_information

# Column mappings from NBA API to our database schema
TRADITIONAL_COLUMNS = {
    "TEAM_NAME": "team_name",
    "GP": "gp",
    "W": "w",
    "L": "l",
    "W_PCT": "w_pct",
    "MIN": "min",
    "FGM": "fgm",
    "FGA": "fga",
    "FG_PCT": "fg_pct",
    "FG3M": "fg3m",
    "FG3A": "fg3a",
    "FG3_PCT": "fg3_pct",
    "FTM": "ftm",
    "FTA": "fta",
    "FT_PCT": "ft_pct",
    "OREB": "oreb",
    "DREB": "dreb",
    "REB": "reb",
    "AST": "ast",
    "TOV": "tov",
    "STL": "stl",
    "BLK": "blk",
    "BLKA": "blka",
    "PF": "pf",
    "PFD": "pfd",
    "PTS": "pts",
    "PLUS_MINUS": "plus_minus",
}

ADVANCED_COLUMNS = {
    "TEAM_NAME": "team_name",
    "GP": "gp",
    "W": "w",
    "L": "l",
    "W_PCT": "w_pct",
    "MIN": "min",
    "E_OFF_RATING": "e_off_rating",
    "OFF_RATING": "off_rating",
    "E_DEF_RATING": "e_def_rating",
    "DEF_RATING": "def_rating",
    "E_NET_RATING": "e_net_rating",
    "NET_RATING": "net_rating",
    "AST_PCT": "ast_pct",
    "AST_TO": "ast_to",
    "AST_RATIO": "ast_ratio",
    "OREB_PCT": "oreb_pct",
    "DREB_PCT": "dreb_pct",
    "REB_PCT": "reb_pct",
    "TM_TOV_PCT": "tm_tov_pct",
    "EFG_PCT": "efg_pct",
    "TS_PCT": "ts_pct",
    "E_PACE": "e_pace",
    "PACE": "pace",
    "PACE_PER40": "pace_per40",
    "POSS": "poss",
    "PIE": "pie",
}

FOURFACTORS_COLUMNS = {
    "TEAM_NAME": "team_name",
    "GP": "gp",
    "W": "w",
    "L": "l",
    "W_PCT": "w_pct",
    "MIN": "min",
    "EFG_PCT": "efg_pct",
    "FTA_RATE": "fta_rate",
    "TM_TOV_PCT": "tm_tov_pct",
    "OREB_PCT": "oreb_pct",
    "OPP_EFG_PCT": "opp_efg_pct",
    "OPP_FTA_RATE": "opp_fta_rate",
    "OPP_TOV_PCT": "opp_tov_pct",
    "OPP_OREB_PCT": "opp_oreb_pct",
}

OPPONENT_COLUMNS = {
    "TEAM_NAME": "team_name",
    "GP": "gp",
    "W": "w",
    "L": "l",
    "W_PCT": "w_pct",
    "MIN": "min",
    "OPP_FGM": "opp_fgm",
    "OPP_FGA": "opp_fga",
    "OPP_FG_PCT": "opp_fg_pct",
    "OPP_FG3M": "opp_fg3m",
    "OPP_FG3A": "opp_fg3a",
    "OPP_FG3_PCT": "opp_fg3_pct",
    "OPP_FTM": "opp_ftm",
    "OPP_FTA": "opp_fta",
    "OPP_FT_PCT": "opp_ft_pct",
    "OPP_OREB": "opp_oreb",
    "OPP_DREB": "opp_dreb",
    "OPP_REB": "opp_reb",
    "OPP_AST": "opp_ast",
    "OPP_TOV": "opp_tov",
    "OPP_STL": "opp_stl",
    "OPP_BLK": "opp_blk",
    "OPP_BLKA": "opp_blka",
    "OPP_PF": "opp_pf",
    "OPP_PFD": "opp_pfd",
    "OPP_PTS": "opp_pts",
    "PLUS_MINUS": "plus_minus",
}


MeasureType = Literal["Base", "Advanced", "Four Factors", "Opponent"]


class NBAStatsFetcher:
    """
    Fetches NBA team statistics using the nba_api library.

    This class replaces the Scrapy-based spiders that were blocked by NBA.com's
    TLS fingerprinting. It provides the same functionality with a simpler interface.

    Includes exponential backoff and retry logic to handle NBA.com rate limiting.
    Supports resume capability - skips dates already in database.
    """

    FIRST_SEASON_START_YEAR = 1996

    # Rate limiting configuration
    # NBA.com can be sensitive to rapid requests - use conservative delays
    MIN_DELAY = 0.6  # Minimum delay between ALL requests (seconds) - prevents ban
    BASE_DELAY = 1.0  # Base delay for exponential backoff after errors
    MAX_DELAY = 60.0  # Maximum delay after repeated failures
    MAX_RETRIES = 3  # Maximum retries per request
    REQUEST_TIMEOUT = 30  # Timeout for individual requests (seconds)

    def __init__(self, view_data: bool = True, resume: bool = True):
        """
        Initialize the fetcher.

        Args:
            view_data: If True, print sample data after fetching
            resume: If True, skip dates already in database (default True)
        """
        self.view_data = view_data
        self.resume = resume
        self.Session = sessionmaker(bind=engine)
        self.errors = {
            "find_season_information": [],
            "api_requests": [],
            "saving": [],
        }
        self.stats = {
            "fetched": 0,
            "saved": 0,
            "skipped": 0,
            "retries": 0,
        }
        self._consecutive_errors = 0  # Track consecutive errors for backoff
        self._current_delay = self.BASE_DELAY

    def _setup_dates(self, dates_input: str) -> list[str]:
        """Convert date input to list of date strings."""
        if dates_input == "daily_update":
            yesterday = get_current_time() - timedelta(days=1)
            return [yesterday.strftime("%Y-%m-%d")]

        if dates_input == "all":
            return self._generate_all_dates()

        # Check if input is a season or multiple seasons (no dashes = season years)
        if "-" not in dates_input:
            seasons_list = [season.strip() for season in dates_input.split(",")]
            dates = []
            for season in seasons_list:
                dates.extend(self._generate_dates_for_season(int(season)))
            return dates

        # Individual dates
        return [date_str.strip() for date_str in dates_input.split(",")]

    def _generate_dates_for_season(self, season_start_year: int) -> list[str]:
        """Generate dates for a given NBA season, stopping at yesterday (no future dates).

        NBA Stats API returns no data for future dates, so we cap at yesterday to avoid
        wasted API calls. For historical seasons, returns all dates in the season.
        """
        if season_start_year < self.FIRST_SEASON_START_YEAR:
            raise ValueError(f"Season start year cannot be before {self.FIRST_SEASON_START_YEAR}")

        full_season = f"{season_start_year}-{season_start_year + 1}"
        season_start = datetime.strptime(
            config.NBA_IMPORTANT_DATES[full_season]["reg_season_start_date"], "%Y-%m-%d"
        )
        season_end = datetime.strptime(
            config.NBA_IMPORTANT_DATES[full_season]["postseason_end_date"], "%Y-%m-%d"
        )

        # Cap at yesterday - NBA Stats API has no data for future dates
        yesterday = get_current_time().replace(tzinfo=None) - timedelta(days=1)
        if season_end > yesterday:
            season_end = yesterday

        # If season hasn't started yet, return empty list
        if season_start > yesterday:
            return []

        return [dt.strftime("%Y-%m-%d") for dt in pd.date_range(start=season_start, end=season_end)]

    def _generate_all_dates(self) -> list[str]:
        """Generate all dates from first season to current."""
        current_year = get_current_year()
        all_dates = []

        for year in range(self.FIRST_SEASON_START_YEAR, current_year + 1):
            try:
                all_dates.extend(self._generate_dates_for_season(year))
            except KeyError:
                # Season not in config yet
                break

        return all_dates

    def _get_season_for_date(self, date_str: str) -> str | None:
        """Get the season string (e.g., '2023-2024') for a given date."""
        try:
            season_info = find_season_information(date_str)
            return season_info["season"]
        except Exception:
            return None

    def _get_existing_dates(self, table_class, season: str = None) -> set[str]:
        """Get dates already in database for a given table and season."""
        from sqlalchemy import text

        with self.Session() as session:
            try:
                table_name = table_class.__tablename__
                if season:
                    result = session.execute(
                        text(
                            f"SELECT DISTINCT substr(to_date, 1, 10) FROM {table_name} WHERE season = :season"
                        ),
                        {"season": season},
                    ).fetchall()
                else:
                    result = session.execute(
                        text(f"SELECT DISTINCT substr(to_date, 1, 10) FROM {table_name}")
                    ).fetchall()
                return {row[0] for row in result if row[0]}
            except Exception as e:
                print(f"Warning: could not get existing dates: {e}")
                return set()

    def _filter_dates_for_resume(
        self, date_list: list[str], table_class, season: str = None
    ) -> list[str]:
        """Filter out dates already in database if resume is enabled."""
        if not self.resume:
            return date_list

        existing = self._get_existing_dates(table_class, season)
        if existing:
            original_count = len(date_list)
            date_list = [d for d in date_list if d not in existing]
            skipped = original_count - len(date_list)
            if skipped > 0:
                self.stats["skipped"] += skipped
                # Silent - skipped count is included in summary
        return date_list

    def _wait_with_backoff(self):
        """Wait with current delay, applying jitter to avoid thundering herd."""
        jitter = random.uniform(0.1, 0.5)
        delay = self._current_delay + jitter
        time.sleep(delay)

    def _handle_success(self):
        """Reset backoff state after successful request."""
        self._consecutive_errors = 0
        self._current_delay = self.BASE_DELAY

    def _handle_error(self):
        """Increase backoff delay after error."""
        self._consecutive_errors += 1
        # Exponential backoff: delay doubles with each consecutive error
        self._current_delay = min(self.BASE_DELAY * (2**self._consecutive_errors), self.MAX_DELAY)

    def _fetch_team_stats(
        self,
        measure_type: MeasureType,
        season: str,
        season_type: str,
        date_to: str,
        date_from: str | None = None,
    ) -> pd.DataFrame | None:
        """
        Fetch team stats from NBA API with retry logic and exponential backoff.

        Args:
            measure_type: Type of stats (Base, Advanced, Four Factors, Opponent)
            season: Season in format "2024-25"
            season_type: "Regular Season" or "Playoffs"
            date_to: End date in MM/DD/YYYY format
            date_from: Start date in MM/DD/YYYY format (None for all games)

        Returns:
            DataFrame with team stats or None on error
        """
        last_error = None

        for attempt in range(self.MAX_RETRIES):
            try:
                # Always apply minimum delay to be respectful to NBA.com
                # On retries, add exponential backoff on top of min delay
                if attempt > 0:
                    self._wait_with_backoff()
                else:
                    time.sleep(self.MIN_DELAY)

                # Set timeout via nba_api's timeout parameter
                stats = leaguedashteamstats.LeagueDashTeamStats(
                    measure_type_detailed_defense=measure_type,
                    per_mode_detailed="PerGame",
                    season=season,
                    season_type_all_star=season_type,
                    date_to_nullable=date_to,
                    date_from_nullable=date_from,
                    timeout=self.REQUEST_TIMEOUT,
                )

                df = stats.get_data_frames()[0]

                # Success - reset backoff
                self._handle_success()
                return df

            except KeyError as e:
                # KeyError 'resultSet' or 'resultSets' means API returned no data
                # This happens for dates with no games (play-in period, etc.)
                # Don't retry - just return None
                if "resultSet" in str(e) or "resultSets" in str(e):
                    self._handle_success()  # Reset backoff - this isn't a real error
                    return None
                # Other KeyErrors should be retried
                last_error = e
                self._handle_error()
                self.stats["retries"] += 1
                if attempt < self.MAX_RETRIES - 1:
                    print(f"  Retry {attempt + 1}/{self.MAX_RETRIES} after error: {e}")
                    time.sleep(self._current_delay)

            except Exception as e:
                last_error = e
                self._handle_error()
                self.stats["retries"] += 1

                if attempt < self.MAX_RETRIES - 1:
                    print(f"  Retry {attempt + 1}/{self.MAX_RETRIES} after error: {e}")
                    # Additional wait before retry
                    time.sleep(self._current_delay)

        # All retries exhausted
        self.errors["api_requests"].append(
            {
                "measure_type": measure_type,
                "season": season,
                "date_to": date_to,
                "date_from": date_from,
                "error": str(last_error),
                "retries": self.MAX_RETRIES,
            }
        )
        print(f"API Error (after {self.MAX_RETRIES} retries): {last_error}")
        return None

    def _process_dataframe(
        self,
        df: pd.DataFrame,
        column_mapping: dict,
        to_date: str,
        season: str,
        season_type: str,
        games: str,
    ) -> list[dict]:
        """Process DataFrame and add metadata columns."""
        # Rename columns
        df = df.rename(columns=column_mapping)

        # Keep only mapped columns
        df = df[[col for col in column_mapping.values() if col in df.columns]]

        # Add metadata
        df["to_date"] = datetime.strptime(to_date, "%m/%d/%Y").date()
        df["season"] = season
        df["season_type"] = season_type
        df["games"] = games

        # Apply team name mapping (team_name_mapper is a function)
        df["team_name"] = df["team_name"].map(config.team_name_mapper)

        return df.to_dict("records")

    def _save_to_database(self, records: list[dict], table_class) -> int:
        """
        Save records to database using upsert (SQLite INSERT OR REPLACE).

        Uses ON CONFLICT DO UPDATE to replace existing records with new data.
        This ensures we always have the latest stats (highest GP) when the same
        date is fetched multiple times.

        Returns:
            Number of records saved
        """
        if not records:
            return 0

        table = table_class.__table__

        with self.Session() as session:
            try:
                # Get non-primary key columns for the SET clause
                pk_cols = [c.name for c in table.primary_key.columns]
                update_cols = {c.name: c for c in table.columns if c.name not in pk_cols}

                # Use SQLite upsert (ON CONFLICT DO UPDATE)
                stmt = sqlite_insert(table).values(records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=pk_cols, set_={k: stmt.excluded[k] for k in update_cols.keys()}
                )

                result = session.execute(stmt)
                session.commit()

                return result.rowcount if result.rowcount else len(records)

            except Exception as e:
                self.errors["saving"].append({"error": str(e), "records": len(records)})
                print(f"Save Error: {e}")
                session.rollback()
                return 0

    def fetch_stats_for_date(
        self,
        date_str: str,
        measure_type: MeasureType,
        column_mapping: dict,
        table_class,
        save_data: bool = False,
    ) -> list[dict]:
        """
        Fetch stats for a specific date.

        Args:
            date_str: Date in YYYY-MM-DD format
            measure_type: Type of stats to fetch
            column_mapping: Column name mapping
            table_class: SQLAlchemy table class for saving
            save_data: Whether to save to database

        Returns:
            List of record dictionaries
        """
        all_records = []

        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            season_info = find_season_information(date_str)
        except Exception as e:
            self.errors["find_season_information"].append({"date": date_str, "error": str(e)})
            print(f"Season info error for {date_str}: {e}")
            return []

        season_api = convert_season_to_short(season_info["season"])  # Short format for NBA API
        season_db = season_info["season"]  # Long format for database storage (e.g., "2023-2024")
        season_type = season_info["season_type"]
        to_date = date.strftime("%m/%d/%Y")

        # Calculate L2W (last 2 weeks) date
        l2w_date = (date - timedelta(days=14)).strftime("%m/%d/%Y")

        # Fetch "all games" stats (no date_from filter)
        df_all = self._fetch_team_stats(
            measure_type=measure_type,
            season=season_api,
            season_type=season_type,
            date_to=to_date,
            date_from=None,
        )

        if df_all is not None and not df_all.empty:
            records = self._process_dataframe(
                df_all, column_mapping, to_date, season_db, season_type, "all"
            )
            all_records.extend(records)
            self.stats["fetched"] += len(records)

        # Fetch "last 2 weeks" stats
        df_l2w = self._fetch_team_stats(
            measure_type=measure_type,
            season=season_api,
            season_type=season_type,
            date_to=to_date,
            date_from=l2w_date,
        )

        if df_l2w is not None and not df_l2w.empty:
            records = self._process_dataframe(
                df_l2w, column_mapping, to_date, season_db, season_type, "l2w"
            )
            all_records.extend(records)
            self.stats["fetched"] += len(records)

        # Save to database if requested
        if save_data and all_records:
            saved = self._save_to_database(all_records, table_class)
            self.stats["saved"] += saved

        return all_records

    def fetch_traditional(self, dates: str, save_data: bool = False) -> list[dict]:
        """Fetch traditional stats for given dates."""
        date_list = self._setup_dates(dates)
        # Determine season for resume filtering (use first date)
        season = self._get_season_for_date(date_list[0]) if date_list else None
        date_list = self._filter_dates_for_resume(
            date_list, NbastatsGeneralTraditionalTable, season
        )
        all_records = []

        if not date_list:
            return all_records

        for date_str in tqdm(date_list, desc="Traditional", unit="date"):
            records = self.fetch_stats_for_date(
                date_str=date_str,
                measure_type="Base",
                column_mapping=TRADITIONAL_COLUMNS,
                table_class=NbastatsGeneralTraditionalTable,
                save_data=save_data,
            )
            all_records.extend(records)

        return all_records

    def fetch_advanced(self, dates: str, save_data: bool = False) -> list[dict]:
        """Fetch advanced stats for given dates."""
        date_list = self._setup_dates(dates)
        season = self._get_season_for_date(date_list[0]) if date_list else None
        date_list = self._filter_dates_for_resume(date_list, NbastatsGeneralAdvancedTable, season)
        all_records = []

        if not date_list:
            return all_records

        for date_str in tqdm(date_list, desc="Advanced", unit="date"):
            records = self.fetch_stats_for_date(
                date_str=date_str,
                measure_type="Advanced",
                column_mapping=ADVANCED_COLUMNS,
                table_class=NbastatsGeneralAdvancedTable,
                save_data=save_data,
            )
            all_records.extend(records)

        return all_records

    def fetch_fourfactors(self, dates: str, save_data: bool = False) -> list[dict]:
        """Fetch four factors stats for given dates."""
        date_list = self._setup_dates(dates)
        season = self._get_season_for_date(date_list[0]) if date_list else None
        date_list = self._filter_dates_for_resume(
            date_list, NbastatsGeneralFourfactorsTable, season
        )
        all_records = []

        if not date_list:
            return all_records

        for date_str in tqdm(date_list, desc="FourFactors", unit="date"):
            records = self.fetch_stats_for_date(
                date_str=date_str,
                measure_type="Four Factors",
                column_mapping=FOURFACTORS_COLUMNS,
                table_class=NbastatsGeneralFourfactorsTable,
                save_data=save_data,
            )
            all_records.extend(records)

        return all_records

    def fetch_opponent(self, dates: str, save_data: bool = False) -> list[dict]:
        """Fetch opponent stats for given dates."""
        date_list = self._setup_dates(dates)
        season = self._get_season_for_date(date_list[0]) if date_list else None
        date_list = self._filter_dates_for_resume(date_list, NbastatsGeneralOpponentTable, season)
        all_records = []

        if not date_list:
            return all_records

        for date_str in tqdm(date_list, desc="Opponent", unit="date"):
            records = self.fetch_stats_for_date(
                date_str=date_str,
                measure_type="Opponent",
                column_mapping=OPPONENT_COLUMNS,
                table_class=NbastatsGeneralOpponentTable,
                save_data=save_data,
            )
            all_records.extend(records)

        return all_records

    def fetch_all_stats(self, dates: str, save_data: bool = False) -> dict:
        """
        Fetch all stat types for given dates.

        Args:
            dates: Date specification (see module docstring)
            save_data: Whether to save to database

        Returns:
            Dictionary with records for each stat type
        """
        results = {
            "traditional": self.fetch_traditional(dates, save_data),
            "advanced": self.fetch_advanced(dates, save_data),
            "fourfactors": self.fetch_fourfactors(dates, save_data),
            "opponent": self.fetch_opponent(dates, save_data),
        }

        self._print_summary()

        return results

    def _print_summary(self):
        """Print concise summary of fetch operation."""
        total_errors = (
            len(self.errors["find_season_information"])
            + len(self.errors["api_requests"])
            + len(self.errors["saving"])
        )

        # Build concise summary
        parts = []
        if self.stats["fetched"] > 0:
            parts.append(f"fetched {self.stats['fetched']}")
        if self.stats["saved"] > 0:
            parts.append(f"saved {self.stats['saved']}")
        if self.stats["skipped"] > 0:
            parts.append(f"skipped {self.stats['skipped']} (cached)")
        if total_errors > 0:
            parts.append(f"{total_errors} errors")

        if parts:
            print(f"  {', '.join(parts)}")
        else:
            print("  No new data to fetch")

        # Only show error details if there are errors
        if self.errors["api_requests"]:
            for err in self.errors["api_requests"][:3]:
                print(f"    API error: {err['measure_type']} {err['date_to']}: {err['error']}")


if __name__ == "__main__":
    # Example usage
    fetcher = NBAStatsFetcher()

    # Test with a single date
    print("Testing NBA Stats Fetcher...")
    records = fetcher.fetch_traditional(dates="2025-01-15", save_data=False)

    if records:
        df = pd.DataFrame(records)
        print("\nSample data:")
        print(df[["team_name", "to_date", "games", "w", "l", "pts"]].head(10))
    else:
        print("No records fetched")
