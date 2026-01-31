"""Data source utilities: date range generation, database upserts."""

import sys
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from src import config
from src.database import engine
from src.utils.timezone import get_current_time, get_current_year

# NBA Stats API headers - updated to match modern browser fingerprints
# These headers are required for NBA.com's API to accept requests
# Based on nba_api library (https://github.com/swar/nba_api)
NBA_STATS_HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://stats.nba.com/",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}


# Import Spider for BaseSpider class
from scrapy import Spider


class BaseSpider(Spider):
    name = "<data_source_name>_spider>"  # Update
    pipeline_name = "<pipeline_name>"  # Update
    project_section = "<project_section>"  # Update
    first_season_start_year = 1976  # Update: First season of data source
    errors = {
        "find_season_information": [],
        "processing": [],
        "saving": {"integrity_error_count": 0, "other": []},
    }

    def __init__(self, dates, save_data=False, view_data=True, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)

        # Verify inputs
        self.save_data = self.verify_inputs(save_data)
        self.view_data = self.verify_inputs(view_data)
        self.dates = self.setup_dates(dates)

    @classmethod
    def create_pipeline_settings(cls, project_section, pipeline_name):
        pipeline_path = f"{project_section}.pipelines.{pipeline_name}"
        return {
            "ITEM_PIPELINES": {pipeline_path: 300},
        }

    def setup_dates(self, dates_input):
        # Validate the input
        if not isinstance(dates_input, str):
            raise TypeError(f"dates_input should be str, but got {type(dates_input)}")

        # For daily_update, return as is
        if dates_input == "daily_update":
            return "daily_update"

        # For all, return all dates from the first season till now
        if dates_input == "all":
            return self._generate_all_dates()

        # Check if input is a season or multiple seasons
        if "-" not in dates_input:
            seasons_list = [season.strip() for season in dates_input.split(",")]
            # Generate all dates in those seasons
            dates = []
            for season in seasons_list:
                dates.extend(self._generate_dates_for_season(int(season)))
            return dates

        # Assuming the remaining input would be individual dates
        dates_list = dates_input.split(",")
        for date_str in dates_list:
            try:
                # Validate each date
                datetime.strptime(date_str.strip(), "%Y-%m-%d")
            except ValueError:
                raise ValueError(
                    f"Invalid date format: {date_str}. Date format should be 'YYYY-MM-DD'"
                )

        return [date_str.strip() for date_str in dates_list]

    @staticmethod
    def verify_inputs(input):
        if isinstance(input, bool):
            return input

        if isinstance(input, str):
            lower_str = input.lower()
            if lower_str in ["t", "f", "true", "false"]:
                return lower_str in ["t", "true"]

        raise ValueError(
            "Invalid input. Please enter a boolean or a string 't', 'f', 'true', 'false'."
        )

    def _generate_dates_for_season(self, season_start_year):
        """Generate dates for a given NBA season, stopping at tomorrow (no far-future dates).

        Covers.com can show tomorrow's scheduled games, so we cap at tomorrow to get
        upcoming games while avoiding requests for dates with no data. For historical
        seasons that have fully completed, returns all dates in the season.
        """
        if season_start_year < self.first_season_start_year:
            raise ValueError(
                "Season start year cannot be before the first season of the data source."
            )

        full_season = f"{season_start_year}-{str(season_start_year + 1)}"
        season_start_date = datetime.strptime(
            config.NBA_IMPORTANT_DATES[full_season]["reg_season_start_date"], "%Y-%m-%d"
        )
        season_end_date = datetime.strptime(
            config.NBA_IMPORTANT_DATES[full_season]["postseason_end_date"], "%Y-%m-%d"
        )

        # Cap at tomorrow - Covers can show scheduled games for tomorrow but not beyond
        tomorrow = get_current_time().replace(tzinfo=None) + timedelta(days=1)
        if season_end_date > tomorrow:
            season_end_date = tomorrow

        # If season hasn't started yet, return empty list
        if season_start_date > tomorrow:
            return []

        # generate dates from start to end
        dates = [
            dt.strftime("%Y-%m-%d")
            for dt in pd.date_range(start=season_start_date, end=season_end_date)
        ]

        return dates

    def _generate_all_dates(self):
        # Get the current year
        current_year = get_current_year()

        # Accumulates all dates from first_season_start_year to current_year
        all_dates = []

        for year in range(self.first_season_start_year, current_year + 1):
            # Get all dates for the season starting with this year
            season_dates = self._generate_dates_for_season(year)
            # Append these dates to the list of all dates
            all_dates.extend(season_dates)

        return all_dates

    async def start(self):
        """Generate initial requests (Scrapy 2.13+ async API). Override in subclass."""
        pass

    def parse(self, response):
        """Parse response. Override in subclass."""
        pass


class BasePipeline:
    """
    The BasePipeline class is a Scrapy pipeline that you can use as a base for
    creating custom pipelines to process, clean, and verify data, and then save
    it to a SQLite database.
    """

    # Use centralized engine from src/database.py
    ITEM_CLASS = None

    @classmethod
    def from_crawler(cls, crawler):
        # Store crawler for later access to spider
        instance = cls(crawler)
        return instance

    def __init__(self, crawler):
        self.crawler = crawler
        self.Session = sessionmaker(bind=engine)

        self.nba_data = []
        self.scraped_items = 0
        self.saved_items = 0

        # These will be set when spider opens
        self.save_data = False
        self.view_data = False
        self.errors = {
            "find_season_information": [],
            "processing": [],
            "saving": {"integrity_error_count": 0, "other": []},
        }

    def open_spider(self):
        """Called when spider opens - initialize spider-dependent attributes.

        Note: Scrapy 2.13+ deprecates passing spider as argument. Access via
        self.crawler.spider instead.
        """
        spider = self.crawler.spider
        self.save_data = spider.save_data
        self.view_data = spider.view_data
        self.errors = spider.errors

    def process_item(self, item):
        """
        Process each scraped item. Cleans, organizes, and verifies
        the item before appending it to the list of scraped data.

        Args:
            item (dict): The scraped item.

        Returns:
            dict: The processed item.
        """
        # Increment the count of total items processed.
        self.scraped_items += 1

        try:
            # Processing logic here
            self.nba_data.append(item)

            # Check if the processed items have reached a certain batch size and if saving data is enabled
            if self.save_data and (len(self.nba_data) % 1000 == 0):
                self.save_to_database()  # Save the batch to the database

        except Exception as e:
            # If an error occurs during processing, log the error along with the associated item.
            self.errors["processing"].append([e, item])

        # Return the item dict, which is useful for potential chaining of item pipelines or debugging.
        return item

    def process_dataset(self):
        """
        This method can be overridden by subclasses to process the full dataset
        once all items have been processed individually.
        """
        pass

    def save_to_database(self):
        table = self.ITEM_CLASS.__table__
        mapper = inspect(self.ITEM_CLASS)

        pks = [column.key for column in mapper.primary_key]  # Get list of primary keys

        rows = [dict(row) for row in self.nba_data]  # Rows to be inserted
        stmt = sqlite_insert(table).values(rows)

        # Handle conflict with composite primary key (SQLite syntax)
        stmt = stmt.on_conflict_do_nothing(index_elements=pks)

        with self.Session() as session:
            try:
                initial_count = session.query(self.ITEM_CLASS).count()  # Count before insert
                session.execute(stmt)
                session.commit()
                final_count = session.query(self.ITEM_CLASS).count()  # Count after insert

                # Count the number of IntegrityErrors (duplicates ignored)
                integrity_error_count = len(rows) - (final_count - initial_count)
                self.errors["saving"]["integrity_error_count"] += integrity_error_count

                self.saved_items += len(rows) - integrity_error_count
                self.nba_data = []  # Empty the list after saving the data

            except IntegrityError as ie:
                print(f"  ERROR: Database integrity error: {str(ie)}")
                self.errors["saving"]["integrity"].append(ie)
                sys.exit(1)

            except Exception as e:
                print(f"  ERROR: Database save failed: {str(e)}")
                self.errors["saving"]["other"].append(e)
                raise e

    def display_data(self):
        """Display a brief summary of scraped data (disabled by default for cleaner output)."""
        # Verbose output disabled for cleaner terminal output
        # Uncomment below for debugging:
        # pd.set_option("display.max_columns", 100)
        # df = pd.DataFrame(self.nba_data)
        # print("Sample of scraped data:")
        # print(df.head(10))
        pass

    def close_spider(self):
        """
        Called when spider finishes scraping. Processes the dataset,
        displays data, saves to database, and reports errors.

        Note: Scrapy 2.13+ deprecates passing spider as argument.
        """
        # Process the dataset
        self.process_dataset()

        if self.view_data:
            self.display_data()
        if self.save_data:
            self.save_to_database()

        # Calculate error counts
        error_count = (
            len(self.errors["saving"]["other"])
            + len(self.errors["processing"])
            + len(self.errors["find_season_information"])
        )

        # Concise summary
        if error_count == 0:
            print(f"  Scraped {self.scraped_items} items, saved {self.saved_items}")
        else:
            print(
                f"  Scraped {self.scraped_items} items, saved {self.saved_items}, {error_count} errors"
            )
            # Only show error details if there are errors
            for error in self.errors["find_season_information"]:
                print(f"    Season error: {error}")
            for error in self.errors["processing"]:
                print(f"    Processing error: {error}")
            for error in self.errors["saving"]["other"]:
                print(f"    Save error: {error}")


def convert_season_to_long(season):
    # Split the input on the dash
    parts = season.split("-")

    # The first part remains unchanged
    start_year = parts[0]

    # For the second part, we prepend the first two digits of the first part
    # if the second part is less than 80 (assuming 2000 as the turning point),
    # otherwise we prepend '19'
    end_year = "20" + parts[1] if int(parts[1]) < 50 else "19" + parts[1]

    return start_year + "-" + end_year


def convert_season_to_short(season_str):
    start_year, end_year = season_str.split("-")
    return start_year + "-" + end_year[2:]


def convert_date_format_1(date_str):
    """Converts date from format 'MM/DD/YYYY' to 'YYYY-MM-DD'"""
    date_obj = datetime.strptime(date_str, "%m/%d/%Y")
    return date_obj.strftime("%Y-%m-%d")
