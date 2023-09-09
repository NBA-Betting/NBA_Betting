import os
import sys
from datetime import datetime

import pandas as pd
import pytz
from dotenv import load_dotenv
from scrapy import Spider
from scrapy.spiders import Spider
from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))

import config

load_dotenv()
RDS_ENDPOINT = os.environ.get("RDS_ENDPOINT")
RDS_PASSWORD = os.environ.get("RDS_PASSWORD")
ZYTE_API_KEY = os.environ.get("ZYTE_API_KEY")


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

        # generate dates from start to end
        dates = [
            dt.strftime("%Y-%m-%d")
            for dt in pd.date_range(start=season_start_date, end=season_end_date)
        ]

        return dates

    def _generate_all_dates(self):
        # Get the current year
        current_year = datetime.now(pytz.timezone("America/Denver")).year

        # Accumulates all dates from first_season_start_year to current_year
        all_dates = []

        for year in range(self.first_season_start_year, current_year + 1):
            # Get all dates for the season starting with this year
            season_dates = self._generate_dates_for_season(year)
            # Append these dates to the list of all dates
            all_dates.extend(season_dates)

        return all_dates

    def start_requests(self):
        pass

    def parse(self, response):
        pass


class BaseSpiderZyte(BaseSpider):
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
        },
        "REQUEST_FINGERPRINTER_CLASS": "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        "ZYTE_API_KEY": ZYTE_API_KEY,
        "ZYTE_API_TRANSPARENT_MODE": True,
        "ZYTE_API_ENABLED": True,
    }


class BasePipeline:
    """
    The BasePipeline class is a Scrapy pipeline that you can use as a base for
    creating custom pipelines to process, clean, and verify data, and then save
    it to a PostgreSQL database.
    """

    engine = create_engine(
        f"postgresql://postgres:{RDS_PASSWORD}@{RDS_ENDPOINT}/nba_betting"
    )
    ITEM_CLASS = None

    @classmethod
    def from_crawler(cls, crawler):
        # Get the spider instance from the crawler
        spider = crawler.spider
        # Pass the spider instance to the pipeline
        return cls(spider)

    def __init__(self, spider):
        self.Session = sessionmaker(bind=self.engine)
        self.save_data = spider.save_data
        self.view_data = spider.view_data

        self.nba_data = []
        self.total_items = 0
        self.errors = spider.errors

    def process_item(self, item, spider):
        """
        This method is called for each item that is scraped. It cleans organizes, and verifies
        the item before appending it to the list of scraped data.

        Args:
            item (dict): The scraped item.
            spider (scrapy.Spider): The spider that scraped the item.

        Returns:
            dict: The processed item.
        """
        try:
            # Processing logic here
            self.nba_data.append(item)
        except Exception as e:
            self.errors["processing"].append([e, item])

        if (
            len(self.nba_data) % 1000 == 0
        ):  # If the length of self.nba_data is a multiple of 1000
            self.save_to_database()  # Save to the database

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
        stmt = pg_insert(table).values(rows)

        # Handle conflict with composite primary key
        stmt = stmt.on_conflict_do_nothing(index_elements=pks)

        with self.Session() as session:
            try:
                initial_count = session.query(
                    self.ITEM_CLASS
                ).count()  # Count before insert
                session.execute(stmt)
                session.commit()
                final_count = session.query(
                    self.ITEM_CLASS
                ).count()  # Count after insert

                # Count the number of IntegrityErrors
                integrity_error_count = len(rows) - (final_count - initial_count)
                self.errors["saving"]["integrity_error_count"] += integrity_error_count

                self.total_items += len(rows)
                self.nba_data = []  # Empty the list after saving the data
            except IntegrityError as ie:
                print(f"Integrity Error: Unable to insert data. Details: {str(ie)}")
                self.errors["saving"]["integrity"].append(ie)
                sys.exit(1)

            except Exception as e:
                print(
                    f"Error: Unable to insert data into the RDS table. Details: {str(e)}"
                )
                self.errors["saving"]["other"].append(e)
                sys.exit(1)
            finally:
                integrity_error_pct = round(
                    self.errors["saving"]["integrity_error_count"]
                    / self.total_items
                    * 100,
                    2,
                )
                non_integrity_error_count = (
                    len(self.errors["saving"]["other"])
                    + len(self.errors["processing"])
                    + len(self.errors["find_season_information"])
                )

                print(f"Inserted {len(rows) - integrity_error_count} rows successfully.")
                print(f"Total items inserted: {self.total_items}")
                print(
                    f"Total integrity errors: {self.errors['saving']['integrity_error_count']} {integrity_error_pct}%"
                )
                print(f"Total other errors: {non_integrity_error_count}")

    def display_data(self):
        """
        This method displays a sample of the scraped data, along with its info
        and the total number of items scraped.
        """
        pd.set_option("display.max_columns", 100)
        df = pd.DataFrame(self.nba_data)
        print("Sample of scraped data:")
        print(df.head(20))
        print(df.info())
        print("Number of items scraped:", len(self.nba_data))

    def close_spider(self, spider):
        """
        This method is called when the spider finishes scraping. It processes
        the dataset, displays the data (if view_data is True), saves the data
        to the database (if save_to_database is True), and reports any errors
        that occurred during cleaning and verification.
        Args:
        spider (scrapy.Spider): The spider that has finished scraping.
        """

        # Process the dataset
        self.process_dataset()

        if spider.view_data:
            self.display_data()
        if spider.save_data:
            self.save_to_database()

        print("\nErrors")
        print("======")
        print(">>>>> Finding season information errors:")
        for error in self.errors["find_season_information"]:
            print(error)
        print(">>>>> Processing errors:")
        for error in self.errors["processing"]:
            print(error)
        print(">>>>> Saving errors:")
        for error in self.errors["saving"]["other"]:
            print(error)

        integrity_error_pct = round(
            self.errors["saving"]["integrity_error_count"] / self.total_items * 100,
            2,
        )
        non_integrity_error_count = (
            len(self.errors["saving"]["other"])
            + len(self.errors["processing"])
            + len(self.errors["find_season_information"])
        )

        print("\nOverall Results")
        print("===============")
        print("Total items scraped:", self.total_items)
        print(
            f"Total integrity errors: {self.errors['saving']['integrity_error_count']} {integrity_error_pct}%"
        )
        print(f"Total other errors: {non_integrity_error_count}")


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
