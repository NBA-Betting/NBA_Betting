import os
import re
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from scrapy.spiders import Spider
from sqlalchemy import create_engine
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

    def __init__(
        self, dates, use_zyte=False, save_data=False, view_data=True, *args, **kwargs
    ):
        super(BaseSpider, self).__init__(*args, **kwargs)

        # Verify inputs
        self.save_data = self.verify_inputs(save_data)
        self.view_data = self.verify_inputs(view_data)
        self.use_zyte = self.verify_inputs(use_zyte)
        self.dates = self.setup_dates(dates)

        # Add pipeline to custom settings
        pipeline_path = f"{self.project_section}.pipelines.{self.pipeline_name}"
        self.custom_settings = {
            "ITEM_PIPELINES": {pipeline_path: 300},
        }

        # Add Zyte API settings if necessary
        if use_zyte:
            self.custom_settings.update(
                {
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
            )

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
        seasons_list = re.findall(r"\b\d{4}\b", dates_input)
        if seasons_list:
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
        current_year = datetime.now().year

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


class BasePipeline:
    """
    The BasePipeline class is a Scrapy pipeline that you can use as a base for
    creating custom pipelines to process, clean, and verify data, and then save
    it to a PostgreSQL database.
    """

    ITEM_CLASS = None

    @classmethod
    def from_crawler(cls, crawler):
        # Get the spider instance from the crawler
        spider = crawler.spider
        # Pass the spider instance to the pipeline
        return cls(spider)

    def __init__(self, spider):
        self.engine = create_engine(
            f"postgresql://postgres:{RDS_PASSWORD}@{RDS_ENDPOINT}/nba_betting"
        )
        self.Session = sessionmaker(bind=self.engine)
        self.nba_data = []
        self.save_data = spider.save_data
        self.view_data = spider.view_data
        self.processing_errors = 0

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
            if (
                len(self.nba_data) % 1000 == 0
            ):  # If the length of self.nba_data is a multiple of 1000
                self.save_to_database()  # Save to the database
            return item
        except Exception as e:
            self.processing_errors += 1
            return False

    def process_dataset(self):
        """
        This method can be overridden by subclasses to process the full dataset
        once all items have been processed individually.
        """
        pass

    def save_to_database(self):
        with self.Session() as session:
            for item in self.nba_data:
                data = self.ITEM_CLASS(**item)
                try:
                    session.add(data)
                    session.commit()
                except IntegrityError:
                    session.rollback()
                    print(
                        f"Error: A record with this primary key already exists. Skipping this item."
                    )
                except Exception as e:
                    session.rollback()
                    print(
                        f"Error: Unable to insert data into the RDS table. Details: {str(e)}"
                    )
                    self.processing_errors += (
                        1  # Increment processing errors if any error occurs
                    )
        self.nba_data = []  # Empty the list after saving the data

    def display_data(self):
        """
        This method displays a sample of the scraped data, along with its info
        and the total number of items scraped.
        """
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

        print(f"Number of processing errors: {self.processing_errors}")

        if len(self.nba_data) > 0:
            success_rate = (
                (len(self.nba_data) - self.processing_errors) / len(self.nba_data) * 100
            )
            print(f"Percentage of successful items: {success_rate:.2f}%")


def find_season_information(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    for season, info in config.NBA_IMPORTANT_DATES.items():
        reg_season_start_date = datetime.strptime(
            info["reg_season_start_date"], "%Y-%m-%d"
        )
        postseason_end_date = datetime.strptime(info["postseason_end_date"], "%Y-%m-%d")

        if reg_season_start_date <= date_obj <= postseason_end_date:
            year1, year2 = season.split("-")
            reg_season_start_date = info["reg_season_start_date"]
            reg_season_end_date = info["reg_season_end_date"]
            postseason_start_date = info["postseason_start_date"]
            postseason_end_date = info["postseason_end_date"]

            return {
                "date": date_str,
                "season": season,
                "year1": year1,
                "year2": year2,
                "reg_season_start_date": reg_season_start_date,
                "reg_season_end_date": reg_season_end_date,
                "postseason_start_date": postseason_start_date,
                "postseason_end_date": postseason_end_date,
            }
        else:
            raise ValueError("Could not find season information for this date.")
