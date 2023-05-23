import sys

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append("../../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

# IMPORT TABLES HERE
from src.database_orm import (
    FivethirtyeightPlayerTable,
    InpredictableWPATable,
    Nba2kPlayerTable,
    NbaStatsBoxscoresAdvAdvancedTable,
    NbaStatsBoxscoresAdvMiscTable,
    NbaStatsBoxscoresAdvScoringTable,
    NbaStatsBoxscoresAdvTraditionalTable,
    NbaStatsBoxscoresAdvUsageTable,
    NbaStatsBoxscoresTraditionalTable,
    NbaStatsGameResultsTable,
    NbaStatsPlayerGeneralAdvancedTable,
    NbaStatsPlayerGeneralDefenseTable,
    NbaStatsPlayerGeneralMiscTable,
    NbaStatsPlayerGeneralOpponentTable,
    NbaStatsPlayerGeneralScoringTable,
    NbaStatsPlayerGeneralTraditionalTable,
    NbaStatsPlayerGeneralUsageTable,
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 200)


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
        """
        This method is responsible for saving the data to the PostgreSQL database
        using the ITEM_CLASS attribute, which should be set in the subclass.

        Raises:
            Exception: If there's an error while inserting data into the RDS table.
        """
        session = self.Session()
        try:
            for item in self.nba_data:
                data = self.ITEM_CLASS(**item)
                session.add(data)
            session.commit()
            print(
                f"Data successfully saved into nba_betting database. {len(self.nba_data)} records inserted."
            )
        except Exception as e:
            print(f"Error: Unable to insert data into the RDS table. Details: {str(e)}")
            session.rollback()
        finally:
            session.close()
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


# ADD NEW PIPELINES HERE
class FivethirtyeightPlayerPipeline(BasePipeline):
    ITEM_CLASS = FivethirtyeightPlayerTable

    def process_dataset(self):
        # Remove duplicates and update records
        df = pd.DataFrame(self.nba_data)
        df.sort_values(
            by=["player_id", "season", "priority"], ascending=False, inplace=True
        )
        df.drop_duplicates(subset=["player_id", "season"], keep="first", inplace=True)

        # Remove the "priority" column as it's not needed anymore
        df.drop(columns=["priority"], inplace=True)

        # Convert the DataFrame back to a list of dictionaries
        self.nba_data = df.to_dict("records")


class InpredictableWPAPipeline(BasePipeline):
    ITEM_CLASS = InpredictableWPATable


# class Nba2kPlayerPipeline(BasePipeline):
#     ITEM_CLASS = Nba2kPlayerTable


class NbaStatsGameResultsPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsGameResultsTable


class NbaStatsPlayerGeneralTraditionalPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralTraditionalTable


class NbaStatsPlayerGeneralAdvancedPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralAdvancedTable


class NbaStatsPlayerGeneralMiscPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralMiscTable


class NbaStatsPlayerGeneralUsagePipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralUsageTable


class NbaStatsPlayerGeneralScoringPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralScoringTable


class NbaStatsPlayerGeneralOpponentPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralOpponentTable


class NbaStatsPlayerGeneralDefensePipeline(BasePipeline):
    ITEM_CLASS = NbaStatsPlayerGeneralDefenseTable


class NbaStatsBoxscoresTraditionalPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresTraditionalTable


class NbaStatsBoxscoresAdvTraditionalPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresAdvTraditionalTable


class NbaStatsBoxscoresAdvAdvancedPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresAdvAdvancedTable


class NbaStatsBoxscoresAdvMiscPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresAdvMiscTable


class NbaStatsBoxscoresAdvScoringPipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresAdvScoringTable


class NbaStatsBoxscoresAdvUsagePipeline(BasePipeline):
    ITEM_CLASS = NbaStatsBoxscoresAdvUsageTable
