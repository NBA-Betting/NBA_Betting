import sys

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append("../../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

# IMPORT TABLES HERE
from src.database_orm import InpredictableWPATable

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 200)


class BasePipeline:
    """
    The BasePipeline class is a Scrapy pipeline that you can use as a base for
    creating custom pipelines to process, clean, and verify data, and then save
    it to a PostgreSQL database.
    """

    ITEM_CLASS = None

    def __init__(self, save_data=False, view_data=True):
        self.engine = create_engine(
            f"postgresql://postgres:{RDS_PASSWORD}@{RDS_ENDPOINT}/nba_betting"
        )
        self.Session = sessionmaker(bind=self.engine)
        self.nba_data = []
        self.save_data = save_data
        self.view_data = view_data
        self.clean_errors = 0
        self.verify_errors = 0

    def process_item(self, item, spider):
        """
        This method is called for each item that is scraped. It cleans and verifies
        the item before appending it to the list of scraped data.

        Args:
            item (dict): The scraped item.
            spider (scrapy.Spider): The spider that scraped the item.

        Returns:
            dict: The processed item.
        """
        self.clean_data(item)
        self.verify_data(item)
        self.nba_data.append(item)
        return item

    def clean_data(self, item):
        """
        This method is responsible for cleaning the data. It should be
        overridden in the subclass to implement custom cleaning logic.

        Args:
            item (dict): The item to clean.

        Returns:
            bool: True if cleaning is successful, False otherwise.
        """
        try:
            # Common cleaning logic
            pass
            return True
        except Exception as e:
            self.clean_errors += 1
            return False

    def verify_data(self, item):
        """
        This method is responsible for verifying the data. It should be
        overridden in the subclass to implement custom verification logic.

        Args:
            item (dict): The item to verify.

        Returns:
            bool: True if verification is successful, False otherwise.
        """
        try:
            # Common verification logic
            pass
            return True
        except Exception as e:
            self.verify_errors += 1
            return False

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
            print("Data successfully saved into nba_betting database.")
        except Exception as e:
            print(f"Error: Unable to insert data into the RDS table. Details: {str(e)}")
            session.rollback()
        finally:
            session.close()
            self.nba_data = []

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
        This method is called when the spider finishes scraping. It displays
        the data (if view_data is True), saves the data to the database (if save_to_database is True),
        and reports any errors that occurred during cleaning and verification.
            Args:
        spider (scrapy.Spider): The spider that has finished scraping.
        """

        if spider.view_data:
            self.display_data()
        if spider.save_data:
            self.save_to_database()

        print(f"Number of cleaning errors: {self.clean_errors}")
        print(f"Number of verification errors: {self.verify_errors}")

        if len(self.nba_data) > 0:
            print(
                f"Percentage of successful items: {((len(self.nba_data) - (self.clean_errors + self.verify_errors)) / len(self.nba_data)) * 100}%"
            )


# ADD NEW PIPELINES HERE


class InpredictableWPAPipeline(BasePipeline):
    ITEM_CLASS = InpredictableWPATable
