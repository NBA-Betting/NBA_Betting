import logging
import os
import sys
from datetime import date, timedelta

# IMPORT SPIDERS HERE
from data_sources.spiders.fivethirtyeight_player_spider import (
    FivethirtyeightPlayerSpider,
)
from data_sources.spiders.inpredictable_wpa_spider import InpredictableWPASpider
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def run_spider(spider_class, **spider_args):
    try:
        process.crawl(spider_class, **spider_args)
        process.start()  # This will block and run the spiders
        print(f"{spider_class.__name__} completed.")

        # Print failed dates
        print("Failed dates:")
        for reason, dates in spider_class.failed_dates.items():
            if dates:
                print(f"{reason}: {', '.join(dates)}")

        # You can return the number of records added to the database here
        # if you can access it from the spider or pipeline
    except Exception as e:
        print(f"{spider_class.__name__} failed. Reason: {e}")
        return spider_class.__name__, e

    return None


if __name__ == "__main__":
    # Set up logging to only show errors
    logging.getLogger("scrapy").setLevel(logging.ERROR)

    # Get Scrapy project settings
    settings = get_project_settings()

    # Set up the CrawlerProcess with the project settings
    process = CrawlerProcess(settings)

    # Run each spider and handle errors
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    spiders_to_run = [
        (
            InpredictableWPASpider,
            {"save_data": True, "view_data": False, "dates": yesterday},
        ),
        (
            FivethirtyeightPlayerSpider,
            {"save_data": True, "view_data": False, "dates": "daily_update"},
        ),
        # ADD MORE SPIDERS HERE
    ]

    failed_spiders = []

    for spider_class, spider_args in spiders_to_run:
        spider_result = run_spider(spider_class, **spider_args)
        if spider_result:
            failed_spiders.append(spider_result)

    if failed_spiders:
        print("\nThe following spiders failed:")
        for spider_name, exception in failed_spiders:
            print(f"{spider_name}: {exception}")
    else:
        print("\nAll spiders completed successfully.")
