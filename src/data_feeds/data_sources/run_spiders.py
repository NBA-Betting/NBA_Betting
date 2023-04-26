import logging
import os
import sys
from datetime import date, timedelta

# IMPORT SPIDERS HERE
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


if __name__ == "__main__":
    # Set up logging to only show errors
    logging.getLogger("scrapy").setLevel(logging.ERROR)

    # Get Scrapy project settings
    settings = get_project_settings()

    # Set up the CrawlerProcess with the project settings
    process = CrawlerProcess(settings)

    # Run each spider and handle errors
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_spider(
        InpredictableWPASpider,
        save_data=True,
        view_data=False,
        dates=yesterday,
    )
    # ADD MORE SPIDERS HERE
