import sys
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd
import scrapy
from scrapy.spiders import Spider
from scrapy.utils.project import get_project_settings

from src.data_feeds.data_sources_config import NBA_IMPORTANT_DATES


class BaseSpider(Spider):
    name = "<data_source_name>_spider"  # Update: data_source_name
    allowed_domains = []  # Update

    custom_settings = {
        "ITEM_PIPELINES": {
            "src.data_feeds.data_sources.data_sources.pipelines.BasePipeline": 300
        }  # Update: DataSourceName + Pipeline
    }

    NBA_IMPORTANT_DATES = NBA_IMPORTANT_DATES

    failed_dates = {
        "find_season_information": [],
        "start_requests": [],
        "parse": [],
        "save": [],
    }

    first_season = 0  # Update: First season of data source

    def __init__(self, dates, save_data=False, view_data=True, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)

        if isinstance(save_data, bool):
            self.save_data = save_data
        elif isinstance(save_data, str) and save_data.lower() in ("true", "false"):
            self.save_data = save_data.lower() == "true"
        else:
            raise ValueError(
                "Invalid input for 'save_data'. It must be a boolean or a string representation of a boolean."
            )

        if isinstance(view_data, bool):
            self.view_data = view_data
        elif isinstance(view_data, str) and view_data.lower() in ("true", "false"):
            self.view_data = view_data.lower() == "true"
        else:
            raise ValueError(
                "Invalid input for 'view_data'. It must be a boolean or a string representation of a boolean."
            )

        self.dates = dates

    def generate_all_dates(self, first_season):
        all_dates = []
        for season, dates in NBA_IMPORTANT_DATES.items():
            start_year = int(season.split("-")[0])
            if start_year < first_season:
                continue
            start_date = datetime.strptime(dates["reg_season_start_date"], "%Y-%m-%d")
            end_date = datetime.strptime(dates["postseason_end_date"], "%Y-%m-%d")
            current_date = start_date
            while current_date <= end_date:
                all_dates.append(current_date.strftime("%Y-%m-%d"))
                current_date += timedelta(days=1)
        return all_dates

    def handle_failed_date(self, date_str, reason):
        self.failed_dates[reason].append(date_str)

    def find_season_information(self, date_str):
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        for season, dates in self.NBA_IMPORTANT_DATES.items():
            reg_season_start_date = datetime.strptime(
                dates["reg_season_start_date"], "%Y-%m-%d"
            )
            postseason_end_date = datetime.strptime(
                dates["postseason_end_date"], "%Y-%m-%d"
            )

            if reg_season_start_date <= date_obj <= postseason_end_date:
                year1, year2 = season.split("-")
                return {
                    "info": f"{year1}-{year2[-2:]}",
                    "error": None,
                }
        return {"info": None, "error": "Unable to find season information"}

    def start_requests(self):
        pass

    def parse(self, response):
        pass
