import sys
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd
import scrapy
from scrapy.spiders import Spider
from scrapy.utils.project import get_project_settings

sys.path.append("../")
from data_sources_config import NBA_IMPORTANT_DATES


class BaseSpider(Spider):
    name = "base_spider"  # Update: data_source_name + _spider
    allowed_domains = []  # Update: Main website domain

    custom_settings = {
        "ITEM_PIPELINES": {
            "data_sources.pipelines.BasePipeline": 300
        }  # Update: DataSourceName + Pipeline
    }
    failed_dates = {
        "find_season_information": [],
        "start_requests": [],
        "parse": [],
        "save": [],
    }

    def __init__(
        self,
        dates=None,
        save_data=False,
        view_data=True,
        first_season=0,
        *args,
        **kwargs,
    ):
        super(BaseSpider, self).__init__(*args, **kwargs)
        self.save_data = save_data
        self.view_data = view_data
        self.first_season = int(first_season) if first_season else 0
        if dates == "all":
            self.dates = self.generate_all_dates(self.first_season)
        else:
            self.dates = dates.split(",") if dates else None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BaseSpider, cls).from_crawler(crawler, *args, **kwargs)
        pipeline_class = (
            "data_sources.pipelines.BasePipeline"  # Update: DataSourceName + Pipeline
        )
        if pipeline_class in crawler.settings.get("ITEM_PIPELINES"):
            spider.save_data = spider.save_data
            spider.view_data = spider.view_data
        return spider

    def generate_all_dates(self, first_season):
        all_dates = []
        for season, dates in NBA_IMPORTANT_DATES.items():
            start_year = int(season.split("-")[0])
            if start_year < first_season:
                continue
            start_date = datetime.strptime(dates["reg_season_start_date"], "%Y-%m-%d")
            end_date = datetime.strptime(dates["reg_season_end_date"], "%Y-%m-%d")
            current_date = start_date
            while current_date <= end_date:
                all_dates.append(current_date.strftime("%Y-%m-%d"))
                current_date += timedelta(days=1)
        return all_dates

    def handle_failed_date(self, date_str, reason):
        self.failed_dates[reason].append(date_str)

    def find_season_information(self, date_str):
        # Logic to use NBA_IMPORTANT_DATES to find necessary season information
        pass

    def start_requests(self):
        pass

    def parse(self, response):
        pass
