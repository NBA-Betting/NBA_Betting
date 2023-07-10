
import json
import os
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode

import pytz
import scrapy
from dotenv import load_dotenv

from ....data_source_utils import BaseSpider
from ..item_loaders import TestDataSourceItemLoader
from ..items import TestDataSourceItem

load_dotenv()
ZYTE_API_KEY = os.environ.get("ZYTE_API_KEY")


class TeamTestDataSourceSpider(BaseSpider):
    name = "team_test_data_source_spider"
    pipeline_name = "TestDataSourcePipeline"
    project_section = "team"
    first_season_start_year = 1996

    def __init__(
        self, dates, save_data=False, view_data=True, use_zyte=False, *args, **kwargs
    ):
        super().__init__(
            dates,
            save_data=save_data,
            view_data=view_data,
            use_zyte=use_zyte,
            *args,
            **kwargs
        )


    def start_requests(self):
        base_url = ""  # Update: Base URL for the data source
        headers = {}  # Update: Headers for the data source, if necessary
        # Headers can be found in the Network tab of Google Dev Tools
        params = {}  # Update: Parameters for the data source.
        # Example: {"season": "2020-21", "frdt": "2020-12-22", "todt": "2020-12-22"}

        # Update this section to create all starting urls needed
        for date_str in self.dates:
            url = base_url + "?" + urlencode(params)
            yield scrapy.Request(url, callback=self.parse)
    
    def parse(self, response):
        # Code to get to the table/iterable for the data
        # Example:
        # table_rows = response.css(".iptbl table tr")

        loader = TestDataSourceItemLoader(
                item=TestDataSourceItem()
        )

        # Example:
        # loader.add_value("url", response.url)
        # loader.add_xpath('name', '//div[@class="name"]/text()')  # replace with your actual XPath
        # loader.add_css('description', '.description::text')  # replace with your actual CSS selector

        loader.add_value("column1", column1)
		loader.add_value("column2", column2)
		loader.add_value("column3", column3)
		loader.add_value("column4", column4)

        yield loader.load_item()

        # Code to get to the next page if pagination
        # Example:
        # next_page_links = response.css("div.slbl a::attr(href)").getall()
        # for link in next_page_links:
        #     next_page_url = response.urljoin(link)
        #     yield scrapy.Request(next_page_url, callback=self.parse)

    