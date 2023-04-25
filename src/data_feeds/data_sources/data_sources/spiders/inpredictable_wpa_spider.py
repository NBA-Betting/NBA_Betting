import sys
from datetime import datetime
from urllib.parse import parse_qs, urlencode, urlparse

import scrapy

sys.path.append("../")
from data_sources_config import NBA_IMPORTANT_DATES

from .base_spider import BaseSpider


class InpredictableWPASpider(BaseSpider):
    name = "inpredictable_wpa_spider"
    allowed_domains = ["stats.inpredictable.com"]

    custom_settings = {
        "ITEM_PIPELINES": {"data_sources.pipelines.InpredictableWPAPipeline": 300}
    }

    def __init__(self, *args, **kwargs):
        super().__init__(first_season=1996, *args, **kwargs)

    def find_season_information(self, date_str):
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        for season, dates in NBA_IMPORTANT_DATES.items():
            reg_season_start_date = datetime.strptime(
                dates["reg_season_start_date"], "%Y-%m-%d"
            )
            reg_season_end_date = datetime.strptime(
                dates["reg_season_end_date"], "%Y-%m-%d"
            )

            if reg_season_start_date <= date_obj <= reg_season_end_date:
                return {
                    "reg_season_start_date": dates["reg_season_start_date"],
                    "start_year": int(season.split("-")[0]),
                }
        return None

    def start_requests(self):
        base_url = "http://stats.inpredictable.com/nba/ssnPlayer.php"
        params = {
            "team": "ALL",
            "pos": "ALL",
            "po": "2",
            "rate": "tot",
            "sort": "sWPA",
            "order": "DESC",
            "grp": "1",
        }

        for date_str in self.dates:
            season_info = self.find_season_information(date_str)
            if not season_info:
                print(
                    f"Error: Unable to find season information for the date {date_str}"
                )
                continue

            frdt, season = season_info.values()

            params.update({"season": season, "frdt": frdt, "todt": date_str})
            url = base_url + "?" + urlencode(params)
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # Scrape the table on the current page
        table_rows = response.css(".iptbl table tr")

        parsed_url = urlparse(response.url)
        query_params = parse_qs(parsed_url.query)
        to_date = query_params.get("todt", [None])[0]
        for row in table_rows[3:]:  # Skip the header rows
            data = {
                "rnk": row.css("td:nth-child(1)::text").get(),
                "player": row.css("td:nth-child(2) a::text").get(),
                "pos": row.css("td:nth-child(3)::text").get(),
                "gms": row.css("td:nth-child(4)::text").get(),
                "wpa": row.css("td:nth-child(5)::text").get(),
                "ewpa": row.css("td:nth-child(6)::text").get(),
                "clwpa": row.css("td:nth-child(7)::text").get(),
                "gbwpa": row.css("td:nth-child(8)::text").get(),
                "sh": row.css("td:nth-child(9)::text").get(),
                "to": row.css("td:nth-child(10)::text").get(),
                "ft": row.css("td:nth-child(11)::text").get(),
                "reb": row.css("td:nth-child(12)::text").get(),
                "ast": row.css("td:nth-child(13)::text").get(),
                "stl": row.css("td:nth-child(14)::text").get(),
                "blk": row.css("td:nth-child(15)::text").get(),
                "kwpa": row.css("td:nth-child(16)::text").get(),
                "to_date": to_date,
            }
            yield data

        # Check if there are more pages to scrape
        next_page_links = response.css("div.slbl a::attr(href)").getall()
        for link in next_page_links:
            next_page_url = response.urljoin(link)
            yield scrapy.Request(next_page_url, callback=self.parse)
