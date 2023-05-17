import re
from datetime import datetime
from urllib.parse import parse_qs, urlencode, urlparse

import scrapy
from data_sources.item_loaders import InpredictableWPAItemLoader
from data_sources.items import InpredictableWPAItem

from .base_spider import BaseSpider


class InpredictableWPASpider(BaseSpider):
    name = "inpredictable_wpa_spider"
    allowed_domains = ["stats.inpredictable.com"]

    custom_settings = {
        "ITEM_PIPELINES": {"data_sources.pipelines.InpredictableWPAPipeline": 300}
    }

    first_season = 1996

    def __init__(self, dates, save_data=False, view_data=True, *args, **kwargs):
        super().__init__(
            dates,
            save_data=save_data,
            view_data=view_data,
            first_season=self.first_season,
            *args,
            **kwargs,
        )

        date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")

        if dates == "all":
            self.dates = self.generate_all_dates(self.first_season)
        else:
            self.dates = []
            for date_str in dates.split(","):
                if not date_pattern.match(date_str) and date_str != "all":
                    raise ValueError(
                        f"Invalid date format: {date_str}. Date format should be 'YYYY-MM-DD' or 'all'"
                    )
                self.dates.append(date_str)

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
                return {
                    "info": {
                        "reg_season_start_date": dates["reg_season_start_date"],
                        "start_year": int(season.split("-")[0]),
                    },
                    "error": None,
                }
        return {"info": None, "error": "Unable to find season information"}

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
            season_info_result = self.find_season_information(date_str)
            if season_info_result["error"]:
                print(f"Error: {season_info_result['error']} for the date {date_str}")
                self.handle_failed_date(date_str, "find_season_information")
                continue

            frdt, season = season_info_result["info"].values()

            params.update({"season": season, "frdt": frdt, "todt": date_str})
            url = base_url + "?" + urlencode(params)
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # Scrape the table on the current page
        table_rows = response.css(".iptbl table tr")

        parsed_url = urlparse(response.url)
        query_params = parse_qs(parsed_url.query)
        to_date = query_params.get("todt", [None])[0]
        if not table_rows:
            self.handle_failed_date(to_date, "parse")
            return

        for row in table_rows[3:]:  # Skip the header rows
            loader = InpredictableWPAItemLoader(
                item=InpredictableWPAItem(), selector=row
            )
            loader.add_css("rnk", "td:nth-child(1)::text")
            loader.add_css("player", "td:nth-child(2) a::text")
            loader.add_css("pos", "td:nth-child(3)::text")
            loader.add_css("gms", "td:nth-child(4)::text")
            loader.add_css("wpa", "td:nth-child(5)::text")
            loader.add_css("ewpa", "td:nth-child(6)::text")
            loader.add_css("clwpa", "td:nth-child(7)::text")
            loader.add_css("gbwpa", "td:nth-child(8)::text")
            loader.add_css("sh", "td:nth-child(9)::text")
            loader.add_css("to", "td:nth-child(10)::text")
            loader.add_css("ft", "td:nth-child(11)::text")
            loader.add_css("reb", "td:nth-child(12)::text")
            loader.add_css("ast", "td:nth-child(13)::text")
            loader.add_css("stl", "td:nth-child(14)::text")
            loader.add_css("blk", "td:nth-child(15)::text")
            loader.add_css("kwpa", "td:nth-child(16)::text")
            loader.add_value("to_date", to_date)
            yield loader.load_item()

        # Check if there are more pages to scrape
        next_page_links = response.css("div.slbl a::attr(href)").getall()
        for link in next_page_links:
            next_page_url = response.urljoin(link)
            yield scrapy.Request(next_page_url, callback=self.parse)
