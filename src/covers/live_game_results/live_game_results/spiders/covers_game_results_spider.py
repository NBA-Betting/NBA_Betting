import datetime
import pytz
from scrapy import Request, Spider

from live_game_results.items import GameItem
from live_game_results.item_loaders import GameItemLoader


class CoversGameResultsSpider(Spider):
    name = "covers_game_results_spider"
    allowed_domains = ["https://www.covers.com/"]

    current_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
    yesterday_datetime = current_datetime - datetime.timedelta(days=1)
    yesterday_day = yesterday_datetime.strftime("%d")
    yesterday_month = yesterday_datetime.strftime("%m")
    yesterday_year = yesterday_datetime.strftime("%Y")

    # Date of results to scrape.
    start_day = yesterday_day
    start_month = yesterday_month
    start_year = yesterday_year

    start_urls = [
        f"https://www.covers.com/sports/nba/matchups?selectedDate={start_year}-{start_month}-{start_day}"
    ]

    def parse(self, response):
        date = response.xpath(
            '//a[@class="cmg_active_navigation_item"]/text()'
        ).get()

        for row in response.xpath(
            '//div[@class="cmg_matchup_game_box cmg_game_data"]'
        ):
            loader = GameItemLoader(item=GameItem(), selector=row)
            loader.add_value("date", date)
            loader.add_xpath("home_team", "@data-home-team-nickname-search")
            loader.add_xpath("away_team", "@data-away-team-nickname-search")
            loader.add_xpath("home_score", "@data-home-score")
            loader.add_xpath("away_score", "@data-away-score")

            # add missing fields
            item = loader.load_item()
            fields = [
                f
                for f in [
                    "date",
                    "home_team",
                    "away_team",
                    "home_score",
                    "away_score",
                ]
                if f not in item
            ]

            for f in fields:
                item[f] = None

            yield item
