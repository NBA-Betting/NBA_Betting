import sys
import datetime
import pytz
from scrapy import Request, Spider
from scrapy_splash import SplashRequest
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

sys.path.append("../../nba")

from items import NBA_HustleStatsItem
from item_loaders import NBA_HustleStatsItemLoader
from helpers import return_season_dates


class NBA_Hustle_Stats_Spider(Spider):
    name = "NBA_hustle_stats_spider"
    # allowed_domains = ["nba.com", "https://www.nba.com/stats/teams/"]

    current_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
    yesterday_datetime = current_datetime - datetime.timedelta(days=1)
    yesterday_day = yesterday_datetime.strftime("%d")
    yesterday_month = yesterday_datetime.strftime("%m")
    yesterday_year = yesterday_datetime.strftime("%Y")

    # Start of scraping and working backwards in time.
    # season_abbrv = '2022-23'
    # start_day = yesterday_day
    # start_month = yesterday_month
    # start_year = yesterday_year

    # start_urls = [
    #     f"https://www.nba.com/stats/teams/hustle/?sort=TEAM_NAME&dir=-1&Season=2020-21&SeasonType=Regular%20Season&DateTo={start_month}%2F{start_day}%2F{start_year}"
    # ]

    def __init__(self, season_dates=None, *args, **kwargs):
        super(NBA_Hustle_Stats_Spider, self).__init__(*args, **kwargs)
        self.season_dates = season_dates
        self.start_urls = [
            f"https://www.nba.com/stats/teams/hustle/?sort=TEAM_NAME&dir=-1&Season={self.season_dates['season_years']}&SeasonType=Regular%20Season&DateTo={self.season_dates['final_month']}%2F{self.season_dates['final_day']}%2F{self.season_dates['final_year']}"
        ]

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(
                url,
                self.parse,
                args={"wait": 10, "private_mode_enabled": False},
            )

    def parse(self, response):
        date_to = response.xpath(
            "//span[starts-with(text(), 'Date To')]/text()"
        ).get()

        for row in response.xpath("//div[@class='nba-stat-table']//tbody/tr")[
            :30
        ]:
            loader = NBA_HustleStatsItemLoader(
                item=NBA_HustleStatsItem(), selector=row
            )
            loader.add_value("date", date_to)
            loader.add_xpath("team", "td[1]/a/text()")
            loader.add_xpath("screen_ast", "td[3]/text()")
            loader.add_xpath("screen_ast_pts", "td[4]/text()")
            loader.add_xpath("deflections", "td[5]/text()")
            loader.add_xpath("off_loose_ball_rec", "td[6]/text()")
            loader.add_xpath("def_loose_ball_rec", "td[7]/text()")
            loader.add_xpath("loose_ball_rec", "td[8]/text()")
            loader.add_xpath("pct_loose_ball_rec_off", "td[9]/text()")
            loader.add_xpath("pct_loose_ball_rec_def", "td[10]/text()")
            loader.add_xpath("charges_drawn", "td[11]/text()")
            loader.add_xpath("contested_2pt", "td[12]/text()")
            loader.add_xpath("contested_3pt", "td[13]/text()")
            loader.add_xpath("contested_shots", "td[14]/text()")

            # add missing fields
            item = loader.load_item()
            fields = [
                f
                for f in [
                    "date",
                    "team",
                    "screen_ast",
                    "screen_ast_pts",
                    "deflections",
                    "off_loose_ball_rec",
                    "def_loose_ball_rec",
                    "loose_ball_rec",
                    "pct_loose_ball_rec_off",
                    "pct_loose_ball_rec_def",
                    "charges_drawn",
                    "contested_2pt",
                    "contested_3pt",
                    "contested_shots",
                ]
                if f not in item
            ]

            for f in fields:
                item[f] = None

            print(item)
            yield item

        # Uncomment below to work with more than one day at a time.

        active_date = datetime.datetime.strptime(
            date_to.strip("Date To : "),
            "%m/%d/%Y",
        )
        previous_date = active_date - datetime.timedelta(days=1)

        # stop_date = datetime.datetime.strptime("Month 00, 0000", "%B %d, %Y")
        stop_date = self.season_dates["start_day"]

        # scrape previous date if before stop date
        if active_date > stop_date:
            month = previous_date.month
            day = previous_date.day
            year = previous_date.year

            url = f"https://www.nba.com/stats/teams/hustle/?sort=TEAM_NAME&dir=-1&Season={self.season_dates['season_years']}&SeasonType=Regular%20Season&DateTo={month}%2F{day}%2F{year}"

            yield SplashRequest(
                url,
                callback=self.parse,
                args={"wait": 10, "private_mode_enabled": False},
            )


if __name__ == "__main__":

    for year in [2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015]:
        season_info = return_season_dates(year)
        process = CrawlerProcess(get_project_settings())
        process.crawl("NBA_hustle_stats_spider", season_dates=season_info)
    process.start()
