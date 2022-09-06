import sys
import datetime
import pytz
from scrapy import Request, Spider
from scrapy_splash import SplashRequest
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

sys.path.append("../../nba")

from items import NBA_TraditionalStatsItem
from item_loaders import NBA_TraditionalStatsItemLoader
from helpers import return_season_dates


class NBA_Traditional_Stats_Spider(Spider):
    name = "NBA_traditional_stats_spider"
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
    #     f"https://www.nba.com/stats/teams/traditional/?sort=TEAM_NAME&dir=-1&Season=2020-21&SeasonType=Regular%20Season&DateTo={start_month}%2F{start_day}%2F{start_year}"
    # ]

    def __init__(self, season_dates=None, *args, **kwargs):
        super(NBA_Traditional_Stats_Spider, self).__init__(*args, **kwargs)
        self.season_dates = season_dates
        self.start_urls = [
            f"https://www.nba.com/stats/teams/traditional/?sort=TEAM_NAME&dir=-1&Season={self.season_dates['season_years']}&SeasonType=Regular%20Season&DateTo={self.season_dates['final_month']}%2F{self.season_dates['final_day']}%2F{self.season_dates['final_year']}"
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
            loader = NBA_TraditionalStatsItemLoader(
                item=NBA_TraditionalStatsItem(), selector=row
            )
            loader.add_value("date", date_to)
            loader.add_xpath("team", "td[2]/a/text()")
            loader.add_xpath("gp", "td[3]/text()")
            loader.add_xpath("win", "td[4]/text()")
            loader.add_xpath("loss", "td[5]/text()")
            loader.add_xpath("w_pct", "td[6]/text()")
            loader.add_xpath("mins", "td[7]/text()")
            loader.add_xpath("pts", "td[8]/text()")
            loader.add_xpath("fgm", "td[9]/a/text()[1]")
            loader.add_xpath("fga", "td[10]/a/text()[1]")
            loader.add_xpath("fg_pct", "td[11]/text()")
            loader.add_xpath("fg3m", "td[12]/a/text()[1]")
            loader.add_xpath("fg3a", "td[13]/a/text()[1]")
            loader.add_xpath("fg3_pct", "td[14]/text()")
            loader.add_xpath("ftm", "td[15]/text()")
            loader.add_xpath("fta", "td[16]/text()")
            loader.add_xpath("ft_pct", "td[17]/text()")
            loader.add_xpath("oreb", "td[18]/a/text()[1]")
            loader.add_xpath("dreb", "td[19]/a/text()[1]")
            loader.add_xpath("reb", "td[20]/a/text()[1]")
            loader.add_xpath("ast", "td[21]/a/text()[1]")
            loader.add_xpath("tov", "td[22]/a/text()[1]")
            loader.add_xpath("stl", "td[23]/a/text()[1]")
            loader.add_xpath("blk", "td[24]/a/text()[1]")
            loader.add_xpath("blka", "td[25]/a/text()[1]")
            loader.add_xpath("pf", "td[26]/text()")
            loader.add_xpath("pfd", "td[27]/text()")
            loader.add_xpath("p_m", "td[28]/text()")

            # add missing fields
            item = loader.load_item()
            fields = [
                f
                for f in [
                    "date",
                    "team",
                    "gp",
                    "win",
                    "loss",
                    "w_pct",
                    "mins",
                    "pts",
                    "fgm",
                    "fga",
                    "fg_pct",
                    "fg3m",
                    "fg3a",
                    "fg3_pct",
                    "ftm",
                    "fta",
                    "ft_pct",
                    "oreb",
                    "dreb",
                    "reb",
                    "ast",
                    "tov",
                    "stl",
                    "blk",
                    "blka",
                    "pf",
                    "pfd",
                    "p_m",
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

            url = f"https://www.nba.com/stats/teams/traditional/?sort=TEAM_NAME&dir=-1&Season={self.season_dates['season_years']}&SeasonType=Regular%20Season&DateTo={month}%2F{day}%2F{year}"

            yield SplashRequest(
                url,
                callback=self.parse,
                args={"wait": 10, "private_mode_enabled": False},
            )


if __name__ == "__main__":

    for year in [2020, 2019, 2018, 2017, 2016]:
        season_info = return_season_dates(year)
        process = CrawlerProcess(get_project_settings())
        process.crawl("NBA_traditional_stats_spider", season_dates=season_info)
    process.start()
