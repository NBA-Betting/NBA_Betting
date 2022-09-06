import sys
import datetime
import pytz
from scrapy import Request, Spider
from scrapy_splash import SplashRequest
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

sys.path.append("../../nba")

from items import NBA_ScoringStatsItem
from item_loaders import NBA_ScoringStatsItemLoader
from helpers import return_season_dates


class NBA_Scoring_Stats_Spider(Spider):
    name = "NBA_scoring_stats_spider"
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
    #     f"https://www.nba.com/stats/teams/scoring/?sort=TEAM_NAME&dir=-1&Season=2020-21&SeasonType=Regular%20Season&DateTo={start_month}%2F{start_day}%2F{start_year}"
    # ]

    def __init__(self, season_dates=None, *args, **kwargs):
        super(NBA_Scoring_Stats_Spider, self).__init__(*args, **kwargs)
        self.season_dates = season_dates
        self.start_urls = [
            f"https://www.nba.com/stats/teams/scoring/?sort=TEAM_NAME&dir=-1&Season={self.season_dates['season_years']}&SeasonType=Regular%20Season&DateTo={self.season_dates['final_month']}%2F{self.season_dates['final_day']}%2F{self.season_dates['final_year']}"
        ]

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(
                url,
                self.parse,
                args={"wait": 5, "private_mode_enabled": False},
            )

    def parse(self, response):
        date_to = response.xpath(
            "//span[starts-with(text(), 'Date To')]/text()"
        ).get()

        for row in response.xpath("//div[@class='nba-stat-table']//tbody/tr")[
            :30
        ]:
            loader = NBA_ScoringStatsItemLoader(
                item=NBA_ScoringStatsItem(), selector=row
            )
            loader.add_value("date", date_to)
            loader.add_xpath("team", "td[2]/a/text()")
            loader.add_xpath("pct_fga_2pt", "td[7]/text()")
            loader.add_xpath("pct_fga_3pt", "td[8]/text()")
            loader.add_xpath("pct_pts_2pt", "td[9]/text()")
            loader.add_xpath("pct_pts_2pt_mid", "td[10]/text()")
            loader.add_xpath("pct_pts_3pt", "td[11]/text()")
            loader.add_xpath("pct_pts_fbps", "td[12]/text()")
            loader.add_xpath("pct_pts_ft", "td[13]/text()")
            loader.add_xpath("pct_pts_off_tov", "td[14]/text()")
            loader.add_xpath("pct_pts_pitp", "td[15]/text()")
            loader.add_xpath("pct_ast_2fgm", "td[16]/text()")
            loader.add_xpath("pct_uast_2fgm", "td[17]/text()")
            loader.add_xpath("pct_ast_3fgm", "td[18]/text()")
            loader.add_xpath("pct_uast_3fgm", "td[19]/text()")
            loader.add_xpath("pct_ast_fgm", "td[20]/text()")
            loader.add_xpath("pct_uast_fgm", "td[21]/text()")

            # add missing fields
            item = loader.load_item()
            fields = [
                f
                for f in [
                    "date",
                    "team",
                    "pct_fga_2pt",
                    "pct_fga_3pt",
                    "pct_pts_2pt",
                    "pct_pts_2pt_mid",
                    "pct_pts_3pt",
                    "pct_pts_fbps",
                    "pct_pts_ft",
                    "pct_pts_off_tov",
                    "pct_pts_pitp",
                    "pct_ast_2fgm",
                    "pct_uast_2fgm",
                    "pct_ast_3fgm",
                    "pct_uast_3fgm",
                    "pct_ast_fgm",
                    "pct_uast_fgm",
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

            url = f"https://www.nba.com/stats/teams/scoring/?sort=TEAM_NAME&dir=-1&Season={self.season_dates['season_years']}&SeasonType=Regular%20Season&DateTo={month}%2F{day}%2F{year}"

            yield SplashRequest(
                url,
                callback=self.parse,
                args={"wait": 5, "private_mode_enabled": False},
            )


if __name__ == "__main__":

    for year in [2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015]:
        season_info = return_season_dates(year)
        process = CrawlerProcess(get_project_settings())
        process.crawl("NBA_scoring_stats_spider", season_dates=season_info)
    process.start()
