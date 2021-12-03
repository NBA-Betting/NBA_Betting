import datetime
import pytz
from scrapy import Request, Spider
from scrapy_splash import SplashRequest

from basketball_reference.items import BR_OpponentStatsItem
from basketball_reference.item_loaders import BR_OpponentStatsItemLoader


class BR_StatsSpider(Spider):
    name = "BR_opponent_stats_spider"
    allowed_domains = ["basketball-reference.com"]

    current_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
    yesterday_datetime = current_datetime - datetime.timedelta(days=1)
    yesterday_day = yesterday_datetime.strftime("%d")
    yesterday_month = yesterday_datetime.strftime("%m")
    yesterday_year = yesterday_datetime.strftime("%Y")

    # Start of scraping and working backwards in time.
    start_day = 30
    start_month = 11
    start_year = 2021

    start_urls = [
        f"https://www.basketball-reference.com/friv/standings.fcgi?month={start_month}&day={start_day}&year={start_year}&lg_id=NBA"
    ]

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(
                url,
                self.parse,
                args={"wait": 0.5},
            )

    def parse(self, response):
        date = response.xpath('//span[@class="button2 index"]/text()').get()

        for row in response.xpath(
            f'//table[@id="opponent"]/tbody/tr[not(@class)]'
        ):
            loader = BR_OpponentStatsItemLoader(
                item=BR_OpponentStatsItem(), selector=row
            )
            loader.add_xpath("team", "td[@data-stat='team_name']/a/text()")
            loader.add_value("date", date)
            loader.add_xpath("opp_g", "td[@data-stat='g']/text()")
            loader.add_xpath("opp_mp", "td[@data-stat='mp']/text()")
            loader.add_xpath("opp_fg", "td[@data-stat='opp_fg']/text()")
            loader.add_xpath("opp_fga", "td[@data-stat='opp_fga']/text()")
            loader.add_xpath(
                "opp_fg_pct", "td[@data-stat='opp_fg_pct']/text()"
            )
            loader.add_xpath("opp_fg3", "td[@data-stat='opp_fg3']/text()")
            loader.add_xpath("opp_fg3a", "td[@data-stat='opp_fg3a']/text()")
            loader.add_xpath(
                "opp_fg3_pct", "td[@data-stat='opp_fg3_pct']/text()"
            )
            loader.add_xpath("opp_fg2", "td[@data-stat='opp_fg2']/text()")
            loader.add_xpath("opp_fg2a", "td[@data-stat='opp_fg2a']/text()")
            loader.add_xpath(
                "opp_fg2_pct", "td[@data-stat='opp_fg2_pct']/text()"
            )
            loader.add_xpath("opp_ft", "td[@data-stat='opp_ft']/text()")
            loader.add_xpath("opp_fta", "td[@data-stat='opp_fta']/text()")
            loader.add_xpath(
                "opp_ft_pct", "td[@data-stat='opp_ft_pct']/text()"
            )
            loader.add_xpath("opp_orb", "td[@data-stat='opp_orb']/text()")
            loader.add_xpath("opp_drb", "td[@data-stat='opp_drb']/text()")
            loader.add_xpath("opp_trb", "td[@data-stat='opp_trb']/text()")
            loader.add_xpath("opp_ast", "td[@data-stat='opp_ast']/text()")
            loader.add_xpath("opp_stl", "td[@data-stat='opp_stl']/text()")
            loader.add_xpath("opp_blk", "td[@data-stat='opp_blk']/text()")
            loader.add_xpath("opp_tov", "td[@data-stat='opp_tov']/text()")
            loader.add_xpath("opp_pf", "td[@data-stat='opp_pf']/text()")
            loader.add_xpath("opp_pts", "td[@data-stat='opp_pts']/text()")

            # add missing fields
            item = loader.load_item()
            fields = [
                f
                for f in [
                    "team",
                    "date",
                    "opp_g",
                    "opp_mp",
                    "opp_fg",
                    "opp_fga",
                    "opp_fg_pct",
                    "opp_fg3",
                    "opp_fg3a",
                    "opp_fg3_pct",
                    "opp_fg2",
                    "opp_fg2a",
                    "opp_fg2_pct",
                    "opp_ft",
                    "opp_fta",
                    "opp_ft_pct",
                    "opp_orb",
                    "opp_drb",
                    "opp_trb",
                    "opp_ast",
                    "opp_stl",
                    "opp_blk",
                    "opp_tov",
                    "opp_pf",
                    "opp_pts",
                ]
                if f not in item
            ]

            for f in fields:
                item[f] = None

            yield item

        # Uncomment below to work with more than one day at a time.

        # active_date = datetime.datetime.strptime(
        #     response.xpath('//span[@class="button2 index"]/text()').get(),
        #     "%B %d, %Y",
        # )
        # previous_date = datetime.datetime.strptime(
        #     response.xpath('//a[@class="button2 prev"]/text()').get(),
        #     "%B %d, %Y",
        # )
        # stop_date = datetime.datetime.strptime(
        #     "November 14, 2021", "%B %d, %Y"
        # )

        # # scrape previous date if before stop date
        # if active_date > stop_date:
        #     month = previous_date.month
        #     day = previous_date.day
        #     year = previous_date.year

        #     url = f"https://www.basketball-reference.com/friv/standings.fcgi?month={month}&day={day}&year={year}"

        #     yield SplashRequest(url, callback=self.parse, args={"wait": 0.5})
