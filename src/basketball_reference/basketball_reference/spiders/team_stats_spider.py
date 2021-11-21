from datetime import datetime
from scrapy import Request, Spider
from scrapy_splash import SplashRequest

from basketball_reference.items import BR_TeamStatsItem
from basketball_reference.item_loaders import BR_TeamStatsItemLoader


class BR_StatsSpider(Spider):
    name = "BR_team_stats_spider"
    allowed_domains = ["basketball-reference.com"]

    current_datetime = datetime.now()
    current_day = current_datetime.strftime("%d")
    current_month = current_datetime.strftime("%m")
    current_year = current_datetime.strftime("%Y")

    # Start of scraping and working backwards in time.
    start_day = 15
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
            f'//table[@id="team"]/tbody/tr[not(@class)]'
        ):
            loader = BR_TeamStatsItemLoader(
                item=BR_TeamStatsItem(), selector=row
            )
            loader.add_xpath("team", "td[@data-stat='team_name']/a/text()")
            loader.add_value("date", date)
            loader.add_xpath("g", "td[@data-stat='g']/text()")
            loader.add_xpath("mp", "td[@data-stat='mp']/text()")
            loader.add_xpath("fg", "td[@data-stat='fg']/text()")
            loader.add_xpath("fga", "td[@data-stat='fga']/text()")
            loader.add_xpath("fg_pct", "td[@data-stat='fg_pct']/text()")
            loader.add_xpath("fg3", "td[@data-stat='fg3']/text()")
            loader.add_xpath("fg3a", "td[@data-stat='fg3a']/text()")
            loader.add_xpath("fg3_pct", "td[@data-stat='fg3_pct']/text()")
            loader.add_xpath("fg2", "td[@data-stat='fg2']/text()")
            loader.add_xpath("fg2a", "td[@data-stat='fg2a']/text()")
            loader.add_xpath("fg2_pct", "td[@data-stat='fg2_pct']/text()")
            loader.add_xpath("ft", "td[@data-stat='ft']/text()")
            loader.add_xpath("fta", "td[@data-stat='fta']/text()")
            loader.add_xpath("ft_pct", "td[@data-stat='ft_pct']/text()")
            loader.add_xpath("orb", "td[@data-stat='orb']/text()")
            loader.add_xpath("drb", "td[@data-stat='drb']/text()")
            loader.add_xpath("trb", "td[@data-stat='trb']/text()")
            loader.add_xpath("ast", "td[@data-stat='ast']/text()")
            loader.add_xpath("stl", "td[@data-stat='stl']/text()")
            loader.add_xpath("blk", "td[@data-stat='blk']/text()")
            loader.add_xpath("tov", "td[@data-stat='tov']/text()")
            loader.add_xpath("pf", "td[@data-stat='pf']/text()")
            loader.add_xpath("pts", "td[@data-stat='pts']/text()")

            # add missing fields
            item = loader.load_item()
            fields = [
                f
                for f in [
                    "team",
                    "date",
                    "g",
                    "mp",
                    "fg",
                    "fga",
                    "fg_pct",
                    "fg3",
                    "fg3a",
                    "fg3_pct",
                    "fg2",
                    "fg2a",
                    "fg2_pct",
                    "ft",
                    "fta",
                    "ft_pct",
                    "orb",
                    "drb",
                    "trb",
                    "ast",
                    "stl",
                    "blk",
                    "tov",
                    "pf",
                    "pts",
                ]
                if f not in item
            ]

            for f in fields:
                item[f] = None

            yield item

        active_date = datetime.strptime(
            response.xpath('//span[@class="button2 index"]/text()').get(),
            "%B %d, %Y",
        )
        previous_date = datetime.strptime(
            response.xpath('//a[@class="button2 prev"]/text()').get(),
            "%B %d, %Y",
        )
        stop_date = datetime.strptime("October 12, 2006", "%B %d, %Y")

        # scrape previous date if before stop date
        if active_date > stop_date:
            month = previous_date.month
            day = previous_date.day
            year = previous_date.year

            url = f"https://www.basketball-reference.com/friv/standings.fcgi?month={month}&day={day}&year={year}"

            yield SplashRequest(url, callback=self.parse, args={"wait": 0.5})
