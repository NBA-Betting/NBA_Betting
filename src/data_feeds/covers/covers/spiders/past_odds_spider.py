import re
from scrapy import Request, Spider

from ..items import PastGameItem
from ..item_loaders import PastGameItemLoader

base_url = "https://www.covers.com"

teams = [
    "boston-celtics",
    "brooklyn-nets",
    "toronto-raptors",
    "new-york-knicks",
    "philadelphia-76ers",
    "chicago-bulls",
    "cleveland-cavaliers",
    "detroit-pistons",
    "indiana-pacers",
    "milwaukee-bucks",
    "atlanta-hawks",
    "charlotte-hornets",
    "miami-heat",
    "orlando-magic",
    "washington-wizards",
    "denver-nuggets",
    "minnesota-timberwolves",
    "oklahoma-city-thunder",
    "portland-trail-blazers",
    "utah-jazz",
    "golden-state-warriors",
    "phoenix-suns",
    "sacramento-kings",
    "los-angeles-clippers",
    "los-angeles-lakers",
    "dallas-mavericks",
    "houston-rockets",
    "memphis-grizzlies",
    "new-orleans-pelicans",
    "san-antonio-spurs",
]


class CoversPastGameSpider(Spider):
    name = "covers_past_game_spider"
    allowed_domains = ["covers.com"]

    # start_urls = [
    #     base_url + "/sport/basketball/nba/teams/main/miami-heat/2022-2023"
    # ]

    start_urls = [
        base_url + f"/sport/basketball/nba/teams/main/{team}/2022-2023"
        for team in teams
    ]

    def parse(self, response):
        for row in response.xpath(
                '//table[@class="table covers-CoversMatchups-Table covers-CoversResults-Table"]/tbody/tr'
        ):
            loader = PastGameItemLoader(item=PastGameItem(), selector=row)
            loader.add_xpath("id_num", "td[3]/a/@href")
            loader.add_xpath("game_url", "td[3]/a/@href")
            loader.add_xpath("date", "td[1]/text()")
            loader.add_xpath("is_home", "td[2]/a/text()")
            loader.add_xpath("opponent", "td[2]/a/text()")
            loader.add_xpath("result", "td[3]/a/text()")
            loader.add_xpath("score", "td[3]/a/text()")
            loader.add_xpath("opponent_score", "td[3]/a/text()")
            loader.add_xpath("spread_result", "td[4]/span/text()")
            loader.add_xpath("spread", "td[4]/text()")
            loader.add_value(
                "team",
                re.search(r"main/(.*)/\d", str(response)).group(1))
            loader.add_value(
                "league_year",
                re.search(r"(\d\d\d\d-\d\d\d\d)", str(response)).group(),
            )

            # add missing fields
            item = loader.load_item()
            fields = [
                f for f in [
                    "id_num",
                    "game_url",
                    "date",
                    "is_home",
                    "opponent",
                    "result",
                    "score",
                    "opponent_score",
                    "spread_result",
                    "spread",
                    "team",
                    "league_year",
                ] if f not in item
            ]

            for f in fields:
                item[f] = None

            print('dog')

            yield item

        # current_season = response.xpath(
        #     '//div[@id="TP_pastResults"]//span[@id="TP-Season-Select"]/text()'
        # ).get()[-4:]
        # previous_season = str(int(current_season) - 1)
        # other_season_urls = response.xpath(
        #     '//div[@id="TP_pastResults"]//div[@id="TP-Season-Drop"]/li/a/@href'
        # ).getall()

        # scrape previous season if one exists
        # if int(current_season) > 2007:
        #     url = (base_url + [
        #         season for season in other_season_urls
        #         if season.endswith(previous_season)
        #     ][0])
        #     yield Request(url, callback=self.parse)
