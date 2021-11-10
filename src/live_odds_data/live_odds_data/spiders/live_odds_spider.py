import scrapy
from datetime import date

from live_odds_data.items import Game
from live_odds_data.item_loaders import GameLoader


class CoversLiveOddsSpider(scrapy.Spider):
    name = "covers_live_odds_spider"
    allowed_domains = ["www.covers.com"]
    start_urls = ["https://www.covers.com/sport/basketball/nba/odds"]

    def parse(self, response):

        todays_date = date.today().strftime("%b %d")
        game_table = response.xpath(
            '//table[contains(@id, "spread-game-nba")]/tbody/tr'
        )
        todays_game_count = sum(
            [
                1
                for row in game_table
                if row.xpath('//div[@class="__date"]/text()').get().strip()
                == todays_date
            ]
        )

        for game_num in range(1, todays_game_count + 1):
            loader = GameLoader(item=Game(), response=response)
            loader.add_xpath(
                "date",
                f"//table[contains(@id, 'spread-game-nba')]/tbody/tr[{game_num}]/td[1]//div[@class='__date']/text()",
            )
            loader.add_xpath(
                "time",
                f"//table[contains(@id, 'spread-game-nba')]/tbody/tr[{game_num}]/td[1]//div[@class='__time']/text()",
            )
            loader.add_xpath(
                "home_team_full_name",
                f"//table[contains(@id, 'spread-game-nba')]/tbody/tr[{game_num}]/td[2]//div[@class='__home']//span[@class='__fullname']/text()",
            )
            loader.add_xpath(
                "home_team_short_name",
                f"//table[contains(@id, 'spread-game-nba')]/tbody/tr[{game_num}]/td[2]//div[@class='__home']//span[@class='__shortname']/text()",
            )
            loader.add_xpath(
                "away_team_full_name",
                f"//table[contains(@id, 'spread-game-nba')]/tbody/tr[{game_num}]/td[2]//div[@class='__away']//span[@class='__fullname']/text()",
            )
            loader.add_xpath(
                "away_team_short_name",
                f"//table[contains(@id, 'spread-game-nba')]/tbody/tr[{game_num}]/td[2]//div[@class='__away']//span[@class='__shortname']/text()",
            )
            loader.add_xpath(
                "open_line_away",
                f"//table[contains(@id, 'spread-game-nba')]/tbody/tr[{game_num}]/td[4]//div[@class='__awayOdds']/div[@class='American']//span[@class='__oddValue']/text()",
            )
            loader.add_xpath(
                "open_line_home",
                f"//table[contains(@id, 'spread-game-nba')]/tbody/tr[{game_num}]/td[4]//div[@class='__homeOdds']/div[@class='American']//span[@class='__oddValue']/text()",
            )
            loader.add_xpath(
                "fanduel_line_away",
                f"//div[contains(@id, '__spreadDiv-nba')]/table/tbody/tr[{game_num}]/td[@data-book='FanDuel']//div[contains(@class,'__awayOdds')]//div[@class='American']//span[1]/text()",
            )
            loader.add_xpath(
                "fanduel_line_price_away",
                f"//div[contains(@id, '__spreadDiv-nba')]/table/tbody/tr[{game_num}]/td[@data-book='FanDuel']//div[contains(@class,'__awayOdds')]//div[@class='American']//span[2]/text()",
            )
            loader.add_xpath(
                "fanduel_line_home",
                f"//div[contains(@id, '__spreadDiv-nba')]/table/tbody/tr[{game_num}]/td[@data-book='FanDuel']//div[contains(@class,'__homeOdds')]//div[@class='American']/span/span[1]/text()",
            )
            loader.add_xpath(
                "fanduel_line_price_home",
                f"//div[contains(@id, '__spreadDiv-nba')]/table/tbody/tr[{game_num}]/td[@data-book='FanDuel']//div[contains(@class,'__homeOdds')]//div[@class='American']//span[2]/text()",
            )
            loader.add_xpath(
                "draftkings_line_away",
                f"//div[contains(@id, '__spreadDiv-nba')]/table/tbody/tr[{game_num}]/td[@data-book='DraftKings']//div[contains(@class,'__awayOdds')]//div[@class='American']//span[1]/text()",
            )
            loader.add_xpath(
                "draftkings_line_price_away",
                f"//div[contains(@id, '__spreadDiv-nba')]/table/tbody/tr[{game_num}]/td[@data-book='DraftKings']//div[contains(@class,'__awayOdds')]//div[@class='American']//span[2]/text()",
            )
            loader.add_xpath(
                "draftkings_line_home",
                f"//div[contains(@id, '__spreadDiv-nba')]/table/tbody/tr[{game_num}]/td[@data-book='DraftKings']//div[contains(@class,'__homeOdds')]//div[@class='American']/span/span[1]/text()",
            )
            loader.add_xpath(
                "draftkings_line_price_home",
                f"//div[contains(@id, '__spreadDiv-nba')]/table/tbody/tr[{game_num}]/td[@data-book='DraftKings']//div[contains(@class,'__homeOdds')]//div[@class='American']//span[2]/text()",
            )
            loader.add_xpath(
                "link",
                f"//table[contains(@id, 'spread-consensus-nba')]/tbody/tr[{game_num}]//a/@href",
            )

            item = loader.load_item()
            fields = [
                f
                for f in [
                    "date",
                    "time",
                    "home_team_full_name",
                    "home_team_short_name",
                    "away_team_full_name",
                    "away_team_short_name",
                    "open_line_away",
                    "open_line_home",
                    "fanduel_line_away",
                    "fanduel_line_price_away",
                    "fanduel_line_home",
                    "fanduel_line_price_home",
                    "draftkings_line_away",
                    "draftkings_line_price_away",
                    "draftkings_line_home",
                    "draftkings_line_price_home",
                ]
                if f not in item
            ]

            for f in fields:
                item[f] = None

            yield item
