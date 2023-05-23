import json
import os
import re
import sys
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlencode, urlparse

import pytz
import scrapy
from data_sources.item_loaders import NbaStatsGameResultsItemLoader
from data_sources.items import NbaStatsGameResultsItem
from data_sources.spiders.base_spider import BaseSpider
from scrapy.loader.processors import Join, MapCompose

sys.path.append("../../../../")
from passkeys import API_KEY_ZYTE


class NbaStatsGameResultsSpider(BaseSpider):
    name = "nba_stats_game_results_spider"
    allowed_domains = ["www.nba.com"]
    custom_settings = {
        "ITEM_PIPELINES": {"data_sources.pipelines.NbaStatsGameResultsPipeline": 300},
    }

    # if os.environ.get("ENVIRONMENT") == "EC2":
    custom_settings.update(
        {
            "DOWNLOAD_HANDLERS": {
                "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
                "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            },
            "DOWNLOADER_MIDDLEWARES": {
                "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
            },
            "REQUEST_FINGERPRINTER_CLASS": "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
            "ZYTE_API_KEY": API_KEY_ZYTE,
            "ZYTE_API_TRANSPARENT_MODE": True,
            "ZYTE_API_ENABLED": True,
        }
    )

    first_season = 1976

    def __init__(self, dates, save_data=False, view_data=True, *args, **kwargs):
        super().__init__(
            dates,
            save_data=save_data,
            view_data=view_data,
            first_season=self.first_season,
            *args,
            **kwargs,
        )

        if dates == "all" or dates == "daily_update":
            pass
        else:
            self.dates = []
            date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
            for date_str in dates.split(","):
                if not date_pattern.match(date_str):
                    raise ValueError(
                        f"Invalid date format: {date_str}. Date format should be 'YYYY-MM-DD', 'daily_update', or 'all'"
                    )
                self.dates.append(date_str)

    def start_requests(self):
        base_url = "https://stats.nba.com/stats/scoreboardv3"
        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "DNT": "1",
            "Origin": "https://www.nba.com",
            "Pragma": "no-cache",
            "Referer": "https://www.nba.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
        }
        params = {"LeagueID": "00"}

        if self.dates == "all":
            for date in self.generate_all_dates(self.first_season):
                params.update({"GameDate": date})
                url = base_url + "?" + urlencode(params)
                yield scrapy.Request(url, headers=headers, callback=self.parse)

        elif self.dates == "daily_update":
            now_mountain = datetime.now(pytz.timezone("America/Denver"))
            yesterday_mountain = now_mountain - timedelta(1)
            yesterday_str = yesterday_mountain.strftime("%Y-%m-%d")
            season_info_result = self.find_season_information(yesterday_str)
            if season_info_result["error"]:
                print(
                    f"Error: {season_info_result['error']} for the date {yesterday_str}"
                )
                self.handle_failed_date(yesterday_str, "find_season_information")
            else:
                params.update(
                    {
                        "GameDate": yesterday_str,
                    }
                )
                url = base_url + "?" + urlencode(params)
                yield scrapy.Request(url, headers=headers, callback=self.parse)

        else:
            for date_str in self.dates:
                season_info_result = self.find_season_information(date_str)
                if season_info_result["error"]:
                    print(
                        f"Error: {season_info_result['error']} for the date {date_str}"
                    )
                    self.handle_failed_date(date_str, "find_season_information")
                    continue

                params.update({"GameDate": date_str})
                url = base_url + "?" + urlencode(params)
                yield scrapy.Request(
                    url, headers=headers, callback=self.parse, meta={"date": date_str}
                )

    def parse(self, response):
        data = json.loads(response.body)
        game_date = data["scoreboard"]["gameDate"]
        for game in data["scoreboard"]["games"]:
            loader = NbaStatsGameResultsItemLoader(
                item=NbaStatsGameResultsItem(), selector=game
            )

            game_id = game["gameId"]
            loader.add_value("game_id", game_id)

            home_team_id = game["homeTeam"]["teamId"]
            away_team_id = game["awayTeam"]["teamId"]
            loader.add_value("home_team_id", home_team_id)
            loader.add_value("away_team_id", away_team_id)

            home_team = game["homeTeam"]["teamTricode"]
            away_team = game["awayTeam"]["teamTricode"]
            loader.add_value("home_team", home_team)
            loader.add_value("away_team", away_team)

            home_score = game["homeTeam"]["score"]
            away_score = game["awayTeam"]["score"]
            loader.add_value("home_score", home_score)
            loader.add_value("away_score", away_score)

            loader.add_value("game_date", game_date)

            yield loader.load_item()
