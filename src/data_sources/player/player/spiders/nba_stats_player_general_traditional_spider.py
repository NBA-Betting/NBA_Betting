import json
import os
import re
import sys
from datetime import datetime, timedelta
from urllib.parse import urlencode

import pytz
import scrapy
from dotenv import load_dotenv

from ..item_loaders import NbaStatsPlayerGeneralTraditionalItemLoader
from ..items import NbaStatsPlayerGeneralTraditionalItem

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../../.."))

from utils.data_source_utils import BaseSpider

load_dotenv()
ZYTE_API_KEY = os.environ.get("ZYTE_API_KEY")


class NbaStatsPlayerGeneralTraditionalSpider(BaseSpider):
    name = "nba_stats_player_general_traditional_spider"
    allowed_domains = ["www.nba.com"]
    custom_settings = {
        "ITEM_PIPELINES": {
            "data_sources.pipelines.NbaStatsPlayerGeneralTraditionalPipeline": 300
        },
    }

    if os.environ.get("ENVIRONMENT") == "EC2":
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
                "ZYTE_API_KEY": ZYTE_API_KEY,
                "ZYTE_API_TRANSPARENT_MODE": True,
                "ZYTE_API_ENABLED": True,
            }
        )

    first_season = "2016 - 2017"

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

    def find_season_information(self, date_str):
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        for _season, dates in self.NBA_IMPORTANT_DATES.items():
            reg_season_start_date = datetime.strptime(
                dates["reg_season_start_date"], "%Y-%m-%d"
            )
            reg_season_end_date = datetime.strptime(
                dates["reg_season_end_date"], "%Y-%m-%d"
            )
            postseason_start_date = datetime.strptime(
                dates["postseason_start_date"], "%Y-%m-%d"
            )
            postseason_end_date = datetime.strptime(
                dates["postseason_end_date"], "%Y-%m-%d"
            )

            if reg_season_start_date <= date_obj <= reg_season_end_date:
                year1, year2 = _season.split("-")
                season = f"{year1}-{year2[-2:]}"
                season_type = "Regular Season"
                return {"season": season, "season_type": season_type, "error": None}

            elif postseason_start_date <= date_obj <= postseason_end_date:
                year1, year2 = _season.split("-")
                season = f"{year1}-{year2[-2:]}"
                season_type = "Playoffs"
                return {"season": season, "season_type": season_type, "error": None}

        return {"error": "Unable to find season information"}

    def start_requests(self):
        base_url = "https://stats.nba.com/stats/leaguedashplayerstats"
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
        params = {
            "LastNGames": 0,
            "LeagueID": "00",
            "MeasureType": "Base",
            "Month": 0,
            "OpponentTeamID": 0,
            "PORound": 0,
            "PaceAdjust": "N",
            "PerMode": "PerGame",
            "Period": 0,
            "PlusMinus": "N",
            "Rank": "N",
            "Season": "2022-23",
            "SeasonType": "Playoffs",
            "TeamID": 0,
        }

        if self.dates == "all":
            # Extract start year of first_season
            first_season_start_year = int(self.first_season.split("-")[0])

            # Update the list comprehension to include condition
            seasons = [
                season
                for season in self.NBA_IMPORTANT_DATES.keys()
                if int(season.split("-")[0]) >= first_season_start_year
            ]

            for season in seasons:
                season_param = f"{season.split('-')[0]}-{season.split('-')[1][-2:]}"
                params.update({"Season": season_param})
                reg_season_start_date = self.NBA_IMPORTANT_DATES[season][
                    "reg_season_start_date"
                ]
                reg_season_end_date = self.NBA_IMPORTANT_DATES[season][
                    "reg_season_end_date"
                ]
                postseason_start_date = self.NBA_IMPORTANT_DATES[season][
                    "postseason_start_date"
                ]
                postseason_end_date = self.NBA_IMPORTANT_DATES[season][
                    "postseason_end_date"
                ]

                # convert string dates to datetime objects
                reg_season_start_date = datetime.strptime(
                    reg_season_start_date, "%Y-%m-%d"
                )
                reg_season_end_date = datetime.strptime(reg_season_end_date, "%Y-%m-%d")
                postseason_start_date = datetime.strptime(
                    postseason_start_date, "%Y-%m-%d"
                )
                postseason_end_date = datetime.strptime(postseason_end_date, "%Y-%m-%d")

                for season_type in ["Regular Season", "Playoffs"]:
                    params.update({"SeasonType": season_type})
                    if season_type == "Playoffs":
                        # loop through each day between postseason start and end dates
                        dt = postseason_start_date
                        while dt <= postseason_end_date:
                            params.update(
                                {"DateTo": dt.strftime("%m/%d/%Y")}
                            )  # convert datetime object to string
                            url = base_url + "?" + urlencode(params)
                            dt += timedelta(days=1)
                            yield scrapy.Request(
                                url, headers=headers, callback=self.parse
                            )
                            dt += timedelta(days=1)
                    elif season_type == "Regular Season":
                        # loop through each day between regular season start and end dates
                        dt = reg_season_start_date
                        while dt <= reg_season_end_date:
                            params.update(
                                {"DateTo": dt.strftime("%m/%d/%Y")}
                            )  # convert datetime object to string
                            url = base_url + "?" + urlencode(params)
                            dt += timedelta(days=1)
                            yield scrapy.Request(
                                url, headers=headers, callback=self.parse
                            )

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
                season = season_info_result["season"]
                season_type = season_info_result["season_type"]
                date_param = datetime.strptime(yesterday_str, "%Y-%m-%d").strftime(
                    "%m/%d/%Y"
                )
                params.update(
                    {
                        "Season": season,
                        "DateTo": date_param,
                        "SeasonType": season_type,
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

                date_param = datetime.strptime(date_str, "%Y-%m-%d").strftime("%m/%d/%Y")
                season = season_info_result["season"]
                season_type = season_info_result["season_type"]

                params.update(
                    {"Season": season, "DateTo": date_param, "SeasonType": season_type}
                )
                url = base_url + "?" + urlencode(params)
                yield scrapy.Request(url, headers=headers, callback=self.parse)

    def parse(self, response):
        json_response = json.loads(response.body)
        row_set = json_response["resultSets"][0]["rowSet"]
        headers = json_response["resultSets"][0]["headers"]
        season = json_response["parameters"]["Season"]
        season_type = json_response["parameters"]["SeasonType"]
        to_date = json_response["parameters"]["DateTo"]

        for row in row_set:
            row_dict = dict(zip(headers, row))

            loader = NbaStatsPlayerGeneralTraditionalItemLoader(
                item=NbaStatsPlayerGeneralTraditionalItem()
            )
            loader.add_value("to_date", to_date)
            loader.add_value("season_year", season)
            loader.add_value("season_type", season_type)
            loader.add_value("player_id", row_dict["PLAYER_ID"])
            loader.add_value("player_name", row_dict["PLAYER_NAME"])
            loader.add_value("age", row_dict["AGE"])
            loader.add_value("gp", row_dict["GP"])
            loader.add_value("w", row_dict["W"])
            loader.add_value("l", row_dict["L"])
            loader.add_value("w_pct", row_dict["W_PCT"])
            loader.add_value("min", row_dict["MIN"])
            loader.add_value("fgm", row_dict["FGM"])
            loader.add_value("fga", row_dict["FGA"])
            loader.add_value("fg_pct", row_dict["FG_PCT"])
            loader.add_value("fg3m", row_dict["FG3M"])
            loader.add_value("fg3a", row_dict["FG3A"])
            loader.add_value("fg3_pct", row_dict["FG3_PCT"])
            loader.add_value("ftm", row_dict["FTM"])
            loader.add_value("fta", row_dict["FTA"])
            loader.add_value("ft_pct", row_dict["FT_PCT"])
            loader.add_value("oreb", row_dict["OREB"])
            loader.add_value("dreb", row_dict["DREB"])
            loader.add_value("reb", row_dict["REB"])
            loader.add_value("ast", row_dict["AST"])
            loader.add_value("tov", row_dict["TOV"])
            loader.add_value("stl", row_dict["STL"])
            loader.add_value("blk", row_dict["BLK"])
            loader.add_value("blka", row_dict["BLKA"])
            loader.add_value("pf", row_dict["PF"])
            loader.add_value("pfd", row_dict["PFD"])
            loader.add_value("pts", row_dict["PTS"])
            loader.add_value("plus_minus", row_dict["PLUS_MINUS"])
            loader.add_value("nba_fantasy_pts", row_dict["NBA_FANTASY_PTS"])
            loader.add_value("dd2", row_dict["DD2"])
            loader.add_value("td3", row_dict["TD3"])

            yield loader.load_item()
