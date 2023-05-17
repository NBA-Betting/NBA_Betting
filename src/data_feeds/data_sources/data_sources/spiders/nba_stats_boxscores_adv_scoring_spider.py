import json
import os
import re
import sys
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlencode, urlparse

import pytz
import scrapy
from data_sources.item_loaders import NbaStatsBoxscoresAdvScoringItemLoader
from data_sources.items import NbaStatsBoxscoresAdvScoringItem
from data_sources.spiders.base_spider import BaseSpider

sys.path.append("../../../../")
from passkeys import API_KEY_ZYTE


class NbaStatsBoxscoresAdvScoringSpider(BaseSpider):
    name = "nba_stats_boxscores_adv_scoring_spider"
    allowed_domains = ["www.nba.com"]
    custom_settings = {
        "ITEM_PIPELINES": {
            "data_sources.pipelines.NbaStatsBoxscoresAdvScoringPipeline": 300
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
                "ZYTE_API_KEY": API_KEY_ZYTE,
                "ZYTE_API_TRANSPARENT_MODE": True,
                "ZYTE_API_ENABLED": True,
            }
        )

    first_season = 1976  # This data source goes back to 1946-1947, but the NBA-ABA merger was in 1976

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

        for season, dates in self.NBA_IMPORTANT_DATES.items():
            reg_season_start_date = datetime.strptime(
                dates["reg_season_start_date"], "%Y-%m-%d"
            )
            postseason_end_date = datetime.strptime(
                dates["postseason_end_date"], "%Y-%m-%d"
            )

            if reg_season_start_date <= date_obj <= postseason_end_date:
                year1, year2 = season.split("-")
                return {
                    "info": f"{year1}-{year2[-2:]}",
                    "error": None,
                }
        return {"info": None, "error": "Unable to find season information"}

    def start_requests(self):
        base_url = "https://stats.nba.com/stats/playergamelogs"
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
            "MeasureType": "Scoring",
            "PerMode": "Totals",
        }

        if self.dates == "all":
            seasons = [
                f"{season.split('-')[0]}-{season.split('-')[1][-2:]}"
                for season in self.NBA_IMPORTANT_DATES.keys()
            ]
            for season in seasons:
                params.update({"Season": season})
                for season_type in ["Regular+Season", "PlayIn", "Playoffs"]:
                    params.update({"SeasonType": season_type})
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
                season = season_info_result["info"]
                date_param = datetime.strptime(yesterday_str, "%Y-%m-%d").strftime(
                    "%m/%d/%Y"
                )
                params.update(
                    {
                        "Season": season,
                        "DateFrom": date_param,
                        "DateTo": date_param,
                    }
                )
                for season_type in ["Regular Season", "PlayIn", "Playoffs"]:
                    params.update({"SeasonType": season_type})
                    url = base_url + "?" + urlencode(params)
                    yield scrapy.Request(url, headers=headers, callback=self.parse)

        else:
            for season_type in ["Regular Season", "PlayIn", "Playoffs"]:
                params.update({"SeasonType": season_type})
                for date_str in self.dates:
                    season_info_result = self.find_season_information(date_str)
                    if season_info_result["error"]:
                        print(
                            f"Error: {season_info_result['error']} for the date {date_str}"
                        )
                        self.handle_failed_date(date_str, "find_season_information")
                        continue
                    date_param = datetime.strptime(date_str, "%Y-%m-%d").strftime(
                        "%m/%d/%Y"
                    )

                    season = season_info_result["info"]

                    params.update(
                        {"Season": season, "DateFrom": date_param, "DateTo": date_param}
                    )
                    url = base_url + "?" + urlencode(params)
                    yield scrapy.Request(url, headers=headers, callback=self.parse)

    def parse(self, response):
        json_response = json.loads(response.body)
        row_set = json_response["resultSets"][0]["rowSet"]
        headers = json_response["resultSets"][0]["headers"]
        season = json_response["parameters"]["SeasonYear"]
        season_type = json_response["parameters"]["SeasonType"]

        for row in row_set:
            row_dict = dict(zip(headers, row))

            loader = NbaStatsBoxscoresAdvScoringItemLoader(
                item=NbaStatsBoxscoresAdvScoringItem()
            )
            loader.add_value("season_year", season)
            loader.add_value("season_type", season_type)
            loader.add_value("player_id", row_dict["PLAYER_ID"])
            loader.add_value("player_name", row_dict["PLAYER_NAME"])
            loader.add_value("nickname", row_dict["NICKNAME"])
            loader.add_value("team_id", row_dict["TEAM_ID"])
            loader.add_value("team_abbreviation", row_dict["TEAM_ABBREVIATION"])
            loader.add_value("team_name", row_dict["TEAM_NAME"])
            loader.add_value("game_id", row_dict["GAME_ID"])
            loader.add_value("game_date", row_dict["GAME_DATE"])
            loader.add_value("matchup", row_dict["MATCHUP"])
            loader.add_value("w_l", row_dict["WL"])
            loader.add_value("min", row_dict["MIN"])
            loader.add_value("pct_fga_2pt", row_dict["PCT_FGA_2PT"])
            loader.add_value("pct_fga_3pt", row_dict["PCT_FGA_3PT"])
            loader.add_value("pct_pts_2pt", row_dict["PCT_PTS_2PT"])
            loader.add_value("pct_pts_2pt_mr", row_dict["PCT_PTS_2PT_MR"])
            loader.add_value("pct_pts_3pt", row_dict["PCT_PTS_3PT"])
            loader.add_value("pct_pts_fb", row_dict["PCT_PTS_FB"])
            loader.add_value("pct_pts_ft", row_dict["PCT_PTS_FT"])
            loader.add_value("pct_pts_off_tov", row_dict["PCT_PTS_OFF_TOV"])
            loader.add_value("pct_pts_paint", row_dict["PCT_PTS_PAINT"])
            loader.add_value("pct_ast_2pm", row_dict["PCT_AST_2PM"])
            loader.add_value("pct_uast_2pm", row_dict["PCT_UAST_2PM"])
            loader.add_value("pct_ast_3pm", row_dict["PCT_AST_3PM"])
            loader.add_value("pct_uast_3pm", row_dict["PCT_UAST_3PM"])
            loader.add_value("pct_ast_fgm", row_dict["PCT_AST_FGM"])
            loader.add_value("pct_uast_fgm", row_dict["PCT_UAST_FGM"])
            loader.add_value("fgm", row_dict["FGM"])
            loader.add_value("fga", row_dict["FGA"])
            loader.add_value("fg_pct", row_dict["FG_PCT"])

            yield loader.load_item()
