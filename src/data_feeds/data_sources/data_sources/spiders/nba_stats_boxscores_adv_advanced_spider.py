import json
import os
import re
import sys
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlencode, urlparse

import pytz
import scrapy
from data_sources.item_loaders import NbaStatsBoxscoresAdvAdvancedItemLoader
from data_sources.items import NbaStatsBoxscoresAdvAdvancedItem
from data_sources.spiders.base_spider import BaseSpider

sys.path.append("../../../../")
from passkeys import API_KEY_ZYTE


class NbaStatsBoxscoresAdvAdvancedSpider(BaseSpider):
    name = "nba_stats_boxscores_adv_advanced_spider"
    allowed_domains = ["www.nba.com"]
    custom_settings = {
        "ITEM_PIPELINES": {
            "data_sources.pipelines.NbaStatsBoxscoresAdvAdvancedPipeline": 300
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
            "MeasureType": "Advanced",
            "PerMode": "Totals",
        }

        if self.dates == "all":
            seasons = [
                f"{season.split('-')[0]}-{season.split('-')[1][-2:]}"
                for season in self.NBA_IMPORTANT_DATES.keys()
            ]
            for season in seasons:
                params.update({"Season": season})
                for season_type in ["Regular Season", "PlayIn", "Playoffs"]:
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
            print(row_dict)

            loader = NbaStatsBoxscoresAdvAdvancedItemLoader(
                item=NbaStatsBoxscoresAdvAdvancedItem()
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
            loader.add_value("e_off_rating", row_dict["E_OFF_RATING"])
            loader.add_value("off_rating", row_dict["OFF_RATING"])
            loader.add_value("sp_work_off_rating", row_dict["sp_work_OFF_RATING"])
            loader.add_value("e_def_rating", row_dict["E_DEF_RATING"])
            loader.add_value("def_rating", row_dict["DEF_RATING"])
            loader.add_value("sp_work_def_rating", row_dict["sp_work_DEF_RATING"])
            loader.add_value("e_net_rating", row_dict["E_NET_RATING"])
            loader.add_value("net_rating", row_dict["NET_RATING"])
            loader.add_value("sp_work_net_rating", row_dict["sp_work_NET_RATING"])
            loader.add_value("ast_pct", row_dict["AST_PCT"])
            loader.add_value("ast_to", row_dict["AST_TO"])
            loader.add_value("ast_ratio", row_dict["AST_RATIO"])
            loader.add_value("oreb_pct", row_dict["OREB_PCT"])
            loader.add_value("dreb_pct", row_dict["DREB_PCT"])
            loader.add_value("reb_pct", row_dict["REB_PCT"])
            loader.add_value("tm_tov_pct", row_dict["TM_TOV_PCT"])
            loader.add_value("e_tov_pct", row_dict["E_TOV_PCT"])
            loader.add_value("efg_pct", row_dict["EFG_PCT"])
            loader.add_value("ts_pct", row_dict["TS_PCT"])
            loader.add_value("usg_pct", row_dict["USG_PCT"])
            loader.add_value("e_usg_pct", row_dict["E_USG_PCT"])
            loader.add_value("e_pace", row_dict["E_PACE"])
            loader.add_value("pace", row_dict["PACE"])
            loader.add_value("pace_per40", row_dict["PACE_PER40"])
            loader.add_value("sp_work_pace", row_dict["sp_work_PACE"])
            loader.add_value("pie", row_dict["PIE"])
            loader.add_value("poss", row_dict["POSS"])
            loader.add_value("fgm", row_dict["FGM"])
            loader.add_value("fga", row_dict["FGA"])
            loader.add_value("fgm_pg", row_dict["FGM_PG"])
            loader.add_value("fga_pg", row_dict["FGA_PG"])
            loader.add_value("fg_pct", row_dict["FG_PCT"])

            yield loader.load_item()
