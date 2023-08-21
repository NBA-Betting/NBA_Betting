import json
import os
import re
import sys
from datetime import datetime, timedelta
from urllib.parse import urlencode

import pytz
import scrapy
from dotenv import load_dotenv

from ..item_loaders import NbastatsGeneralOpponentItemLoader
from ..items import NbastatsGeneralOpponentItem

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../../../.."))

from utils.data_source_utils import (
    BaseSpider,
    BaseSpiderZyte,
    convert_season_to_short,
    find_season_information,
)

load_dotenv()
ZYTE_API_KEY = os.environ.get("ZYTE_API_KEY")


class TeamNbastatsGeneralOpponentSpider(BaseSpider):
    name = "team_nbastats_general_opponent_spider"
    pipeline_name = "NbastatsGeneralOpponentPipeline"
    project_section = "team"
    first_season_start_year = 1996

    custom_settings = BaseSpider.create_pipeline_settings(project_section, pipeline_name)

    def __init__(self, dates, save_data=False, view_data=True, *args, **kwargs):
        super().__init__(
            dates, save_data=save_data, view_data=view_data, *args, **kwargs
        )

    def start_requests(self):
        base_url = "https://stats.nba.com/stats/leaguedashteamstats"
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.nba.com",
            "Referer": "https://www.nba.com/",
        }
        params = {
            "MeasureType": "Opponent",
            "PerMode": "PerGame",
            "LeagueID": "00",
            "Season": "",
            "SeasonType": "",
            "DateFrom": "",
            "DateTo": "",
            "LastNGames": 0,
            "Month": 0,
            "OpponentTeamID": 0,
            "PORound": 0,
            "PaceAdjust": "N",
            "Period": 0,
            "PlusMinus": "N",
            "Rank": "N",
            "TeamID": 0,
        }

        for date_str in (
            [datetime.now(pytz.timezone("America/Denver"))]
            if self.dates == "daily_update"
            else self.dates
        ):
            date = (
                date_str
                if isinstance(date_str, datetime)
                else datetime.strptime(date_str, "%Y-%m-%d")
            )

            if self.dates == "daily_update":
                date -= timedelta(days=1)

            l2w_date = date - timedelta(days=14)
            to_date = date.strftime("%m/%d/%Y")
            from_date = l2w_date.strftime("%m/%d/%Y")

            try:
                season_info = find_season_information(date.strftime("%Y-%m-%d"))
            except Exception as e:
                print(e)
                self.errors["find_season_information"].append(e)
                continue

            season = convert_season_to_short(season_info["season"])
            season_type = season_info["season_type"]

            for from_date in [None, from_date]:
                params["DateFrom"] = from_date
                params["DateTo"] = to_date
                params["Season"] = season
                params["SeasonType"] = season_type

                if from_date is None:
                    params.pop("DateFrom")

                url = base_url + "?" + urlencode(params)
                yield scrapy.Request(url, headers=headers, callback=self.parse)

    def parse(self, response):
        json_response = json.loads(response.body)
        row_set = json_response["resultSets"][0]["rowSet"]
        headers = json_response["resultSets"][0]["headers"]
        to_date = json_response["parameters"]["DateTo"]
        season = json_response["parameters"]["Season"]
        season_type = json_response["parameters"]["SeasonType"]
        games = "all" if json_response["parameters"]["DateFrom"] in [None, ""] else "l2w"

        for row in row_set:
            row_dict = dict(zip(headers, row))

            loader = NbastatsGeneralOpponentItemLoader(
                item=NbastatsGeneralOpponentItem()
            )

            loader.add_value("team_name", row_dict["TEAM_NAME"])
            loader.add_value("to_date", to_date)
            loader.add_value("season", season)
            loader.add_value("season_type", season_type)
            loader.add_value("games", games)
            loader.add_value("gp", row_dict["GP"])
            loader.add_value("w", row_dict["W"])
            loader.add_value("l", row_dict["L"])
            loader.add_value("w_pct", row_dict["W_PCT"])
            loader.add_value("min", row_dict["MIN"])
            loader.add_value("opp_fgm", row_dict["OPP_FGM"])
            loader.add_value("opp_fga", row_dict["OPP_FGA"])
            loader.add_value("opp_fg_pct", row_dict["OPP_FG_PCT"])
            loader.add_value("opp_fg3m", row_dict["OPP_FG3M"])
            loader.add_value("opp_fg3a", row_dict["OPP_FG3A"])
            loader.add_value("opp_fg3_pct", row_dict["OPP_FG3_PCT"])
            loader.add_value("opp_ftm", row_dict["OPP_FTM"])
            loader.add_value("opp_fta", row_dict["OPP_FTA"])
            loader.add_value("opp_ft_pct", row_dict["OPP_FT_PCT"])
            loader.add_value("opp_oreb", row_dict["OPP_OREB"])
            loader.add_value("opp_dreb", row_dict["OPP_DREB"])
            loader.add_value("opp_reb", row_dict["OPP_REB"])
            loader.add_value("opp_ast", row_dict["OPP_AST"])
            loader.add_value("opp_tov", row_dict["OPP_TOV"])
            loader.add_value("opp_stl", row_dict["OPP_STL"])
            loader.add_value("opp_blk", row_dict["OPP_BLK"])
            loader.add_value("opp_blka", row_dict["OPP_BLKA"])
            loader.add_value("opp_pf", row_dict["OPP_PF"])
            loader.add_value("opp_pfd", row_dict["OPP_PFD"])
            loader.add_value("opp_pts", row_dict["OPP_PTS"])
            loader.add_value("plus_minus", row_dict["PLUS_MINUS"])

            yield loader.load_item()


class TeamNbastatsGeneralOpponentSpiderZyte(
    BaseSpiderZyte, TeamNbastatsGeneralOpponentSpider
):
    name = "team_nbastats_general_opponent_spider_zyte"
    pipeline_name = "NbastatsGeneralOpponentPipeline"
    project_section = "team"

    # Merge pipeline settings into custom_settings
    pipeline_settings = BaseSpiderZyte.create_pipeline_settings(
        project_section, pipeline_name
    )
    custom_settings = {**BaseSpiderZyte.custom_settings, **pipeline_settings}
