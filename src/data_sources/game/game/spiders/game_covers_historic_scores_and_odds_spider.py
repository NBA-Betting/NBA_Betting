import os
import sys
from datetime import datetime, timedelta
from urllib.parse import urlencode

import pytz
import scrapy
from dotenv import load_dotenv

from ..item_loaders import CoversHistoricScoresAndOddsItemLoader
from ..items import CoversHistoricScoresAndOddsItem

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../../../.."))

from utils.data_source_utils import BaseSpider, BaseSpiderZyte

load_dotenv()
ZYTE_API_KEY = os.environ.get("ZYTE_API_KEY")


class GameCoversHistoricScoresAndOddsSpider(BaseSpider):
    name = "game_covers_historic_scores_and_odds_spider"
    pipeline_name = "CoversHistoricScoresAndOddsPipeline"
    project_section = "game"
    first_season_start_year = 1996

    custom_settings = BaseSpider.create_pipeline_settings(project_section, pipeline_name)

    def __init__(self, dates, save_data=False, view_data=True, *args, **kwargs):
        super().__init__(
            dates, save_data=save_data, view_data=view_data, *args, **kwargs
        )

    def setup_dates(self, dates_input):
        # Validate the input
        if not isinstance(dates_input, str):
            raise TypeError(f"dates_input should be str, but got {type(dates_input)}")

        if dates_input == "daily_update":
            # Define the timezone
            denver_tz = pytz.timezone("America/Denver")
            # Get 'now' for the Denver timezone
            denver_now = datetime.now(denver_tz)
            # Get today's date
            todays_date = denver_now.strftime("%Y-%m-%d")
            # Calculate yesterday's date by subtracting a day
            yesterdays_date = (denver_now - timedelta(days=1)).strftime("%Y-%m-%d")
            return [todays_date, yesterdays_date]

        # Check if input is a season or multiple seasons
        if "-" not in dates_input:
            seasons_list = [season.strip() for season in dates_input.split(",")]
            # Generate all dates in those seasons
            dates = []
            for season in seasons_list:
                dates.extend(self._generate_dates_for_season(int(season)))
            return dates

        # Assuming the remaining input would be individual dates
        dates_list = dates_input.split(",")
        for date_str in dates_list:
            try:
                # Validate each date
                datetime.strptime(date_str.strip(), "%Y-%m-%d")
            except ValueError:
                raise ValueError(
                    f"Invalid date format: {date_str}. Date format should be 'YYYY-MM-DD'"
                )

        return [date_str.strip() for date_str in dates_list]

    def start_requests(self):
        base_url = "https://www.covers.com/sports/NBA/matchups"
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }
        params = {}

        for date_str in self.dates:
            params["selectedDate"] = date_str
            url = base_url + "?" + urlencode(params)
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # Select the main container
        matchups = response.css(".cmg_matchups_list .cmg_matchup_game_box.cmg_game_data")

        for matchup in matchups:
            loader = CoversHistoricScoresAndOddsItemLoader(
                item=CoversHistoricScoresAndOddsItem(), selector=matchup
            )

            # Scrape the data using the data-* attributes
            loader.add_css("game_datetime", "::attr(data-game-date)")
            loader.add_css("home_team", "::attr(data-home-team-shortname-search)")
            loader.add_css("away_team", "::attr(data-away-team-shortname-search)")
            loader.add_css("open_line", "::attr(data-game-odd)")
            loader.add_css("home_score", "::attr(data-home-score)")
            loader.add_css("away_score", "::attr(data-away-score)")

            yield loader.load_item()


class GameCoversHistoricScoresAndOddsSpiderZyte(
    BaseSpiderZyte, GameCoversHistoricScoresAndOddsSpider
):
    name = "game_covers_historic_scores_and_odds_spider_zyte"
    pipeline_name = "CoversHistoricScoresAndOddsPipeline"
    project_section = "game"

    # Merge pipeline settings into custom_settings
    pipeline_settings = BaseSpiderZyte.create_pipeline_settings(
        project_section, pipeline_name
    )
    custom_settings = {**BaseSpiderZyte.custom_settings, **pipeline_settings}
