import datetime

import pandas as pd
import scrapy
from data_sources.item_loaders import FivethirtyeightPlayerItemLoader
from data_sources.items import FivethirtyeightPlayerItem

from .base_spider import BaseSpider


class FivethirtyeightPlayerSpider(BaseSpider):
    name = "fivethirtyeight_player_spider"
    allowed_domains = ["github.com", "fivethirtyeight.com"]

    custom_settings = {
        "ITEM_PIPELINES": {"data_sources.pipelines.FivethirtyeightPlayerPipeline": 300}
    }

    modern_csv_url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-raptor/modern_RAPTOR_by_player.csv"
    historical_csv_url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-raptor/historical_RAPTOR_by_player.csv"
    latest_csv_url = (
        "https://projects.fivethirtyeight.com/nba-model/2023/latest_RAPTOR_by_player.csv"
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

        if dates in ["all", "daily_update"]:
            self.dates = dates
        else:
            raise ValueError(
                f"Invalid date format: {dates}. Date format should be 'all' or 'daily_update'"
            )

    def start_requests(self):
        if self.dates == "all":
            yield scrapy.Request(url=self.historical_csv_url, callback=self.parse)
            yield scrapy.Request(url=self.modern_csv_url, callback=self.parse)
            yield scrapy.Request(url=self.latest_csv_url, callback=self.parse)
        elif self.dates == "daily_update":
            yield scrapy.Request(url=self.latest_csv_url, callback=self.parse)

    def parse(self, response):
        data = pd.read_csv(response.url)
        # Get yesterday's date
        yesterday = datetime.date.today() - datetime.timedelta(days=1)

        for index, row in data.iterrows():
            loader = FivethirtyeightPlayerItemLoader(
                item=FivethirtyeightPlayerItem(), selector=row
            )

            # Set the 'priority' field based on the response URL
            if response.url == self.latest_csv_url:
                loader.add_value("priority", 3)
            elif response.url == self.modern_csv_url:
                loader.add_value("priority", 2)
            else:  # response.url == self.historical_csv_url
                loader.add_value("priority", 1)

            # Set the 'to_date' field based on the response URL
            if response.url == self.latest_csv_url:
                loader.add_value("to_date", yesterday)
            else:
                season = row.get("season", None)
                if season:
                    # Get the postseason_end_date from the NBA_IMPORTANT_DATES dictionary
                    postseason_end_date = self.NBA_IMPORTANT_DATES[
                        f"{int(season)-1}-{season}"
                    ]["postseason_end_date"]
                    loader.add_value(
                        "to_date",
                        datetime.datetime.strptime(
                            postseason_end_date, "%Y-%m-%d"
                        ).date(),
                    )
                else:
                    loader.add_value("to_date", None)

            loader.add_value("player_name", row.get("player_name", None))
            loader.add_value("player_id", row.get("player_id", None))
            loader.add_value("season", row.get("season", None))
            loader.add_value("poss", row.get("poss", None))
            loader.add_value("mp", row.get("mp", None))
            loader.add_value("raptor_box_offense", row.get("raptor_box_offense", None))
            loader.add_value("raptor_box_defense", row.get("raptor_box_defense", None))
            loader.add_value("raptor_box_total", row.get("raptor_box_total", None))
            loader.add_value(
                "raptor_onoff_offense", row.get("raptor_onoff_offense", None)
            )
            loader.add_value(
                "raptor_onoff_defense", row.get("raptor_onoff_defense", None)
            )
            loader.add_value("raptor_onoff_total", row.get("raptor_onoff_total", None))
            loader.add_value("raptor_offense", row.get("raptor_offense", None))
            loader.add_value("raptor_defense", row.get("raptor_defense", None))
            loader.add_value("raptor_total", row.get("raptor_total", None))
            loader.add_value("war_total", row.get("war_total", None))
            loader.add_value("war_reg_season", row.get("war_reg_season", None))
            loader.add_value("war_playoffs", row.get("war_playoffs", None))
            loader.add_value("predator_offense", row.get("predator_offense", None))
            loader.add_value("predator_defense", row.get("predator_defense", None))
            loader.add_value("predator_total", row.get("predator_total", None))
            loader.add_value("pace_impact", row.get("pace_impact", None))

            yield loader.load_item()
