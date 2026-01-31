from itemloaders.processors import MapCompose, TakeFirst
from scrapy.loader import ItemLoader

from .items import CoversHistoricScoresAndOddsItem


def convert_to_int(value):
    """Convert a single value to int, handling None and empty strings."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def convert_to_float(value):
    """Convert a single value to float, handling None and empty strings."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


class CoversHistoricScoresAndOddsItemLoader(ItemLoader):
    default_item_class = CoversHistoricScoresAndOddsItem
    default_output_processor = TakeFirst()

    game_id_in = MapCompose(str.strip)
    game_datetime_in = MapCompose(str.strip)
    home_team_in = MapCompose(str.strip)
    away_team_in = MapCompose(str.strip)
    home_score_in = MapCompose(convert_to_int)
    away_score_in = MapCompose(convert_to_int)
    open_line_in = MapCompose(convert_to_float)

    def load_item(self):
        item = super().load_item()
        fields_to_ensure = [
            "game_id",
            "game_datetime",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
            "open_line",
        ]
        for field in fields_to_ensure:
            if field not in item:
                item[field] = None

        return item
