from itemloaders.processors import MapCompose, TakeFirst
from scrapy.loader import ItemLoader

from .items import CoversHistoricScoresAndOddsItem


def handle_none_to_int(value):
    # Check if the list is empty, which would indicate a missing value.
    if not value or value[0] is None or value[0] == "":
        return None
    # If there's a value, convert it to an integer.
    return int(value[0])


def handle_none_to_float(value):
    if not value or value[0] is None or value[0] == "":
        return None
    return float(value[0])


class CoversHistoricScoresAndOddsItemLoader(ItemLoader):
    default_item_class = CoversHistoricScoresAndOddsItem
    default_output_processor = TakeFirst()

    game_id_in = MapCompose(str.strip)
    game_datetime_in = MapCompose(str.strip)
    home_team_in = MapCompose(str.strip)
    away_team_in = MapCompose(str.strip)
    home_score_in = handle_none_to_int
    away_score_in = handle_none_to_int
    open_line_in = handle_none_to_float

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
