from itemloaders.processors import MapCompose, TakeFirst
from scrapy.loader import ItemLoader

from .items import CoversHistoricScoresAndOddsItem


class CoversHistoricScoresAndOddsItemLoader(ItemLoader):
    default_item_class = CoversHistoricScoresAndOddsItem
    default_output_processor = TakeFirst()

    game_id_in = MapCompose(str.strip)
    game_datetime_in = MapCompose(str.strip)
    home_team_in = MapCompose(str.strip)
    away_team_in = MapCompose(str.strip)
    home_score_in = MapCompose(int)
    away_score_in = MapCompose(int)
    open_line_in = MapCompose(float)
