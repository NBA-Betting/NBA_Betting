from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst


class GameItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip)
    default_output_processor = TakeFirst()

    home_score_in = MapCompose(int)
    away_score_in = MapCompose(int)
