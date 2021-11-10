from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, Identity, TakeFirst


class GameLoader(ItemLoader):
    default_input_processor = Identity()
    default_output_processor = TakeFirst()

    date_in = MapCompose(str.strip)
    time_in = MapCompose(str.strip)
    draftkings_over_under_in = MapCompose(str.strip)
    # draftkings_line_home_in = MapCompose(str.strip)
    fanduel_over_under_in = MapCompose(str.strip)
    # fanduel_line_home_in = MapCompose(str.strip)
