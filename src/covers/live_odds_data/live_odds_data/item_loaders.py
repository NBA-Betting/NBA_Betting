import datetime
import pytz
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, Identity, TakeFirst

# Grabs the year based on the day the script is run
# which should be the same day as the game.
game_year = datetime.datetime.now(pytz.timezone("America/Denver")).strftime(
    "%Y"
)


class GameLoader(ItemLoader):
    default_input_processor = Identity()
    default_output_processor = TakeFirst()

    id_num_in = MapCompose(lambda x: x[-6:], int)
    date_in = MapCompose(str.strip, lambda x: x + " " + game_year)
    time_in = MapCompose(str.strip)
    draftkings_line_price_away_in = MapCompose(int)
    draftkings_line_price_home_in = MapCompose(int)
    fanduel_line_price_away_in = MapCompose(int)
    fanduel_line_price_home_in = MapCompose(int)
    open_line_away_in = MapCompose(
        lambda x: 0 if x in ("PK", "pk") else x, float
    )
    open_line_home_in = MapCompose(
        lambda x: 0 if x in ("PK", "pk") else x, float
    )
    fanduel_line_away_in = MapCompose(
        lambda x: 0 if x in ("PK", "pk") else x, float
    )
    fanduel_line_home_in = MapCompose(
        lambda x: 0 if x in ("PK", "pk") else x, float
    )
    draftkings_line_away_in = MapCompose(
        lambda x: 0 if x in ("PK", "pk") else x, float
    )
    draftkings_line_home_in = MapCompose(
        lambda x: 0 if x in ("PK", "pk") else x, float
    )
    covers_away_consenses_in = MapCompose(
        lambda x: x.replace("%", ""), str.strip, int, lambda x: x / 100
    )
    covers_home_consenses_in = MapCompose(
        lambda x: x.replace("%", ""), str.strip, int, lambda x: x / 100
    )
