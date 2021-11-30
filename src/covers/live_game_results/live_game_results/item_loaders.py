import datetime
import pytz
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst

# Grabs the year based on the day before the script is run
# which should be the same day as the game.
today = datetime.datetime.now(pytz.timezone("America/Denver"))
yesterday = today - datetime.timedelta(5)
yesterday_year = yesterday.strftime("%Y")


class GameItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: str.split(x, ".")[1],
        str.strip,
        lambda x: x + " " + yesterday_year,
    )
    home_score_in = MapCompose(int)
    away_score_in = MapCompose(int)
