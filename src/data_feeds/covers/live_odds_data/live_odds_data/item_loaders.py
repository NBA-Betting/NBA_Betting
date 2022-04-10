import datetime
import pytz
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, Identity, TakeFirst

# Grabs the year based on the day the script is run
# which should be the same day as the game!!!!!
game_year = datetime.datetime.now(pytz.timezone("America/Denver")).strftime(
    "%Y"
)

game_month = datetime.datetime.now(pytz.timezone("America/Denver")).strftime(
    "%m"
)

team_abrv_map = {
    "BK": "BKN",
    "BOS": "BOS",
    "MIL": "MIL",
    "ATL": "ATL",
    "CHA": "CHA",
    "CHI": "CHI",
    "CLE": "CLE",
    "DAL": "DAL",
    "DEN": "DEN",
    "DET": "DET",
    "GS": "GSW",
    "HOU": "HOU",
    "IND": "IND",
    "LAC": "LAC",
    "LAL": "LAL",
    "MEM": "MEM",
    "MIA": "MIA",
    "MIN": "MIN",
    "NO": "NOP",
    "NY": "NYK",
    "OKC": "OKC",
    "ORL": "ORL",
    "PHI": "PHI",
    "PHO": "PHX",
    "POR": "POR",
    "SA": "SAS",
    "SAC": "SAC",
    "TOR": "TOR",
    "UTA": "UTA",
    "WAS": "WAS",
}


def get_league_year(game_year, game_month):
    if int(game_year) in [10, 11, 12]:
        start_year = int(game_year)
        end_year = int(game_year) + 1
    else:
        start_year = int(game_year) - 1
        end_year = int(game_year)
    return f"{start_year}-{end_year}"


class GameLoader(ItemLoader):
    default_input_processor = Identity()
    default_output_processor = TakeFirst()

    id_num_in = MapCompose(lambda x: x[-6:], int)
    date_in = MapCompose(
        str.strip,
        lambda x: x + " " + game_year,
        lambda x: datetime.datetime.strptime(x, "%b %d %Y").strftime("%Y%m%d"),
    )
    league_year_in = MapCompose(
        lambda x: get_league_year(x, game_month=game_month)
    )
    home_team_short_name_in = MapCompose(str.strip, lambda x: team_abrv_map[x])
    away_team_short_name_in = MapCompose(str.strip, lambda x: team_abrv_map[x])
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
