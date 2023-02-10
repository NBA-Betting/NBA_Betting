import datetime
import re

import pytz
from itemloaders.processors import Identity, MapCompose, TakeFirst
from scrapy.loader import ItemLoader

# Grabs the year based on the day before the script is run
# which should be the same day as the game.
today = datetime.datetime.now(pytz.timezone("America/Denver"))
yesterday = today - datetime.timedelta(5)
yesterday_year = yesterday.strftime("%Y")

game_year = datetime.datetime.now(pytz.timezone("America/Denver")).strftime("%Y")

game_month = datetime.datetime.now(pytz.timezone("America/Denver")).strftime("%m")

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
    "boston-celtics": "BOS",
    "brooklyn-nets": "BKN",
    "new-jersey-nets": "BKN",
    "toronto-raptors": "TOR",
    "new-york-knicks": "NYK",
    "philadelphia-76ers": "PHI",
    "chicago-bulls": "CHI",
    "cleveland-cavaliers": "CLE",
    "detroit-pistons": "DET",
    "indiana-pacers": "IND",
    "milwaukee-bucks": "MIL",
    "atlanta-hawks": "ATL",
    "charlotte-hornets": "CHA",
    "charlotte-bobcats": "CHA",
    "miami-heat": "MIA",
    "orlando-magic": "ORL",
    "washington-wizards": "WAS",
    "denver-nuggets": "DEN",
    "minnesota-timberwolves": "MIN",
    "oklahoma-city-thunder": "OKC",
    "portland-trail-blazers": "POR",
    "utah-jazz": "UTA",
    "golden-state-warriors": "GSW",
    "phoenix-suns": "PHX",
    "sacramento-kings": "SAC",
    "los-angeles-clippers": "LAC",
    "los-angeles-lakers": "LAL",
    "dallas-mavericks": "DAL",
    "houston-rockets": "HOU",
    "memphis-grizzlies": "MEM",
    "new-orleans-pelicans": "NOP",
    "new-orleans-hornets": "NOP",
    "san-antonio-spurs": "SAS",
}


def get_league_year(game_year, game_month):
    if int(game_month) in [10, 11, 12]:
        start_year = int(game_year)
        end_year = int(game_year) + 1
    else:
        start_year = int(game_year) - 1
        end_year = int(game_year)
    return f"{start_year}-{end_year}"


def get_score(score, group):
    match = re.search(r"(\d+)-(\d+)", score)
    return match.group(group)


class LiveGameItemLoader(ItemLoader):
    default_input_processor = Identity()
    default_output_processor = TakeFirst()

    id_num_in = MapCompose(lambda x: x[-6:], int)
    date_in = MapCompose(
        str.strip,
        lambda x: x + " " + game_year,
        lambda x: datetime.datetime.strptime(x, "%b %d %Y").strftime("%Y%m%d"),
    )
    league_year_in = MapCompose(lambda x: get_league_year(x, game_month=game_month))
    home_team_in = MapCompose(str.strip, lambda x: team_abrv_map[x])
    away_team_in = MapCompose(str.strip, lambda x: team_abrv_map[x])
    time_in = MapCompose(str.strip)
    draftkings_line_price_away_in = MapCompose(int)
    draftkings_line_price_home_in = MapCompose(int)
    fanduel_line_price_away_in = MapCompose(int)
    fanduel_line_price_home_in = MapCompose(int)
    spread_in = MapCompose(lambda x: 0 if x in ("PK", "pk") else x, float)
    fanduel_line_away_in = MapCompose(lambda x: 0 if x in ("PK", "pk") else x, float)
    fanduel_line_home_in = MapCompose(lambda x: 0 if x in ("PK", "pk") else x, float)
    draftkings_line_away_in = MapCompose(lambda x: 0 if x in ("PK", "pk") else x, float)
    draftkings_line_home_in = MapCompose(lambda x: 0 if x in ("PK", "pk") else x, float)
    covers_away_consensus_in = MapCompose(
        lambda x: x.replace("%", ""), str.strip, int, lambda x: x / 100
    )
    covers_home_consensus_in = MapCompose(
        lambda x: x.replace("%", ""), str.strip, int, lambda x: x / 100
    )
