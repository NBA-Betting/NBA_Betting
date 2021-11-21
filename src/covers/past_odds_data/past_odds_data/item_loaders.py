import re

from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst

team_abbreviations = {
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


def get_score(score, group):
    match = re.search(r"(\d+)-(\d+)", score)
    return match.group(group)


class GameLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip)
    default_output_processor = TakeFirst()

    team_in = MapCompose(lambda x: team_abbreviations[x])
    game_id_in = MapCompose(lambda x: re.sub("[^0-9]", "", x), int)
    home_in = MapCompose(lambda x: "@" not in x)
    opponent_in = MapCompose(lambda x: x.replace("@", ""), str.strip)
    result_in = MapCompose(str.strip, str.split)
    score_in = MapCompose(lambda x: get_score(x, 1), int)
    opponent_score_in = MapCompose(lambda x: get_score(x, 2), int)
    spread_in = MapCompose(str.strip, lambda x: 0 if x == "PK" else x, float)
