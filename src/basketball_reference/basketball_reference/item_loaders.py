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


class BR_StandingsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip)
    default_output_processor = TakeFirst()

    wins_in = MapCompose(int)
    losses_in = MapCompose(int)
    win_perc_in = MapCompose(float)
    points_scored_per_game_in = MapCompose(float)
    points_allowed_per_game_in = MapCompose(float)
    expected_wins_in = MapCompose(int)
    expected_losses_in = MapCompose(int)


class BR_TeamStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, int)
    default_output_processor = TakeFirst()

    team_in = MapCompose(str.strip)
    date_in = MapCompose(str.strip)
    fg_pct_in = MapCompose(float)
    fg3_pct_in = MapCompose(float)
    ft_pct_in = MapCompose(float)
    fg2_pct_in = MapCompose(float)


class BR_OpponentStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, int)
    default_output_processor = TakeFirst()

    team_in = MapCompose(str.strip)
    date_in = MapCompose(str.strip)
    opp_fg_pct_in = MapCompose(float)
    opp_fg3_pct_in = MapCompose(float)
    opp_ft_pct_in = MapCompose(float)
    opp_fg2_pct_in = MapCompose(float)
