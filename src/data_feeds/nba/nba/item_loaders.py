import datetime
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst, Join

team_abbreviations = {
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "New Jersey Nets": "BKN",
    "Toronto Raptors": "TOR",
    "New York Knicks": "NYK",
    "Philadelphia 76ers": "PHI",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Detroit Pistons": "DET",
    "Indiana Pacers": "IND",
    "Milwaukee Bucks": "MIL",
    "Atlanta Hawks": "ATL",
    "Charlotte Hornets": "CHA",
    "Charlotte Bobcats": "CHA",
    "Miami Heat": "MIA",
    "Orlando Magic": "ORL",
    "Washington Wizards": "WAS",
    "Denver Nuggets": "DEN",
    "Minnesota Timberwolves": "MIN",
    "Oklahoma City Thunder": "OKC",
    "Seattle SuperSonics": "OKC",
    "Portland Trail Blazers": "POR",
    "Utah Jazz": "UTA",
    "Golden State Warriors": "GSW",
    "Phoenix Suns": "PHX",
    "Sacramento Kings": "SAC",
    "LA Clippers": "LAC",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Dallas Mavericks": "DAL",
    "Houston Rockets": "HOU",
    "Memphis Grizzlies": "MEM",
    "San Antonio Spurs": "SAS",
}


class NBA_TraditionalStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, float)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: datetime.datetime.strptime(
            x.strip("Date To : "), "%m/%d/%Y"
        ).strftime("%Y%m%d")
    )
    team_in = MapCompose(str.strip)
    gp_in = MapCompose(str.strip, int)
    win_in = MapCompose(str.strip, int)
    loss_in = MapCompose(str.strip, int)


class NBA_AdvancedStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, float)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: datetime.datetime.strptime(
            x.strip("Date To : "), "%m/%d/%Y"
        ).strftime("%Y%m%d")
    )
    team_in = MapCompose(str.strip)
    poss_in = MapCompose(lambda x: int(x.strip().replace(",", "")))


class NBA_FourFactorsStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, float)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: datetime.datetime.strptime(
            x.strip("Date To : "), "%m/%d/%Y"
        ).strftime("%Y%m%d")
    )
    team_in = MapCompose(str.strip)


class NBA_MiscStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, float)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: datetime.datetime.strptime(
            x.strip("Date To : "), "%m/%d/%Y"
        ).strftime("%Y%m%d")
    )
    team_in = MapCompose(str.strip)


class NBA_ScoringStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, float)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: datetime.datetime.strptime(
            x.strip("Date To : "), "%m/%d/%Y"
        ).strftime("%Y%m%d")
    )
    team_in = MapCompose(str.strip)


class NBA_OpponentStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, float)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: datetime.datetime.strptime(
            x.strip("Date To : "), "%m/%d/%Y"
        ).strftime("%Y%m%d")
    )
    team_in = MapCompose(str.strip)


class NBA_SpeedDistanceStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, float)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: datetime.datetime.strptime(
            x.strip("Date To : "), "%m/%d/%Y"
        ).strftime("%Y%m%d")
    )
    team_in = MapCompose(str.strip)


class NBA_ShootingStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, float)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: datetime.datetime.strptime(
            x.strip("Date To : "), "%m/%d/%Y"
        ).strftime("%Y%m%d")
    )
    team_in = MapCompose(str.strip)


class NBA_OpponentShootingStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, float)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: datetime.datetime.strptime(
            x.strip("Date To : "), "%m/%d/%Y"
        ).strftime("%Y%m%d")
    )
    team_in = MapCompose(str.strip)


class NBA_HustleStatsItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip, float)
    default_output_processor = TakeFirst()

    date_in = MapCompose(
        lambda x: datetime.datetime.strptime(
            x.strip("Date To : "), "%m/%d/%Y"
        ).strftime("%Y%m%d")
    )
    team_in = MapCompose(str.strip)
