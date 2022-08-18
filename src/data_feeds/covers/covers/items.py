from scrapy import Item, Field


class LiveGameResultsItem(Item):
    date = Field()
    home_team = Field()
    away_team = Field()
    home_score = Field()
    away_score = Field()


class LiveGameItem(Item):
    id_num = Field()
    date = Field()
    time = Field()
    league_year = Field()
    home_team_full_name = Field()
    home_team_short_name = Field()
    away_team_full_name = Field()
    away_team_short_name = Field()
    open_line_away = Field()
    open_line_home = Field()
    fanduel_line_away = Field()
    fanduel_line_price_away = Field()
    fanduel_line_home = Field()
    fanduel_line_price_home = Field()
    draftkings_line_away = Field()
    draftkings_line_price_away = Field()
    draftkings_line_home = Field()
    draftkings_line_price_home = Field()
    covers_away_consenses = Field()
    covers_home_consenses = Field()
    link = Field()


class PastGameItem(Item):
    team = Field()
    game_id = Field()
    game_url = Field()
    date = Field()
    league_year = Field()
    home = Field()
    opponent = Field()
    score = Field()
    opponent_score = Field()
    result = Field()
    spread = Field()
    spread_result = Field()
