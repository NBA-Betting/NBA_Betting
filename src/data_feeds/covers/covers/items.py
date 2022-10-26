from scrapy import Item, Field


class LiveGameItem(Item):
    id_num = Field()
    date = Field()
    time = Field()
    league_year = Field()
    home_team = Field()
    away_team = Field()
    spread = Field()
    fanduel_line_away = Field()
    fanduel_line_price_away = Field()
    fanduel_line_home = Field()
    fanduel_line_price_home = Field()
    draftkings_line_away = Field()
    draftkings_line_price_away = Field()
    draftkings_line_home = Field()
    draftkings_line_price_home = Field()
    covers_away_consensus = Field()
    covers_home_consensus = Field()
    game_url = Field()


class PastGameItem(Item):
    id_num = Field()
    date = Field()
    league_year = Field()
    team = Field()
    opponent = Field()
    score = Field()
    opponent_score = Field()
    result = Field()
    spread = Field()
    spread_result = Field()
    game_url = Field()
    is_home = Field()
