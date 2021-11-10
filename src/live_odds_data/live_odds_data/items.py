from scrapy import Item, Field


class Game(Item):
    date = Field()
    time = Field()
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
    link = Field()

    pass
