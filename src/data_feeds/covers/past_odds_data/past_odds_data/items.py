from scrapy import Field, Item


class Game(Item):
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
