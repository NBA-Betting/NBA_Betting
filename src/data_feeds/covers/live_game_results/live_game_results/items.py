from scrapy import Item, Field


class GameItem(Item):
    date = Field()
    home_team = Field()
    away_team = Field()
    home_score = Field()
    away_score = Field()
