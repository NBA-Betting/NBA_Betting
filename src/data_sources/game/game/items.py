import scrapy


class CoversHistoricScoresAndOddsItem(scrapy.Item):
    game_id = scrapy.Field()
    game_datetime = scrapy.Field()
    home_team = scrapy.Field()
    away_team = scrapy.Field()
    home_score = scrapy.Field()
    away_score = scrapy.Field()
    open_line = scrapy.Field()
