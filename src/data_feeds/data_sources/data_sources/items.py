import scrapy

# ADD NEW ITEMS HERE


class FivethirtyeightPlayerItem(scrapy.Item):
    priority = scrapy.Field()
    to_date = scrapy.Field()
    player_name = scrapy.Field()
    player_id = scrapy.Field()
    season = scrapy.Field()
    poss = scrapy.Field()
    mp = scrapy.Field()
    raptor_box_offense = scrapy.Field()
    raptor_box_defense = scrapy.Field()
    raptor_box_total = scrapy.Field()
    raptor_onoff_offense = scrapy.Field()
    raptor_onoff_defense = scrapy.Field()
    raptor_onoff_total = scrapy.Field()
    raptor_offense = scrapy.Field()
    raptor_defense = scrapy.Field()
    raptor_total = scrapy.Field()
    war_total = scrapy.Field()
    war_reg_season = scrapy.Field()
    war_playoffs = scrapy.Field()
    predator_offense = scrapy.Field()
    predator_defense = scrapy.Field()
    predator_total = scrapy.Field()
    pace_impact = scrapy.Field()


class InpredictableWPAItem(scrapy.Item):
    rnk = scrapy.Field()
    player = scrapy.Field()
    pos = scrapy.Field()
    gms = scrapy.Field()
    wpa = scrapy.Field()
    ewpa = scrapy.Field()
    clwpa = scrapy.Field()
    gbwpa = scrapy.Field()
    sh = scrapy.Field()
    to = scrapy.Field()
    ft = scrapy.Field()
    reb = scrapy.Field()
    ast = scrapy.Field()
    stl = scrapy.Field()
    blk = scrapy.Field()
    kwpa = scrapy.Field()
    to_date = scrapy.Field()
