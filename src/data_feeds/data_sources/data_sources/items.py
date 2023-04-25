import scrapy

# ADD NEW ITEMS HERE


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
