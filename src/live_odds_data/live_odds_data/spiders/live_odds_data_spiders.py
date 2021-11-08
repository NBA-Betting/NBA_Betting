import scrapy


class CoversLiveOddsSpider(scrapy.Spider):
    name = "covers_live_odds"
    allowed_domains = ["www.covers.com"]
    start_urls = ["https://www.covers.com/sport/basketball/nba/odds"]
