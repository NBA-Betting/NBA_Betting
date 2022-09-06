import sys
import json
import datetime
import pytz
from scrapy import Request, Spider
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

sys.path.append("../../nba")

from items import NBA_ShootingStatsItem
from item_loaders import NBA_ShootingStatsItemLoader
from helpers import return_season_dates


class NBA_Shooting_Stats_Json_Spider(Spider):
    name = "NBA_shooting_stats_json_spider"

    current_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
    yesterday_datetime = current_datetime - datetime.timedelta(days=1)
    yesterday_day = yesterday_datetime.strftime("%d")
    yesterday_month = yesterday_datetime.strftime("%m")
    yesterday_year = yesterday_datetime.strftime("%Y")

    # Start of scraping and working backwards in time.
    # season_abbrv = '2022-23'
    # start_day = yesterday_day
    # start_month = yesterday_month
    # start_year = yesterday_year

    def __init__(self, season_dates=None, *args, **kwargs):
        super(NBA_Shooting_Stats_Json_Spider, self).__init__(*args, **kwargs)
        self.season_dates = season_dates
        self.start_urls = ["https://stats.nba.com/stats/leaguedashteamshotlocations"\
                           f"?Conference=&DateFrom=&DateTo={self.season_dates['final_month']}"\
                           f"%2F{self.season_dates['final_day']}%2F{self.season_dates['final_year']}"\
                           "&DistanceRange=By+Zone&Division=&GameScope=&GameSegment=&LastNGames=0"\
                           "&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0"\
                           "&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience="\
                           f"&PlayerPosition=&PlusMinus=N&Rank=N&Season={self.season_dates['season_years']}"\
                           "&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench="\
                           "&TeamID=0&VsConference=&VsDivision="]
   
        self.headers = {"Accept": "application/json, text/plain, */*",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Connection": "keep-alive",
                        "DNT": "1",
                        "If-Modified-Since": "Sat, 20 Aug 2022 16:34:56 GMT",
                        "Origin": "https://www.nba.com",
                        "Referer": "https://www.nba.com/",
                        "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "cors",
                        "Sec-Fetch-Site": "same-site",
                        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
                        "sec-ch-ua": "\"Chromium\";v=\"104\", \" Not A;Brand\";v=\"99\", \"Google Chrome\";v=\"104\"",
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": "\"Linux\"",
                        "x-nba-stats-origin": "stats",
                        "x-nba-stats-token": "true"}

    def start_requests(self):
        for url in self.start_urls:
            yield Request(
                url,
                self.parse,
                headers=self.headers
            )

    def parse(self, response):
        data = json.loads(response.text)
        print(data)

        # for row in response.xpath(
        #     "//div[@class='nba-stat-table']//tbody/tr[@aria-hidden='false']"
        # )[:30]:
        #     loader = NBA_ShootingStatsItemLoader(
        #         item=NBA_ShootingStatsItem(), selector=row
        #     )
        #     loader.add_value("date", date_to)
        #     loader.add_xpath("team", "td[1]/a/text()")
        #     loader.add_xpath("fgm_ra", "td[2]/text()")
        #     loader.add_xpath("fga_ra", "td[3]/text()")
        #     loader.add_xpath("fg_pct_ra", "td[4]/text()")
        #     loader.add_xpath("fgm_paint", "td[5]/text()")
        #     loader.add_xpath("fga_paint", "td[6]/text()")
        #     loader.add_xpath("fg_pct_paint", "td[7]/text()")
        #     loader.add_xpath("fgm_mr", "td[8]/text()")
        #     loader.add_xpath("fga_mr", "td[9]/text()")
        #     loader.add_xpath("fg_pct_mr", "td[10]/text()")
        #     loader.add_xpath("fgm_lc3", "td[11]/text()")
        #     loader.add_xpath("fga_lc3", "td[12]/text()")
        #     loader.add_xpath("fg_pct_lc3", "td[13]/text()")
        #     loader.add_xpath("fgm_rc3", "td[14]/text()")
        #     loader.add_xpath("fga_rc3", "td[15]/text()")
        #     loader.add_xpath("fg_pct_rc3", "td[16]/text()")
        #     loader.add_xpath("fgm_c3", "td[17]/text()")
        #     loader.add_xpath("fga_c3", "td[18]/text()")
        #     loader.add_xpath("fg_pct_c3", "td[19]/text()")
        #     loader.add_xpath("fgm_atb3", "td[20]/text()")
        #     loader.add_xpath("fga_atb3", "td[21]/text()")
        #     loader.add_xpath("fg_pct_atb3", "td[22]/text()")

        #     # add missing fields
        #     item = loader.load_item()
        #     fields = [
        #         f
        #         for f in [
        #             "date",
        #             "team",
        #             "fgm_ra",
        #             "fga_ra",
        #             "fg_pct_ra",
        #             "fgm_paint",
        #             "fga_paint",
        #             "fg_pct_paint",
        #             "fgm_mr",
        #             "fga_mr",
        #             "fg_pct_mr",
        #             "fgm_lc3",
        #             "fga_lc3",
        #             "fg_pct_lc3",
        #             "fgm_rc3",
        #             "fga_rc3",
        #             "fg_pct_rc3",
        #             "fgm_c3",
        #             "fga_c3",
        #             "fg_pct_c3",
        #             "fgm_atb3",
        #             "fga_atb3",
        #             "fg_pct_atb3",
        #         ]
        #         if f not in item
        #     ]

        #     for f in fields:
        #         item[f] = None

        #     print(item)
        #     yield item

        # # Uncomment below to work with more than one day at a time.

        # active_date = datetime.datetime.strptime(
        #     date_to.strip("Date To : "),
        #     "%m/%d/%Y",
        # )
        # previous_date = active_date - datetime.timedelta(days=1)

        # # stop_date = datetime.datetime.strptime("Month 00, 0000", "%B %d, %Y")
        # stop_date = self.season_dates["start_day"]

        # # scrape previous date if before stop date
        # if active_date > stop_date:
        #     month = previous_date.month
        #     day = previous_date.day
        #     year = previous_date.year

        #     url = f"https://www.nba.com/stats/teams/shooting/?sort=TEAM_NAME&dir=-1&Season={self.season_dates['season_years']}&SeasonType=Regular%20Season&DateTo={month}%2F{day}%2F{year}"

        #     yield Request(
        #         url,
        #         callback=self.parse,
        #         args={"wait": 5, "private_mode_enabled": False},
        #     )


if __name__ == "__main__":

    for year in [2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015]:
        season_info = return_season_dates(year)
        process = CrawlerProcess(get_project_settings())
        process.crawl("NBA_shooting_stats_json_spider", season_dates=season_info)
    process.start()
