# Used for connection to Postgres Database on AWS RDS.
import sys
import psycopg2
from datetime import datetime
from scrapy.exceptions import DropItem

sys.path.append('/home/jeff/Documents/Data_Science_Projects/NBA_Betting')
from passkeys import RDS_ENDPOINT, RDS_PASSWORD


class CoversPostgresPipeline(object):
    team_abbr = {
        "BK": "BKN",
        "CHAR": "CHA",
        "GS": "GSW",
        "NETS": "BKN",
        "NJ": "BKN",
        "NO": "NOP",
        "NY": "NYK",
        "PHO": "PHX",
        "SA": "SAS",
    }

    def open_spider(self, spider):
        hostname = RDS_ENDPOINT
        username = "postgres"
        password = RDS_PASSWORD
        database = "nba_betting"
        port = "5432"
        self.connection = psycopg2.connect(
            host=hostname,
            user=username,
            dbname=database,
            password=password,
            port=port,
        )
        self.cur = self.connection.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        if spider.name == "covers_live_game_spider":
            print(item)
            self.cur.execute(
                """INSERT INTO covers(id_num,
                        date,
                        time,
                        team,
                        opponent,
                        spread,
                        fanduel_line_away,
                        fanduel_line_price_away,
                        fanduel_line_home,
                        fanduel_line_price_home,
                        draftkings_line_away,
                        draftkings_line_price_away,
                        draftkings_line_home,
                        draftkings_line_price_home,
                        covers_away_consensus,
                        covers_home_consensus,
                        game_url,
                        league_year)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id_num)
                    DO NOTHING
                    ;""",
                (item["id_num"], item["date"], item["time"], item["home_team"],
                 item["away_team"], item["spread"], item["fanduel_line_away"],
                 item["fanduel_line_price_away"], item["fanduel_line_home"],
                 item["fanduel_line_price_home"], item["draftkings_line_away"],
                 item["draftkings_line_price_away"],
                 item["draftkings_line_home"],
                 item["draftkings_line_price_home"],
                 item["covers_away_consensus"], item["covers_home_consensus"],
                 item["game_url"], item["league_year"]))
            self.connection.commit()
        elif spider.name == "covers_past_game_spider":
            if item["is_home"]:
                if item["opponent"].upper() in self.team_abbr:
                    item["opponent"] = self.team_abbr[item["opponent"].upper()]
                league_year = item["league_year"]
                start_year = league_year.split("-")[0]
                end_year = league_year.split("-")[1]
                date = item["date"]
                if date.split()[0] in ["Nov", "Dec"]:
                    date = datetime.strptime(f"{date} {start_year}",
                                             "%b %d %Y")
                    item["date"] = date.strftime("%Y%m%d")
                elif (date.split()[0]
                      == "Oct") and (int(date.split()[1]) > 12):
                    date = datetime.strptime(f"{date} {start_year}",
                                             "%b %d %Y")
                    item["date"] = date.strftime("%Y%m%d")
                else:
                    date = datetime.strptime(f"{date} {end_year}", "%b %d %Y")
                    item["date"] = date.strftime("%Y%m%d")
                print(item)
                self.cur.execute(
                    """INSERT INTO covers(id_num,
                        date,
                        league_year,
                        game_url,
                        team,
                        score,
                        opponent,
                        opponent_score,
                        result,
                        spread,
                        spread_result)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id_num)
                    DO UPDATE
                        SET date = (%s),
                            league_year = (%s),
                            game_url = (%s),
                            team = (%s),
                            score = (%s),
                            opponent = (%s),
                            opponent_score = (%s),
                            result = (%s),
                            spread = (%s),
                            spread_result = (%s)
                    ;""",
                    (item["id_num"], item["date"], item["league_year"],
                     item["game_url"], item["team"], item["score"],
                     item["opponent"], item["opponent_score"], item["result"],
                     item["spread"], item["spread_result"], item["date"],
                     item["league_year"], item["game_url"], item["team"],
                     item["score"], item["opponent"], item["opponent_score"],
                     item["result"], item["spread"], item["spread_result"]))
                self.connection.commit()

            else:
                raise DropItem()
