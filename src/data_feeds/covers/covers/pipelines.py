# Used for connection to Postgres Database on AWS RDS.
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()
RDS_PASSWORD = os.environ.get("RDS_PASSWORD")
RDS_ENDPOINT = os.environ.get("RDS_ENDPOINT")


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
        # print(item)
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
            (
                item["id_num"],
                item["date"],
                item["time"],
                item["home_team"],
                item["away_team"],
                item["spread"],
                item["fanduel_line_away"],
                item["fanduel_line_price_away"],
                item["fanduel_line_home"],
                item["fanduel_line_price_home"],
                item["draftkings_line_away"],
                item["draftkings_line_price_away"],
                item["draftkings_line_home"],
                item["draftkings_line_price_home"],
                item["covers_away_consensus"],
                item["covers_home_consensus"],
                item["game_url"],
                item["league_year"],
            ),
        )
        self.connection.commit()
