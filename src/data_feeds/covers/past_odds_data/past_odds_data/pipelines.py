import psycopg2
from datetime import datetime
from scrapy.exceptions import DropItem


class PastOddsPipeline:

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
        hostname = ""
        username = "postgres"
        password = ""
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
        if item["home"]:
            if item["opponent"].upper() in self.team_abbr:
                item["opponent"] = self.team_abbr[item["opponent"].upper()]
            league_year = item["league_year"]
            start_year = league_year.split("-")[0]
            end_year = league_year.split("-")[1]
            date = item["date"]
            if date.split()[0] in ["Nov", "Dec"]:
                date = datetime.strptime(f"{date} {start_year}", "%b %d %Y")
                item["date"] = date.strftime("%Y-%m-%d")
            elif (date.split()[0] == "Oct") and (int(date.split()[1]) > 12):
                date = datetime.strptime(f"{date} {start_year}", "%b %d %Y")
                item["date"] = date.strftime("%Y-%m-%d")
            else:
                date = datetime.strptime(f"{date} {end_year}", "%b %d %Y")
                item["date"] = date.strftime("%Y-%m-%d")
            self.cur.execute(
                """INSERT INTO past_covers_odds(game_id,
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
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["game_id"],
                    item["date"],
                    item["league_year"],
                    item["game_url"],
                    item["team"],
                    item["score"],
                    item["opponent"],
                    item["opponent_score"],
                    item["result"],
                    item["spread"],
                    item["spread_result"],
                ),
            )
            self.connection.commit()

        else:
            raise DropItem()


# Script to create original table in psql command line.

"""CREATE TABLE past_covers_odds (
    game_id int4 PRIMARY KEY NOT NULL,
    date varchar NOT NULL,
    league_year varchar,
    game_url varchar,
    team varchar,
    score int4,
    opponent varchar,
    opponent_score int4,
    result varchar,
    spread float4,
    spread_result varchar
);"""
