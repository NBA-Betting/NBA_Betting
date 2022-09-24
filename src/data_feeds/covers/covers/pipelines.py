# Used for connection to Postgres Database on AWS RDS.
import sys
import psycopg2
from datetime import datetime
from scrapy.exceptions import DropItem

sys.path.append('../../../../')
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
        if spider.name == "Covers_live_game_results_spider":
            self.cur.execute(
                """INSERT INTO dfc_covers_game_results(
                        date,
                        home_team,
                        away_team,
                        home_score,
                        away_score)
                    VALUES(%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["home_team"],
                    item["away_team"],
                    item["home_score"],
                    item["away_score"],
                ),
            )
            self.connection.commit()
        elif spider.name == "Covers_live_game_spider":
            self.cur.execute(
                """INSERT INTO dfc_covers_odds(id_num,
                        date,
                        time,
                        home_team_full_name,
                        home_team_short_name,
                        away_team_full_name,
                        away_team_short_name,
                        open_line_away,
                        open_line_home,
                        fanduel_line_away,
                        fanduel_line_price_away,
                        fanduel_line_home,
                        fanduel_line_price_home,
                        draftkings_line_away,
                        draftkings_line_price_away,
                        draftkings_line_home,
                        draftkings_line_price_home,
                        covers_away_consenses,
                        covers_home_consenses,
                        link,
                        league_year)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["id_num"],
                    item["date"],
                    item["time"],
                    item["home_team_full_name"],
                    item["home_team_short_name"],
                    item["away_team_full_name"],
                    item["away_team_short_name"],
                    item["open_line_away"],
                    item["open_line_home"],
                    item["fanduel_line_away"],
                    item["fanduel_line_price_away"],
                    item["fanduel_line_home"],
                    item["fanduel_line_price_home"],
                    item["draftkings_line_away"],
                    item["draftkings_line_price_away"],
                    item["draftkings_line_home"],
                    item["draftkings_line_price_home"],
                    item["covers_away_consenses"],
                    item["covers_home_consenses"],
                    item["link"],
                    item["league_year"],
                ),
            )
            self.connection.commit()
        elif spider.name == "Covers_past_game_spider":
            if item["home"]:
                if item["opponent"].upper() in self.team_abbr:
                    item["opponent"] = self.team_abbr[item["opponent"].upper()]
                league_year = item["league_year"]
                start_year = league_year.split("-")[0]
                end_year = league_year.split("-")[1]
                date = item["date"]
                if date.split()[0] in ["Nov", "Dec"]:
                    date = datetime.strptime(f"{date} {start_year}",
                                             "%b %d %Y")
                    item["date"] = date.strftime("%Y-%m-%d")
                elif (date.split()[0]
                      == "Oct") and (int(date.split()[1]) > 12):
                    date = datetime.strptime(f"{date} {start_year}",
                                             "%b %d %Y")
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


# Scripts to create original tables in psql command line.
"""CREATE TABLE dfc_covers_game_results (
    date varchar NOT NULL,
    home_team varchar NOT NULL,
    away_team varchar NOT NULL,
    home_score int4 NOT NULL,
    away_score int4 NOT NULL
);"""
"""CREATE TABLE dfc_covers_odds (
    id_num int4 PRIMARY KEY NOT NULL,
    date varchar NOT NULL,
    time varchar NOT NULL,
    home_team_full_name varchar NOT NULL,
    home_team_short_name varchar NOT NULL,
    away_team_full_name varchar NOT NULL,
    away_team_short_name varchar NOT NULL,
    open_line_away float4,
    open_line_home float4,
    fanduel_line_away float4,
    fanduel_line_price_away int4,
    fanduel_line_home float4,
    fanduel_line_price_home int4,
    draftkings_line_away float4,
    draftkings_line_price_away int4,
    draftkings_line_home float4,
    draftkings_line_price_home int4,
    covers_away_consenses float4,
    covers_home_consenses float4,
    link varchar,
    league_year varchar
);"""
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
