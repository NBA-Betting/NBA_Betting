# Used for connection to Postgres Database on AWS RDS.

import psycopg2


class LiveOddsToPostgresPipeline(object):
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

    # Script to create original table in psql command line.

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
