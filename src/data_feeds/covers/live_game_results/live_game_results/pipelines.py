# Used for connection to Postgres Database on AWS RDS.

import psycopg2


class GameResultsToPostgresPipeline(object):
    def open_spider(self, spider):
        # hostname = ""
        username = "postgres"
        # password = ""
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

    # Script to create original table in psql command line.

    """CREATE TABLE dfc_covers_game_results (
        date varchar NOT NULL,
        home_team varchar NOT NULL,
        away_team varchar NOT NULL,
        home_score int4 NOT NULL,
        away_score int4 NOT NULL
    );"""
