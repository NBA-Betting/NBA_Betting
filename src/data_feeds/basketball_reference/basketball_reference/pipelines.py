# Used for connection to Postgres Database on AWS RDS.
import sys
import psycopg2

sys.path.append('../../../../')
from passkeys import RDS_ENDPOINT, RDS_PASSWORD


class SaveToPostgresPipeline(object):
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
        if spider.name == "BR_standings_spider":
            self.cur.execute(
                """INSERT INTO dfc_br_standings(
                    team,
                    date,
                    wins,
                    losses,
                    win_perc,
                    points_scored_per_game,
                    points_allowed_per_game,
                    expected_wins,
                    expected_losses)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["team"],
                    item["date"],
                    item["wins"],
                    item["losses"],
                    item["win_perc"],
                    item["points_scored_per_game"],
                    item["points_allowed_per_game"],
                    item["expected_wins"],
                    item["expected_losses"],
                ),
            )
            self.connection.commit()
        elif spider.name == "BR_team_stats_spider":
            self.cur.execute(
                """INSERT INTO dfc_br_team_stats(
                    team,
                    date,
                    g,
                    mp,
                    fg,
                    fga,
                    fg_pct,
                    fg3,
                    fg3a,
                    fg3_pct,
                    fg2,
                    fg2a,
                    fg2_pct,
                    ft,
                    fta,
                    ft_pct,
                    orb,
                    drb,
                    trb,
                    ast,
                    stl,
                    blk,
                    tov,
                    pf,
                    pts)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["team"],
                    item["date"],
                    item["g"],
                    item["mp"],
                    item["fg"],
                    item["fga"],
                    item["fg_pct"],
                    item["fg3"],
                    item["fg3a"],
                    item["fg3_pct"],
                    item["fg2"],
                    item["fg2a"],
                    item["fg2_pct"],
                    item["ft"],
                    item["fta"],
                    item["ft_pct"],
                    item["orb"],
                    item["drb"],
                    item["trb"],
                    item["ast"],
                    item["stl"],
                    item["blk"],
                    item["tov"],
                    item["pf"],
                    item["pts"],
                ),
            )
            self.connection.commit()
        elif spider.name == "BR_opponent_stats_spider":
            self.cur.execute(
                """INSERT INTO dfc_br_opponent_stats(
                    team,
                    date,
                    opp_g,
                    opp_mp,
                    opp_fg,
                    opp_fga,
                    opp_fg_pct,
                    opp_fg3,
                    opp_fg3a,
                    opp_fg3_pct,
                    opp_fg2,
                    opp_fg2a,
                    opp_fg2_pct,
                    opp_ft,
                    opp_fta,
                    opp_ft_pct,
                    opp_orb,
                    opp_drb,
                    opp_trb,
                    opp_ast,
                    opp_stl,
                    opp_blk,
                    opp_tov,
                    opp_pf,
                    opp_pts)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["team"],
                    item["date"],
                    item["opp_g"],
                    item["opp_mp"],
                    item["opp_fg"],
                    item["opp_fga"],
                    item["opp_fg_pct"],
                    item["opp_fg3"],
                    item["opp_fg3a"],
                    item["opp_fg3_pct"],
                    item["opp_fg2"],
                    item["opp_fg2a"],
                    item["opp_fg2_pct"],
                    item["opp_ft"],
                    item["opp_fta"],
                    item["opp_ft_pct"],
                    item["opp_orb"],
                    item["opp_drb"],
                    item["opp_trb"],
                    item["opp_ast"],
                    item["opp_stl"],
                    item["opp_blk"],
                    item["opp_tov"],
                    item["opp_pf"],
                    item["opp_pts"],
                ),
            )
            self.connection.commit()
        else:
            return item

    # Scripts to create original tables in psql command line.

    # Basketball Reference Standings
    """
    CREATE TABLE dfc_br_standings (
        team varchar NOT NULL,
        date varchar NOT NULL,
        wins int4,
        losses int4,
        win_perc float4,
        points_scored_per_game float4,
        points_allowed_per_game float4,
        expected_wins float4,
        expected_losses float4
    );
    """

    # Basketball Reference Team Stats
    """
    CREATE TABLE dfc_br_team_stats (
        team varchar NOT NULL,
        date varchar NOT NULL,
        g int4,
        mp int4,
        fg int4,
        fga int4,
        fg_pct float4,
        fg3 int4,
        fg3a int4,
        fg3_pct float4,
        fg2 int4,
        fg2a int4,
        fg2_pct float4,
        ft int4,
        fta int4,
        ft_pct float4,
        orb int4,
        drb int4,
        trb int4,
        ast int4,
        stl int4,
        blk int4,
        tov int4,
        pf int4,
        pts int4
    );
    """

    # Basketball Reference Opponent Stats
    """
    CREATE TABLE dfc_br_opponent_stats (
        team varchar NOT NULL,
        date varchar NOT NULL,
        opp_g int4,
        opp_mp int4,
        opp_fg int4,
        opp_fga int4,
        opp_fg_pct float4,
        opp_fg3 int4,
        opp_fg3a int4,
        opp_fg3_pct float4,
        opp_fg2 int4,
        opp_fg2a int4,
        opp_fg2_pct float4,
        opp_ft int4,
        opp_fta int4,
        opp_ft_pct float4,
        opp_orb int4,
        opp_drb int4,
        opp_trb int4,
        opp_ast int4,
        opp_stl int4,
        opp_blk int4,
        opp_tov int4,
        opp_pf int4,
        opp_pts int4
    );
    """
