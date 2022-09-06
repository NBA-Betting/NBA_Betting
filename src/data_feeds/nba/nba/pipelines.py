# Used for connection to Postgres Database on AWS RDS.

import psycopg2


class SaveToPostgresPipeline(object):
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
        if spider.name == "NBA_traditional_stats_spider":
            self.cur.execute(
                """INSERT INTO nba_traditional(
                    date,
                    team,
                    gp,
                    win,
                    loss,
                    w_pct,
                    mins,
                    pts,
                    fgm,
                    fga,
                    fg_pct,
                    fg3m,
                    fg3a,
                    fg3_pct,
                    ftm,
                    fta,
                    ft_pct,
                    oreb,
                    dreb,
                    reb,
                    ast,
                    tov,
                    stl,
                    blk,
                    blka,
                    pf,
                    pfd,
                    p_m)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["team"],
                    item["gp"],
                    item["win"],
                    item["loss"],
                    item["w_pct"],
                    item["mins"],
                    item["pts"],
                    item["fgm"],
                    item["fga"],
                    item["fg_pct"],
                    item["fg3m"],
                    item["fg3a"],
                    item["fg3_pct"],
                    item["ftm"],
                    item["fta"],
                    item["ft_pct"],
                    item["oreb"],
                    item["dreb"],
                    item["reb"],
                    item["ast"],
                    item["tov"],
                    item["stl"],
                    item["blk"],
                    item["blka"],
                    item["pf"],
                    item["pfd"],
                    item["p_m"],
                ),
            )
            self.connection.commit()
        elif spider.name == "NBA_advanced_stats_spider":
            self.cur.execute(
                """INSERT INTO nba_advanced(
                    date,
                    team,
                    offrtg,
                    defrtg,
                    netrtg,
                    ast_pct,
                    ast_v_tov,
                    ast_ratio,
                    oreb_pct,
                    dreb_pct,
                    reb_pct,
                    tov_pct,
                    efg_pct,
                    ts_pct,
                    pace,
                    pie,
                    poss)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["team"],
                    item["offrtg"],
                    item["defrtg"],
                    item["netrtg"],
                    item["ast_pct"],
                    item["ast_v_tov"],
                    item["ast_ratio"],
                    item["oreb_pct"],
                    item["dreb_pct"],
                    item["reb_pct"],
                    item["tov_pct"],
                    item["efg_pct"],
                    item["ts_pct"],
                    item["pace"],
                    item["pie"],
                    item["poss"],
                ),
            )
            self.connection.commit()
        elif spider.name == "NBA_four_factors_stats_spider":
            self.cur.execute(
                """INSERT INTO nba_four_factors(
                    date,
                    team,
                    fta_rate,
                    opp_efg_pct,
                    opp_fta_rate,
                    opp_tov_pct,
                    opp_oreb_pct)
                    VALUES(%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["team"],
                    item["fta_rate"],
                    item["opp_efg_pct"],
                    item["opp_fta_rate"],
                    item["opp_tov_pct"],
                    item["opp_oreb_pct"],
                ),
            )
            self.connection.commit()
        elif spider.name == "NBA_misc_stats_spider":
            self.cur.execute(
                """INSERT INTO nba_misc(
                    date,
                    team,
                    pts_off_tov,
                    second_pts,
                    fbps,
                    pitp,
                    opp_pts_off_tov,
                    opp_second_pts,
                    opp_fbps,
                    opp_pitp)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["team"],
                    item["pts_off_tov"],
                    item["second_pts"],
                    item["fbps"],
                    item["pitp"],
                    item["opp_pts_off_tov"],
                    item["opp_second_pts"],
                    item["opp_fbps"],
                    item["opp_pitp"],
                ),
            )
            self.connection.commit()
        elif spider.name == "NBA_scoring_stats_spider":
            self.cur.execute(
                """INSERT INTO nba_scoring(
                    date,
                    team,
                    pct_fga_2pt,
                    pct_fga_3pt,
                    pct_pts_2pt,
                    pct_pts_2pt_mid,
                    pct_pts_3pt,
                    pct_pts_fbps,
                    pct_pts_ft,
                    pct_pts_off_tov,
                    pct_pts_pitp,
                    pct_ast_2fgm,
                    pct_uast_2fgm,
                    pct_ast_3fgm,
                    pct_uast_3fgm,
                    pct_ast_fgm,
                    pct_uast_fgm)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["team"],
                    item["pct_fga_2pt"],
                    item["pct_fga_3pt"],
                    item["pct_pts_2pt"],
                    item["pct_pts_2pt_mid"],
                    item["pct_pts_3pt"],
                    item["pct_pts_fbps"],
                    item["pct_pts_ft"],
                    item["pct_pts_off_tov"],
                    item["pct_pts_pitp"],
                    item["pct_ast_2fgm"],
                    item["pct_uast_2fgm"],
                    item["pct_ast_3fgm"],
                    item["pct_uast_3fgm"],
                    item["pct_ast_fgm"],
                    item["pct_uast_fgm"],
                ),
            )
            self.connection.commit()
        elif spider.name == "NBA_opponent_stats_spider":
            self.cur.execute(
                """INSERT INTO nba_opponent(
                    date,
                    team,
                    opp_fgm,
                    opp_fga,
                    opp_fg_pct,
                    opp_3pm,
                    opp_3pa,
                    opp_3pt_pct,
                    opp_ftm,
                    opp_fta,
                    opp_ft_pct,
                    opp_oreb,
                    opp_dreb,
                    opp_reb,
                    opp_ast,
                    opp_tov,
                    opp_stl,
                    opp_blk,
                    opp_blka,
                    opp_pf,
                    opp_pfd,
                    opp_pts,
                    opp_pm)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["team"],
                    item["opp_fgm"],
                    item["opp_fga"],
                    item["opp_fg_pct"],
                    item["opp_3pm"],
                    item["opp_3pa"],
                    item["opp_3pt_pct"],
                    item["opp_ftm"],
                    item["opp_fta"],
                    item["opp_ft_pct"],
                    item["opp_oreb"],
                    item["opp_dreb"],
                    item["opp_reb"],
                    item["opp_ast"],
                    item["opp_tov"],
                    item["opp_stl"],
                    item["opp_blk"],
                    item["opp_blka"],
                    item["opp_pf"],
                    item["opp_pfd"],
                    item["opp_pts"],
                    item["opp_pm"],
                ),
            )
            self.connection.commit()
        elif spider.name == "NBA_speed_distance_stats_spider":
            self.cur.execute(
                """INSERT INTO nba_speed_distance(
                    date,
                    team,
                    dist_feet,
                    dist_miles,
                    dist_miles_off,
                    dist_miles_def,
                    avg_speed,
                    avg_speed_off,
                    avg_speed_def)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["team"],
                    item["dist_feet"],
                    item["dist_miles"],
                    item["dist_miles_off"],
                    item["dist_miles_def"],
                    item["avg_speed"],
                    item["avg_speed_off"],
                    item["avg_speed_def"],
                ),
            )
            self.connection.commit()
        elif spider.name == "NBA_shooting_stats_spider":
            self.cur.execute(
                """INSERT INTO nba_shooting(
                    date,
                    team,
                    fgm_ra,
                    fga_ra,
                    fg_pct_ra,
                    fgm_paint,
                    fga_paint,
                    fg_pct_paint,
                    fgm_mr,
                    fga_mr,
                    fg_pct_mr,
                    fgm_lc3,
                    fga_lc3,
                    fg_pct_lc3,
                    fgm_rc3,
                    fga_rc3,
                    fg_pct_rc3,
                    fgm_c3,
                    fga_c3,
                    fg_pct_c3,
                    fgm_atb3,
                    fga_atb3,
                    fg_pct_atb3)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["team"],
                    item["fgm_ra"],
                    item["fga_ra"],
                    item["fg_pct_ra"],
                    item["fgm_paint"],
                    item["fga_paint"],
                    item["fg_pct_paint"],
                    item["fgm_mr"],
                    item["fga_mr"],
                    item["fg_pct_mr"],
                    item["fgm_lc3"],
                    item["fga_lc3"],
                    item["fg_pct_lc3"],
                    item["fgm_rc3"],
                    item["fga_rc3"],
                    item["fg_pct_rc3"],
                    item["fgm_c3"],
                    item["fga_c3"],
                    item["fg_pct_c3"],
                    item["fgm_atb3"],
                    item["fga_atb3"],
                    item["fg_pct_atb3"],
                ),
            )
            self.connection.commit()
        elif spider.name == "NBA_opponent_shooting_stats_spider":
            self.cur.execute(
                """INSERT INTO nba_opponent_shooting(
                    date,
                    team,
                    opp_fgm_ra,
                    opp_fga_ra,
                    opp_fg_pct_ra,
                    opp_fgm_paint,
                    opp_fga_paint,
                    opp_fg_pct_paint,
                    opp_fgm_mr,
                    opp_fga_mr,
                    opp_fg_pct_mr,
                    opp_fgm_lc3,
                    opp_fga_lc3,
                    opp_fg_pct_lc3,
                    opp_fgm_rc3,
                    opp_fga_rc3,
                    opp_fg_pct_rc3,
                    opp_fgm_c3,
                    opp_fga_c3,
                    opp_fg_pct_c3,
                    opp_fgm_atb3,
                    opp_fga_atb3,
                    opp_fg_pct_atb3)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["team"],
                    item["opp_fgm_ra"],
                    item["opp_fga_ra"],
                    item["opp_fg_pct_ra"],
                    item["opp_fgm_paint"],
                    item["opp_fga_paint"],
                    item["opp_fg_pct_paint"],
                    item["opp_fgm_mr"],
                    item["opp_fga_mr"],
                    item["opp_fg_pct_mr"],
                    item["opp_fgm_lc3"],
                    item["opp_fga_lc3"],
                    item["opp_fg_pct_lc3"],
                    item["opp_fgm_rc3"],
                    item["opp_fga_rc3"],
                    item["opp_fg_pct_rc3"],
                    item["opp_fgm_c3"],
                    item["opp_fga_c3"],
                    item["opp_fg_pct_c3"],
                    item["opp_fgm_atb3"],
                    item["opp_fga_atb3"],
                    item["opp_fg_pct_atb3"],
                ),
            )
            self.connection.commit()
        elif spider.name == "NBA_hustle_stats_spider":
            self.cur.execute(
                """INSERT INTO nba_hustle(
                    date,
                    team,
                    screen_ast,
                    screen_ast_pts,
                    deflections,
                    off_loose_ball_rec,
                    def_loose_ball_rec,
                    loose_ball_rec,
                    pct_loose_ball_rec_off,
                    pct_loose_ball_rec_def,
                    charges_drawn,
                    contested_2pt,
                    contested_3pt,
                    contested_shots)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    item["date"],
                    item["team"],
                    item["screen_ast"],
                    item["screen_ast_pts"],
                    item["deflections"],
                    item["off_loose_ball_rec"],
                    item["def_loose_ball_rec"],
                    item["loose_ball_rec"],
                    item["pct_loose_ball_rec_off"],
                    item["pct_loose_ball_rec_def"],
                    item["charges_drawn"],
                    item["contested_2pt"],
                    item["contested_3pt"],
                    item["contested_shots"],
                ),
            )
            self.connection.commit()
        else:
            return item

    # Scripts to create original tables in psql command line.

    # NBA Traditional Stats
    """
    CREATE TABLE nba_traditional(
        date varchar NOT NULL,
        team varchar NOT NULL,
        gp int4,
        win int4,
        loss int4,
        w_pct float4,
        mins float4,
        pts float4,
        fgm float4,
        fga float4,
        fg_pct float4,
        fg3m float4,
        fg3a float4,
        fg3_pct float4,
        ftm float4,
        fta float4,
        ft_pct float4,
        oreb float4,
        dreb float4,
        reb float4,
        ast float4,
        tov float4,
        stl float4,
        blk float4,
        blka float4,
        pf float4,
        pfd float4,
        p_m float4
    );
    """

    # NBA Advanced Stats
    """
    CREATE TABLE nba_advanced(
        date varchar NOT NULL,
        team varchar NOT NULL,
        offrtg float4,
        defrtg float4,
        netrtg float4,
        ast_pct float4,
        ast_v_tov float4,
        ast_ratio float4,
        oreb_pct float4,
        dreb_pct float4,
        reb_pct float4,
        tov_pct float4,
        efg_pct float4,
        ts_pct float4,
        pace float4,
        pie float4,
        poss int4
    );
    """

    # NBA Four Factors Stats
    """
    CREATE TABLE nba_four_factors(
        date varchar NOT NULL,
        team varchar NOT NULL,
        fta_rate float4,
        opp_efg_pct float4,
        opp_fta_rate float4,
        opp_tov_pct float4,
        opp_oreb_pct float4
    );
    """

    # NBA Misc Stats
    """
    CREATE TABLE nba_misc(
        date varchar NOT NULL,
        team varchar NOT NULL,
        pts_off_tov float4,
        second_pts float4,
        fbps float4,
        pitp float4,
        opp_pts_off_tov float4,
        opp_second_pts float4,
        opp_fbps float4,
        opp_pitp float4
    );
    """

    # NBA Scoring Stats
    """
    CREATE TABLE nba_scoring(
        date varchar NOT NULL,
        team varchar NOT NULL,
        pct_fga_2pt float4,
        pct_fga_3pt float4,
        pct_pts_2pt float4,
        pct_pts_2pt_mid float4,
        pct_pts_3pt float4,
        pct_pts_fbps float4,
        pct_pts_ft float4,
        pct_pts_off_tov float4,
        pct_pts_pitp float4,
        pct_ast_2fgm float4,
        pct_uast_2fgm float4,
        pct_ast_3fgm float4,
        pct_uast_3fgm float4,
        pct_ast_fgm float4,
        pct_uast_fgm float4
    );
    """

    # NBA Opponent Stats
    """
    CREATE TABLE nba_opponent(
        date varchar NOT NULL,
        team varchar NOT NULL,
        opp_fgm float4,
        opp_fga float4,
        opp_fg_pct float4,
        opp_3pm float4,
        opp_3pa float4,
        opp_3pt_pct float4,
        opp_ftm float4,
        opp_fta float4,
        opp_ft_pct float4,
        opp_oreb float4,
        opp_dreb float4,
        opp_reb float4,
        opp_ast float4,
        opp_tov float4,
        opp_stl float4,
        opp_blk float4,
        opp_blka float4,
        opp_pf float4,
        opp_pfd float4,
        opp_pts float4,
        opp_pm float4
    );
    """

    # NBA Speed and Distance Stats
    """
    CREATE TABLE nba_speed_distance(
        date varchar NOT NULL,
        team varchar NOT NULL,
        dist_feet float4,
        dist_miles float4,
        dist_miles_off float4,
        dist_miles_def float4,
        avg_speed float4,
        avg_speed_off float4,
        avg_speed_def float4
    );
    """

    # NBA Shooting Stats
    """
    CREATE TABLE nba_shooting(
        date varchar NOT NULL,
        team varchar NOT NULL,
        fgm_ra float4,
        fga_ra float4,
        fg_pct_ra float4,
        fgm_paint float4,
        fga_paint float4,
        fg_pct_paint float4,
        fgm_mr float4,
        fga_mr float4,
        fg_pct_mr float4,
        fgm_lc3 float4,
        fga_lc3 float4,
        fg_pct_lc3 float4,
        fgm_rc3 float4,
        fga_rc3 float4,
        fg_pct_rc3 float4,
        fgm_c3 float4,
        fga_c3 float4,
        fg_pct_c3 float4,
        fgm_atb3 float4,
        fga_atb3 float4,
        fg_pct_atb3 float4
    );
    """

    # NBA Opponent Shooting Stats
    """
    CREATE TABLE nba_opponent_shooting(
        date varchar NOT NULL,
        team varchar NOT NULL,
        opp_fgm_ra float4,
        opp_fga_ra float4,
        opp_fg_pct_ra float4,
        opp_fgm_paint float4,
        opp_fga_paint float4,
        opp_fg_pct_paint float4,
        opp_fgm_mr float4,
        opp_fga_mr float4,
        opp_fg_pct_mr float4,
        opp_fgm_lc3 float4,
        opp_fga_lc3 float4,
        opp_fg_pct_lc3 float4,
        opp_fgm_rc3 float4,
        opp_fga_rc3 float4,
        opp_fg_pct_rc3 float4,
        opp_fgm_c3 float4,
        opp_fga_c3 float4,
        opp_fg_pct_c3 float4,
        opp_fgm_atb3 float4,
        opp_fga_atb3 float4,
        opp_fg_pct_atb3 float4
    );
    """

    # NBA Hustle Stats
    """
    CREATE TABLE nba_hustle(
        date varchar NOT NULL,
        team varchar NOT NULL,
        screen_ast float4,
        screen_ast_pts float4,
        deflections float4,
        off_loose_ball_rec float4,
        def_loose_ball_rec float4,
        loose_ball_rec float4,
        pct_loose_ball_rec_off float4,
        pct_loose_ball_rec_def float4,
        charges_drawn float4,
        contested_2pt float4,
        contested_3pt float4,
        contested_shots float4
    );
    """
