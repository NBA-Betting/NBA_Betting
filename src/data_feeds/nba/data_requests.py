import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine

from nba.spiders.helpers import return_season_dates

pd.set_option('display.max_columns', 100)
pd.options.display.width = 0


def scrape_nba_stats(years, url_stat_type, inbound_params, header_row,
                     data_row_nums, database_table_name):
    """Scrape and save data from data feed
       that supplies NBA stats tables.

    Args:
        years (list of int): Years to scrape.
        url_stat_type (str): Stat type from url.
                             After 'leaguedash'
        inbound_params (dict): Params for get request.
                               Specific to stat type.
        header_row (list of str): Columns headers that match database table.
        data_row_nums (list of int): Data row indexes from Chrome Inpsect.
        database_table_name (str): In nba_betting database.
    """
    for year in years:
        season_dates = return_season_dates(year)
        # dates = [season_dates["final_date"]]
        dates = pd.date_range(start=season_dates["start_date"],
                              end=season_dates["final_date"],
                              freq='D')
        # dates = [pd.to_datetime("2018-11-3", format="%Y-%m-%d")]

        errors = []
        try:
            for date in dates:
                url = f'https://stats.nba.com/stats/league{url_stat_type}'

                headers = {
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                    'If-Modified-Since': 'Sat, 20 Aug 2022 18:27:38 GMT',
                    'Origin': 'https://www.nba.com',
                    'Referer': 'https://www.nba.com/',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-site',
                    'User-Agent':
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
                    'sec-ch-ua':
                    '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Linux"',
                    'x-nba-stats-origin': 'stats',
                    'x-nba-stats-token': 'true'
                }

                params = inbound_params
                params['DateTo'] = f'{date.strftime("%m/%d/%Y")}'
                params['Season'] = f'{season_dates["season_years"]}'

                response = requests.get(url, params=params, headers=headers)
                data = response.json()
                if database_table_name in [
                        'nba_shooting', 'nba_opponent_shooting'
                ]:
                    data_rows = [[date.strftime("%Y%m%d")] +
                                 [row[item] for item in data_row_nums]
                                 for row in data['resultSets']['rowSet']]
                else:
                    data_rows = [[date.strftime("%Y%m%d")] +
                                 [row[item] for item in data_row_nums]
                                 for row in data['resultSets'][0]['rowSet']]
                df = pd.DataFrame(data_rows, columns=header_row)
                df.to_sql(database_table_name,
                          connection,
                          if_exists='append',
                          index=False)
                print('-' * 25)
                print(f'{date} saved. Data shape: {df.shape} Example:')
                print(df.head(1))
                # print(df.info())
        except Exception as e:
            errors.append(f'{date} - {e}')
            print(errors)
            print(f'Error Count: {len(errors)}')
            continue


if __name__ == "__main__":
    years = [2018]
    nba_stats_header_rows = {
        'traditional': [
            "date", "team", "gp", "win", "loss", "w_pct", "mins", "pts", "fgm",
            "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct", "ftm", "fta", "ft_pct",
            "oreb", "dreb", "reb", "ast", "tov", "stl", "blk", "blka", "pf",
            "pfd", "p_m"
        ],
        'advanced': [
            "date", "team", "offrtg", "defrtg", "netrtg", "ast_pct",
            "ast_v_tov", "ast_ratio", "oreb_pct", "dreb_pct", "reb_pct",
            "tov_pct", "efg_pct", "ts_pct", "pace", "pie", "poss"
        ],
        'four_factors': [
            "date", "team", "fta_rate", "opp_efg_pct", "opp_fta_rate",
            "opp_tov_pct", "opp_oreb_pct"
        ],
        'misc': [
            "date", "team", "pts_off_tov", "second_pts", "fbps", "pitp",
            "opp_pts_off_tov", "opp_second_pts", "opp_fbps", "opp_pitp"
        ],
        'scoring': [
            "date", "team", "pct_fga_2pt", "pct_fga_3pt", "pct_pts_2pt",
            "pct_pts_2pt_mid", "pct_pts_3pt", "pct_pts_fbps", "pct_pts_ft",
            "pct_pts_off_tov", "pct_pts_pitp", "pct_ast_2fgm", "pct_uast_2fgm",
            "pct_ast_3fgm", "pct_uast_3fgm", "pct_ast_fgm", "pct_uast_fgm"
        ],
        'opponent': [
            "date", "team", "opp_fgm", "opp_fga", "opp_fg_pct", "opp_3pm",
            "opp_3pa", "opp_3pt_pct", "opp_ftm", "opp_fta", "opp_ft_pct",
            "opp_oreb", "opp_dreb", "opp_reb", "opp_ast", "opp_tov", "opp_stl",
            "opp_blk", "opp_blka", "opp_pf", "opp_pfd", "opp_pts", "opp_pm"
        ],
        'speed_distance': [
            "date", "team", "dist_feet", "dist_miles", "dist_miles_off",
            "dist_miles_def", "avg_speed", "avg_speed_off", "avg_speed_def"
        ],
        'shooting': [
            'date', 'team', 'fgm_ra', 'fga_ra', 'fg_pct_ra', 'fgm_paint',
            'fga_paint', 'fg_pct_paint', 'fgm_mr', 'fga_mr', 'fg_pct_mr',
            'fgm_lc3', 'fga_lc3', 'fg_pct_lc3', 'fgm_rc3', 'fga_rc3',
            'fg_pct_rc3', 'fgm_c3', 'fga_c3', 'fg_pct_c3', 'fgm_atb3',
            'fga_atb3', 'fg_pct_atb3'
        ],
        'opponent_shooting': [
            "date", "team", "opp_fgm_ra", "opp_fga_ra", "opp_fg_pct_ra",
            "opp_fgm_paint", "opp_fga_paint", "opp_fg_pct_paint", "opp_fgm_mr",
            "opp_fga_mr", "opp_fg_pct_mr", "opp_fgm_lc3", "opp_fga_lc3",
            "opp_fg_pct_lc3", "opp_fgm_rc3", "opp_fga_rc3", "opp_fg_pct_rc3",
            "opp_fgm_c3", "opp_fga_c3", "opp_fg_pct_c3", "opp_fgm_atb3",
            "opp_fga_atb3", "opp_fg_pct_atb3"
        ],
        'hustle': [
            "date", "team", "screen_ast", "screen_ast_pts", "deflections",
            "off_loose_ball_rec", "def_loose_ball_rec", "loose_ball_rec",
            "pct_loose_ball_rec_off", "pct_loose_ball_rec_def",
            "charges_drawn", "contested_2pt", "contested_3pt",
            "contested_shots"
        ]
    }
    nba_stats_url_postfix = {
        'traditional': 'dashteamstats',
        'advanced': 'dashteamstats',
        'four_factors': 'dashteamstats',
        'misc': 'dashteamstats',
        'scoring': 'dashteamstats',
        'opponent': 'dashteamstats',
        'speed_distance': 'dashptstats',
        'shooting': 'dashteamshotlocations',
        'opponent_shooting': 'dashteamshotlocations',
        'hustle': 'hustlestatsteam'
    }
    nba_stats_params = {
        'traditional': {
            'Conference': '',
            'DateFrom': '',
            'DateTo': '',
            'Division': '',
            'GameScope': '',
            'GameSegment': '',
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': '',
            'MeasureType': 'Base',
            'Month': '0',
            'OpponentTeamID': '0',
            'Outcome': '',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlayerExperience': '',
            'PlayerPosition': '',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'StarterBench': '',
            'TeamID': '0',
            'TwoWay': '0',
            'VsConference': '',
            'VsDivision': '',
        },
        'advanced': {
            'Conference': '',
            'DateFrom': '',
            'DateTo': '',
            'Division': '',
            'GameScope': '',
            'GameSegment': '',
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': '',
            'MeasureType': 'Advanced',
            'Month': '0',
            'OpponentTeamID': '0',
            'Outcome': '',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlayerExperience': '',
            'PlayerPosition': '',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'StarterBench': '',
            'TeamID': '0',
            'TwoWay': '0',
            'VsConference': '',
            'VsDivision': '',
        },
        'four_factors': {
            'Conference': '',
            'DateFrom': '',
            'DateTo': '',
            'Division': '',
            'GameScope': '',
            'GameSegment': '',
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': '',
            'MeasureType': 'Four Factors',
            'Month': '0',
            'OpponentTeamID': '0',
            'Outcome': '',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlayerExperience': '',
            'PlayerPosition': '',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'StarterBench': '',
            'TeamID': '0',
            'TwoWay': '0',
            'VsConference': '',
            'VsDivision': '',
        },
        'misc': {
            'Conference': '',
            'DateFrom': '',
            'DateTo': '',
            'Division': '',
            'GameScope': '',
            'GameSegment': '',
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': '',
            'MeasureType': 'Misc',
            'Month': '0',
            'OpponentTeamID': '0',
            'Outcome': '',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlayerExperience': '',
            'PlayerPosition': '',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'StarterBench': '',
            'TeamID': '0',
            'TwoWay': '0',
            'VsConference': '',
            'VsDivision': '',
        },
        'scoring': {
            'Conference': '',
            'DateFrom': '',
            'DateTo': '',
            'Division': '',
            'GameScope': '',
            'GameSegment': '',
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': '',
            'MeasureType': 'Scoring',
            'Month': '0',
            'OpponentTeamID': '0',
            'Outcome': '',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlayerExperience': '',
            'PlayerPosition': '',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'StarterBench': '',
            'TeamID': '0',
            'TwoWay': '0',
            'VsConference': '',
            'VsDivision': '',
        },
        'opponent': {
            'Conference': '',
            'DateFrom': '',
            'DateTo': '',
            'Division': '',
            'GameScope': '',
            'GameSegment': '',
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': '',
            'MeasureType': 'Opponent',
            'Month': '0',
            'OpponentTeamID': '0',
            'Outcome': '',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlayerExperience': '',
            'PlayerPosition': '',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'StarterBench': '',
            'TeamID': '0',
            'TwoWay': '0',
            'VsConference': '',
            'VsDivision': '',
        },
        'speed_distance': {
            'College': '',
            'Conference': '',
            'Country': '',
            'DateFrom': '',
            'DateTo': '',
            'Division': '',
            'DraftPick': '',
            'DraftYear': '',
            'GameScope': '',
            'GameSegment': '',
            'Height': '',
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': '',
            'Month': '0',
            'OpponentTeamID': '0',
            'Outcome': '',
            'PORound': '0',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlayerExperience': '',
            'PlayerOrTeam': 'Team',
            'PlayerPosition': '',
            'PtMeasureType': 'SpeedDistance',
            'Season': '',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'StarterBench': '',
            'TeamID': '0',
            'VsConference': '',
            'VsDivision': '',
            'Weight': '',
        },
        'shooting': {
            'Conference': '',
            'DateFrom': '',
            'DateTo': '',
            'DistanceRange': 'By Zone',
            'Division': '',
            'GameScope': '',
            'GameSegment': '',
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': '',
            'MeasureType': 'Base',
            'Month': '0',
            'OpponentTeamID': '0',
            'Outcome': '',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlayerExperience': '',
            'PlayerPosition': '',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'StarterBench': '',
            'TeamID': '0',
            'TwoWay': '0',
            'VsConference': '',
            'VsDivision': '',
        },
        'opponent_shooting': {
            'Conference': '',
            'DateFrom': '',
            'DateTo': '',
            'DistanceRange': 'By Zone',
            'Division': '',
            'GameScope': '',
            'GameSegment': '',
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': '',
            'MeasureType': 'Opponent',
            'Month': '0',
            'OpponentTeamID': '0',
            'Outcome': '',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlayerExperience': '',
            'PlayerPosition': '',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'StarterBench': '',
            'TeamID': '0',
            'VsConference': '',
            'VsDivision': '',
        },
        'hustle': {
            'College': '',
            'Conference': '',
            'Country': '',
            'DateFrom': '',
            'DateTo': '',
            'Division': '',
            'DraftPick': '',
            'DraftYear': '',
            'GameScope': '',
            'GameSegment': '',
            'Height': '',
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': '',
            'Month': '0',
            'OpponentTeamID': '0',
            'Outcome': '',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlayerExperience': '',
            'PlayerPosition': '',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'ShotClockRange': '',
            'TeamID': '0',
            'VsConference': '',
            'VsDivision': '',
            'Weight': '',
        }
    }

    nba_stats_data_rows = {
        'traditional': [
            1, 2, 3, 4, 5, 6, 26, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 21, 22, 23, 24, 25, 27
        ],
        'advanced':
        [1, 8, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23, 26, 25],
        'four_factors': [1, 8, 11, 12, 13, 14],
        'misc': [1, 7, 8, 9, 10, 11, 12, 13, 14],
        'scoring':
        [1, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
        'opponent': [
            1, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27
        ],
        'speed_distance': [2, 8, 9, 10, 11, 12, 13, 14],
        'shooting': [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 23, 24, 25,
            17, 18, 19
        ],
        'opponent_shooting': [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 23, 24, 25,
            17, 18, 19
        ],
        'hustle': [1, 8, 9, 6, 10, 11, 12, 13, 14, 7, 4, 5, 3]
    }

    stat_datatype = 'scoring'

    username = 'postgres'
    password = ''
    endpoint = ''
    database = 'nba_betting'
    engine = create_engine(
        f'postgresql+psycopg2://{username}:{password}@{endpoint}/{database}')

    with engine.connect() as connection:
        scrape_nba_stats(years, nba_stats_url_postfix[stat_datatype],
                         nba_stats_params[stat_datatype],
                         nba_stats_header_rows[stat_datatype],
                         nba_stats_data_rows[stat_datatype],
                         f'nba_{stat_datatype}')
