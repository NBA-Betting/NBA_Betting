import datetime
import json
import ssl
import sys
import urllib.parse as urlparse
import urllib.request
from urllib.parse import urlencode

import pandas as pd
import pytz
import requests
from NBAStats_data_request_config import (
    DATA_ROWS,
    HEADER_ROWS,
    HTTP_REQUEST_HEADERS,
    PARAMS,
    URL_POSTFIX,
    return_season_dates,
)
from sqlalchemy import create_engine

sys.path.append("../../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD, WEB_UNLOCKER_PROXY_HANDLER

pd.set_option("display.max_columns", 100)
pd.options.display.width = 0


def nba_stats_data_request_local(
    years,
    http_headers,
    url_postfix,
    inbound_params,
    header_row,
    data_row_nums,
    database_table_name,
):
    """Scrape and save data from data feed
       that supplies NBA stats tables using local environment.

    Args:
        years (list of int): Years to scrape.
        url_postfix (str): Stat type from url.
                             After 'leaguedash'
        http_headers (dict): Headers for get request.
        inbound_params (dict): Params for get request.
                               Specific to stat type.
        header_row (list of str): Columns headers that match database table.
        data_row_nums (list of int): Data row indexes from Chrome Inspect.
        database_table_name (str): In nba_betting database.
    """
    for year in years:
        season_dates = return_season_dates(year)
        current_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
        yesterday_datetime = current_datetime - datetime.timedelta(days=1)
        dates = [yesterday_datetime]
        # dates = [pd.to_datetime("2022-04-10", format="%Y-%m-%d")]
        # dates = pd.date_range(
        #     start=season_dates["start_date"], end=season_dates["final_date"], freq="D"
        # )

        errors = []
        try:
            for date in dates:
                url = f"https://stats.nba.com/stats/league{url_postfix}"

                params = inbound_params
                params["DateTo"] = f'{date.strftime("%m/%d/%Y")}'
                params["Season"] = f'{season_dates["season_years"]}'

                response = requests.get(url, params=params, headers=http_headers)
                data = response.json()

                if database_table_name in ["nba_shooting", "nba_opponent_shooting"]:
                    data_rows = [
                        [date.strftime("%Y%m%d")] + [row[item] for item in data_row_nums]
                        for row in data["resultSets"]["rowSet"]
                    ]
                else:
                    data_rows = [
                        [date.strftime("%Y%m%d")] + [row[item] for item in data_row_nums]
                        for row in data["resultSets"][0]["rowSet"]
                    ]
                df = pd.DataFrame(data_rows, columns=header_row)
                standardized_df = standardize_nbastats_features(df)
                # standardized_df.to_sql(
                #     database_table_name, connection, if_exists="append", index=False
                # )
                print(standardized_df.head())
                print(standardized_df.info())
        except Exception as e:
            errors.append(f"{date} - {e}")
            print(errors)
            print(f"Error Count: {len(errors)}")
            continue


def nba_stats_data_request_ec2(
    years,
    http_headers,
    url_postfix,
    inbound_params,
    header_row,
    data_row_nums,
    database_table_name,
):
    """Scrape and save data from data feed
       that supplies NBA stats tables using EC2 environment.

    Args:
        years (list of int): Years to scrape.
        url_postfix (str): Stat type from url.
                             After 'leaguedash'
        http_headers (dict): Headers for get request.
        inbound_params (dict): Params for get request.
                               Specific to stat type.
        header_row (list of str): Columns headers that match database table.
        data_row_nums (list of int): Data row indexes from Chrome Inspect.
        database_table_name (str): In nba_betting database.
    """
    for year in years:
        season_dates = return_season_dates(year)
        current_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
        yesterday_datetime = current_datetime - datetime.timedelta(days=1)
        dates = [yesterday_datetime]
        # dates = [pd.to_datetime("2022-04-10", format="%Y-%m-%d")]
        # dates = pd.date_range(
        #     start=season_dates["start_date"], end=season_dates["final_date"], freq="D"
        # )

        errors = []
        try:
            for date in dates:
                url = f"https://stats.nba.com/stats/league{url_postfix}"

                params = inbound_params
                params["DateTo"] = f'{date.strftime("%m/%d/%Y")}'
                params["Season"] = f'{season_dates["season_years"]}'

                ssl._create_default_https_context = ssl._create_unverified_context

                url_parts = list(urlparse.urlparse(url))
                query = dict(urlparse.parse_qsl(url_parts[4]))
                query.update(params)
                url_parts[4] = urlencode(query)
                full_url = urlparse.urlunparse(url_parts)

                nba_stats_request = urllib.request.Request(
                    full_url, headers=http_headers
                )

                opener = urllib.request.build_opener(
                    urllib.request.ProxyHandler(WEB_UNLOCKER_PROXY_HANDLER)
                )
                response = opener.open(nba_stats_request).read().decode("utf-8")

                data = json.loads(response)

                if database_table_name in ["nba_shooting", "nba_opponent_shooting"]:
                    data_rows = [
                        [date.strftime("%Y%m%d")] + [row[item] for item in data_row_nums]
                        for row in data["resultSets"]["rowSet"]
                    ]
                else:
                    data_rows = [
                        [date.strftime("%Y%m%d")] + [row[item] for item in data_row_nums]
                        for row in data["resultSets"][0]["rowSet"]
                    ]
                df = pd.DataFrame(data_rows, columns=header_row)
                standardized_df = standardize_nbastats_features(df)
                standardized_df.to_sql(
                    database_table_name, connection, if_exists="append", index=False
                )
        except Exception as e:
            errors.append(f"{date} - {e}")
            print(errors)
            print(f"Error Count: {len(errors)}")
            continue


def standardize_nbastats_features(df):
    """Standardize NBA stats features.

    Args:
    df (pd.DataFrame): The input dataframe.

    Returns:
    pd.DataFrame: The input dataframe with additional features added.

    """
    non_features = ["date", "team"]
    features = [i for i in list(df) if i not in non_features]
    standardized_df = df.copy()
    for feature in features:
        # League Rank
        standardized_df[f"{feature}_rank"] = standardized_df.groupby(["date"])[
            feature
        ].rank(method="min", ascending=False)
        # Z-Score
        means = standardized_df.groupby("date")[feature].transform("mean")
        stdevs = standardized_df.groupby("date")[feature].transform("std")
        standardized_df[f"{feature}_zscore"] = (standardized_df[feature] - means) / (
            stdevs + 1e-10
        )
    return standardized_df


if __name__ == "__main__":
    exception = None

    years = [2023]

    for stat_datatype in [
        "traditional",
        "advanced",
        "four_factors",
        "misc",
        "scoring",
        "opponent",
        "speed_distance",
        "shooting",
        "opponent_shooting",
        "hustle",
    ]:

        try:
            username = "postgres"
            password = RDS_PASSWORD
            endpoint = RDS_ENDPOINT
            database = "nba_betting"
            engine = create_engine(
                f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
            )

            with engine.connect() as connection:
                nba_stats_data_request_local(
                    years,
                    HTTP_REQUEST_HEADERS,
                    URL_POSTFIX[stat_datatype],
                    PARAMS[stat_datatype],
                    HEADER_ROWS[stat_datatype],
                    DATA_ROWS[stat_datatype],
                    f"nbastats_{stat_datatype}",
                )

                # nba_stats_data_request_ec2(
                #     years,
                #     HTTP_REQUEST_HEADERS,
                #     URL_POSTFIX[stat_datatype],
                #     PARAMS[stat_datatype],
                #     HEADER_ROWS[stat_datatype],
                #     DATA_ROWS[stat_datatype],
                #     f"nbastats_{stat_datatype}",
                # )

            print(f"NBAStats {stat_datatype} Updated.")
        except Exception as e:
            exception = e
            print(f"{stat_datatype} failed to load.")
            continue

    if exception:
        print("-----NBAStats Data Update Failed-----")
        raise exception
    else:
        print("-----NBAStats Data Update Successful-----")
