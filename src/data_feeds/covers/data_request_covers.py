import datetime
import re
import sys

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from covers_config import COVERS_SEASON_ID, COVERS_TEAM_ID, TEAM_NAMES
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.append("../../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 200)


def get_game_date(row, league_year):
    """
    Extract the date of a game from a row of data.

    Arguments:
    row -- an HTML element representing a row of data
    league_year -- a string representing the year of the league in the format "YYYY-YYYY"

    Returns:
    A localized datetime object representing the date of the game, or None if no date was found in the row.

    Raises:
    Prints an error message if the year of the game date could not be determined.
    """
    td = row.find("td")
    if td:
        first_two_items = td.text.strip().split(" ")[:2]
        combined_text = (
            " ".join(first_two_items)
            .replace("W", "")
            .replace("L", "")
            .replace("\n", "")
            .replace("\r", "")
        )
        year = None
        if combined_text.split()[0] in ["Nov", "Dec"]:
            year = league_year.split("-")[0]
        elif (combined_text.split()[0] == "Oct") and (
            int(combined_text.split()[1]) > 12
        ):
            year = league_year.split("-")[0]
        elif combined_text.split()[0] in [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
        ]:
            year = league_year.split("-")[1]
        else:
            print("Issue finding the year for the game date.")
        game_date = datetime.datetime.strptime(f"{combined_text} {year}", "%b %d %Y")
        game_date = pytz.timezone("America/Denver").localize(game_date)
        return game_date
    else:
        print("Error parsing game date from html row.")


def game_data_http_request(team, league_year):
    """
    Send an HTTP request to retrieve game data for a team in a specific league year.

    Arguments:
    team -- a string representing the team for which to retrieve data
    league_year -- a string representing the year of the league in the format "YYYY-YYYY"

    Returns:
    A list of HTML elements representing rows of game data.

    Raises:
    Prints an error message if the season ID for the specified league year is not in the `COVERS_SEASON_ID` dictionary.
    """
    season_id = None
    if league_year in COVERS_SEASON_ID:
        season_id = COVERS_SEASON_ID[league_year]
    else:
        print("Season ID not yet added to COVERS_SEASON_ID dict.")

    url = f"https://www.covers.com/sport/basketball/nba/teams/main/{team}/tab/schedule"

    headers = {
        "authority": "www.covers.com",
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "dnt": "1",
        "origin": "https://www.covers.com",
        "referer": f"https://www.covers.com/sport/basketball/nba/teams/main/{team}/{league_year}/schedule",
        "sec-ch-ua": '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    data = {
        "teamId": COVERS_TEAM_ID[team],
        "seasonId": season_id,
        "seasonName": league_year,
        "leagueName": "NBA",
    }

    response = requests.post(url, headers=headers, data=data)
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("tbody")
    rows = table.find_all("tr")
    return rows


def process_html_rows(rows, team, league_year):
    """
    Process HTML elements representing rows of game data into a pandas DataFrame.

    Arguments:
    rows -- a list of HTML elements representing rows of game data
    team -- a string representing the team for which the data was retrieved
    league_year -- a string representing the year of the league in the format "YYYY-YYYY"

    Returns:
    A pandas DataFrame containing information about each game, including the date, opponent, result, score, and spread.
    """
    opponent_abbrv_issue_dict = {
        "CHAR": "CHA",
        "NETS": "BKN",
        "NJ": "BKN",
        "BK": "BKN",
        "GS": "GSW",
        "NO": "NOP",
        "NY": "NYK",
        "PHO": "PHX",
        "SA": "SAS",
    }
    parsed_records = pd.DataFrame()
    for row in rows:
        game_date = get_game_date(row, league_year)
        if game_date == "No date found in this row.":
            continue
        if (
            game_date.date()
            >= datetime.datetime.now(pytz.timezone("America/Denver")).date()
        ):
            continue
        game_date = game_date.strftime("%Y%m%d")
        # team, opponent
        team_abbrv = TEAM_NAMES[team]
        img_tag = row.find("img")
        opponent = img_tag.find_next_sibling("a").text.strip()
        if "@" in opponent:
            continue
        if opponent in opponent_abbrv_issue_dict:
            opponent = opponent_abbrv_issue_dict[opponent]
        # id_num
        a_tag = row.find("a", href=re.compile("/sport/basketball/nba/boxscore/"))
        id_num = re.search("\d{6}", a_tag["href"]).group()
        # game_url
        a_tag = row.find("a", href=re.compile("/sport/basketball/nba/boxscore/"))
        game_url = a_tag.get("href")
        # result, score, opponent_score
        a_tag = row.find("a", href=re.compile("/sport/basketball/nba/boxscore/"))
        score_info = a_tag.text
        result, scores = score_info.split(" ")[0], score_info.split(" ")[1]
        score, opponent_score = scores.split("-")
        # spread, spread_result
        spread_text = row.text.strip().split("\n")[-2]
        spread = spread_text.split(" ")[1]
        spread_result = spread_text.split(" ")[0]

        new_record = pd.DataFrame(
            {
                "team": [team_abbrv],
                "game_date": [game_date],
                "league_year": [league_year],
                "id_num": [id_num],
                "game_url": [game_url],
                "opponent": [opponent],
                "result": [result],
                "score": [score],
                "opponent_score": [opponent_score],
                "spread_result": [spread_result],
                "spread": [spread],
            }
        )

        parsed_records = pd.concat([parsed_records, new_record], ignore_index=True)

    return parsed_records


def save_records(records, engine):
    """
    Function to perform an upsert (insert-or-update) for multiple records in a database table.

    Arguments:
    records -- A pandas dataframe containing the records to be upserted.
    engine -- A SQLAlchemy engine object that connects to the database.

    Returns:
    None
    """
    statement = text(
        """
        INSERT INTO covers(id_num,
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
                    VALUES(:id_num, :game_date, :league_year, :game_url, :team, :score, :opponent, :opponent_score, :result, :spread, :spread_result)
                    ON CONFLICT (id_num)
                    DO UPDATE
                        SET date = :game_date,
                            league_year = :league_year,
                            game_url = :game_url,
                            team = :team,
                            score = :score,
                            opponent = :opponent,
                            opponent_score = :opponent_score,
                            result = :result,
                            spread = :spread,
                            spread_result = :spread_result;
    """
    )
    with engine.connect() as conn:
        for _, row in records.iterrows():
            conn.execute(
                statement,
                id_num=row["id_num"],
                game_date=row["game_date"],
                league_year=row["league_year"],
                game_url=row["game_url"],
                team=row["team"],
                score=row["score"],
                opponent=row["opponent"],
                opponent_score=row["opponent_score"],
                result=row["result"],
                spread=row["spread"],
                spread_result=row["spread_result"],
            )


if __name__ == "__main__":
    try:
        username = "postgres"
        password = RDS_PASSWORD
        endpoint = RDS_ENDPOINT
        database = "nba_betting"
        engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
        )

        league_year = "2022-2023"

        for team in TEAM_NAMES:
            print(team)
            rows = game_data_http_request(team, league_year)
            records = process_html_rows(rows, team, league_year)
            print(records)
            # save_records(records, engine)

    except Exception as e:
        print("-----Covers Past Game Data Update Failed-----")
        raise e
    else:
        print("-----Covers Past Game Data Update Successful-----")
