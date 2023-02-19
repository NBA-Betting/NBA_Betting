import datetime
import io
import sys

import flask
import matplotlib.dates as mdates
import matplotlib.style as style
import numpy as np
import pandas as pd
import pytz
from flask import Response, request
from flask_httpauth import HTTPBasicAuth
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from sqlalchemy import create_engine
from werkzeug.security import check_password_hash, generate_password_hash

sys.path.append("../../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

app = flask.Flask(__name__)
auth = HTTPBasicAuth()

users = {"jeff": generate_password_hash(RDS_PASSWORD)}


@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username


def nba_data_inbound():
    todays_datetime = datetime.datetime.now(pytz.timezone("America/Denver")).strftime(
        "%Y-%m-%d"
    )
    yesterdays_datetime = (
        datetime.datetime.now(pytz.timezone("America/Denver"))
        - datetime.timedelta(days=1)
    ).strftime("%Y-%m-%d")
    week_ago_datetime = (
        datetime.datetime.now(pytz.timezone("America/Denver"))
        - datetime.timedelta(days=7)
    ).strftime("%Y-%m-%d %H:%M:%S")
    month_ago_datetime = (
        datetime.datetime.now(pytz.timezone("America/Denver"))
        - datetime.timedelta(days=30)
    ).strftime("%Y-%m-%d %H:%M:%S")
    year_ago_datetime = (
        datetime.datetime.now(pytz.timezone("America/Denver"))
        - datetime.timedelta(days=365)
    ).strftime("%Y-%m-%d %H:%M:%S")

    # SQL Queries
    records_query = """
                    SELECT game_records.game_id,
                        date,
                        home,
                        away,
                        home_line,
                        game_score,
                        game_score_direction,
                        rec_bet_amount,
                        game_result,
                        bet_status,
                        bet_outcome,
                        bet_amount,
                        bet_line,
                        bet_direction,
                        bet_price,
                        bet_location,
                        bet_profit_loss,
                        bet_datetime,
                        game_info,
                        bet_direction_vote
                        FROM game_records
                        FULL OUTER JOIN bets
                        ON game_records.game_id = bets.game_id
                        ORDER BY game_records.game_id DESC
                        LIMIT 100"""

    current_balance_query = (
        "SELECT balance FROM bank_account ORDER BY datetime DESC LIMIT 1;"
    )

    year_ago_query = f"SELECT * FROM bank_account WHERE datetime < '{year_ago_datetime}' ORDER BY datetime DESC LIMIT 1;"

    month_ago_query = f"SELECT * FROM bank_account WHERE datetime < '{month_ago_datetime}' ORDER BY datetime DESC LIMIT 1;"

    week_ago_query = f"SELECT * FROM bank_account WHERE datetime < '{week_ago_datetime}' ORDER BY datetime DESC LIMIT 1;"

    yest_win_loss_query = f"""SELECT COALESCE(SUM(bets.bet_profit_loss), 0)
                              FROM game_records
                              FULL OUTER JOIN bets
                              ON game_records.game_id = bets.game_id
                              WHERE game_records.date = '{yesterdays_datetime}'
                              ;"""

    rec_bets_query = f"SELECT COUNT(*) FROM game_records WHERE date = '{todays_datetime}' AND rec_bet_amount > 0;"

    active_bets_query = (
        "SELECT COUNT(*) FROM bets WHERE bet_status IN ('Active', 'active', 'ACTIVE');"
    )

    money_in_play_query = "SELECT COALESCE(SUM(bet_amount), 0) FROM bets WHERE bet_status IN ('Active', 'active', 'ACTIVE');"

    # Database Calls
    with engine.connect() as connection:
        records = connection.execute(records_query).fetchall()
        current_balance = connection.execute(current_balance_query).fetchall()[0][0]
        # year_ago_balance = connection.execute(year_ago_query).fetchall()[0][0]
        month_ago_balance = connection.execute(month_ago_query).fetchall()[0][1]
        week_ago_balance = connection.execute(week_ago_query).fetchall()[0][1]
        yest_win_loss = connection.execute(yest_win_loss_query).fetchall()[0][0]
        rec_bets_count = connection.execute(rec_bets_query).fetchall()[0][0]
        active_bets_count = connection.execute(active_bets_query).fetchall()[0][0]
        money_in_play = connection.execute(money_in_play_query).fetchall()[0][0]

    # Python/Data Formatting
    records_df = pd.DataFrame(records)
    records_df[5] = records_df[5].apply(lambda x: round(x))
    records_df[7] = records_df[7].apply(lambda x: f"${round(x)}")
    records_df[8] = records_df[8].apply(lambda x: "-" if pd.isnull(x) else f"{x:.0f}")
    records_df[9] = records_df[9].apply(lambda x: "-" if pd.isnull(x) else x)
    records_df[10] = records_df[10].apply(lambda x: "-" if pd.isnull(x) else x)
    records_df[11] = records_df[11].apply(lambda x: "-" if pd.isnull(x) else f"${x:.0f}")
    records_df[12] = records_df[12].apply(lambda x: "-" if pd.isnull(x) else x)
    records_df[13] = records_df[13].apply(lambda x: "-" if pd.isnull(x) else x)
    records_df[14] = records_df[14].apply(lambda x: "-" if pd.isnull(x) else f"{x:.0f}")
    records_df[15] = records_df[15].apply(lambda x: "-" if pd.isnull(x) else x)
    records_df[16] = records_df[16].apply(lambda x: "-" if pd.isnull(x) else f"${x:.0f}")
    records_df[17] = records_df[17].apply(lambda x: "-" if pd.isnull(x) else x)
    current_balance = round(current_balance)
    starting_balance = 1000
    year_ago_balance = 1000
    # month_ago_balance = 575
    # week_ago_balance = 810
    alltime_diff = round(current_balance - starting_balance)
    year_diff = round(current_balance - year_ago_balance)
    month_diff = round(current_balance - month_ago_balance)
    week_diff = round(current_balance - week_ago_balance)
    alltime_pct_diff = round(
        ((current_balance - starting_balance) / starting_balance) * 100, 1
    )
    year_pct_diff = round(
        ((current_balance - year_ago_balance) / year_ago_balance) * 100, 1
    )
    month_pct_diff = round(
        ((current_balance - month_ago_balance) / month_ago_balance) * 100, 1
    )
    week_pct_diff = round(
        ((current_balance - week_ago_balance) / week_ago_balance) * 100, 1
    )
    yest_win_loss = round(yest_win_loss)
    money_in_play = round(money_in_play)

    # Return Data
    output_records = list(records_df.to_records(index=False))
    return {
        "records": output_records,
        "current_balance": current_balance,
        "starting_balance": starting_balance,
        "year_ago_balance": year_ago_balance,
        "month_ago_balance": month_ago_balance,
        "week_ago_balance": week_ago_balance,
        "alltime_diff": alltime_diff,
        "year_diff": year_diff,
        "month_diff": month_diff,
        "week_diff": week_diff,
        "alltime_pct_diff": alltime_pct_diff,
        "year_pct_diff": year_pct_diff,
        "month_pct_diff": month_pct_diff,
        "week_pct_diff": week_pct_diff,
        "yest_win_loss": yest_win_loss,
        "rec_bets_count": rec_bets_count,
        "active_bets_count": active_bets_count,
        "money_in_play": money_in_play,
    }


@app.route("/", methods=["POST", "GET"])
@auth.login_required
def home_table():
    data = nba_data_inbound()
    if request.method == "POST":
        bet_datetime = datetime.datetime.now(pytz.timezone("America/Denver")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        bet_game_id = request.form["bet_game_id"]
        bet_status = request.form["bet_status"]
        bet_outcome = request.form["bet_outcome"]
        bet_amount = float(request.form["bet_amount"])
        bet_line = float(request.form["bet_line"])
        bet_direction = request.form["bet_direction"]
        bet_price = int(request.form["bet_price"])
        bet_location = request.form["bet_location"]
        bet_profit_loss = float(request.form["bet_profitloss"])
        old_profit_loss = (
            0
            if request.form["old_profit_loss"] == "-"
            else float(request.form["old_profit_loss"].strip("$"))
        )
        old_bank_balance = float(request.form["bankBalance"])

        diff_bet_profit_loss = bet_profit_loss - old_profit_loss
        new_bank_balance = old_bank_balance + diff_bet_profit_loss

        upsert_stmt = f"""
            INSERT INTO bets (game_id,
                                bet_datetime,
                                bet_status,
                                bet_outcome,
                                bet_amount,
                                bet_price,
                                bet_location,
                                bet_line,
                                bet_profit_loss,
                                bet_direction)
            VALUES ('{bet_game_id}',
                    '{bet_datetime}',
                    '{bet_status}',
                    '{bet_outcome}',
                    {bet_amount},
                    {bet_price},
                    '{bet_location}',
                    {bet_line},
                    {bet_profit_loss},
                    '{bet_direction}')
            ON CONFLICT (game_id)
            DO
                UPDATE
                    SET bet_datetime = '{bet_datetime}',
                        bet_status = '{bet_status}',
                        bet_outcome = '{bet_outcome}',
                        bet_amount = {bet_amount},
                        bet_price = {bet_price},
                        bet_location = '{bet_location}',
                        bet_line = {bet_line},
                        bet_profit_loss = {bet_profit_loss},
                        bet_direction = '{bet_direction}'
            ;
            """

        bank_account_stmt = f"""INSERT INTO bank_account (datetime, balance)
                                VALUES ('{bet_datetime}',{new_bank_balance});
                            """

        with engine.connect() as connection:
            connection.execute(upsert_stmt)
            connection.execute(bank_account_stmt)

        data = nba_data_inbound()
        return flask.render_template("nba_home.html", **data)

    return flask.render_template("nba_home.html", **data)


@app.route("/nba_dashboard")
@auth.login_required
def dashboard():
    # data = nba_dashboard_data_inbound()
    return flask.render_template("nba_dashboard.html")


@app.route("/home_page_plot.png")
def plot_png():
    fig = create_figure()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype="image/png")


def create_figure():
    figure_data_query = "SELECT datetime, balance from (SELECT *, row_number() OVER (PARTITION BY date_trunc('day', datetime) ORDER BY datetime DESC) r FROM bank_account) T WHERE T.r=1;"
    with engine.connect() as connection:
        figure_records = connection.execute(figure_data_query).fetchall()
    figure_df = pd.DataFrame(figure_records)

    style.use("seaborn-whitegrid")
    fig = Figure(dpi=300, tight_layout=True)
    ax = fig.subplots()
    x = figure_df[0]
    y = figure_df[1]

    # ax.set_title("Bank Account Balance", fontsize=36, pad=16)
    ax.tick_params(axis="y", labelsize=20, pad=6)
    ax.yaxis.set_major_formatter("${x:1.0f}")
    ax.tick_params(axis="x", labelsize=20, pad=6)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y\n%b"))
    ax.plot(x, y, color="#0d6efd")
    return fig


@app.after_request
def add_header(r):
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers["Cache-Control"] = "public, max-age=0"
    return r


if __name__ == "__main__":
    username = "postgres"
    password = RDS_PASSWORD
    endpoint = RDS_ENDPOINT
    database = "nba_betting"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    )

    app.debug = True
    app.run()
