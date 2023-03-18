import datetime
import io
import os
import sys
from builtins import isinstance

import flask
import matplotlib.dates as mdates
import matplotlib.style as style
import pandas as pd
import pytz
from flask import Response, redirect, render_template, request, url_for
from flask_login import LoginManager, UserMixin, current_user, login_user, logout_user
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from sqlalchemy import create_engine
from werkzeug.security import check_password_hash, generate_password_hash

sys.path.append("../../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD, WEB_APP_SECRET_KEY

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def create_app():
    app = flask.Flask(__name__)
    app.jinja_env.globals.update(isinstance=isinstance)
    app.jinja_env.globals.update(str=str)
    app.secret_key = WEB_APP_SECRET_KEY
    login_manager = LoginManager()
    login_manager.init_app(app)

    # ----- USER AUTHENTICATION -----

    users = {
        "jeff": {"username": "jeff", "password": RDS_PASSWORD},
    }

    class User(UserMixin):
        def __init__(self, id, username, password):
            self.id = id
            self.username = username
            self.password_hash = generate_password_hash(password)

        def check_password(self, password):
            return check_password_hash(self.password_hash, password)

    @login_manager.user_loader
    def load_user(user_id):
        user_dict = users.get(user_id)
        if user_dict:
            return User(user_id, user_dict["username"], user_dict["password"])
        return None

    def nba_data_inbound():
        # ----- LOAD DATA -----
        todays_datetime = datetime.datetime.now(
            pytz.timezone("America/Denver")
        ).strftime("%Y-%m-%d")
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

        username = "postgres"
        password = RDS_PASSWORD
        endpoint = RDS_ENDPOINT
        database = "nba_betting"

        engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
        )

        with engine.connect() as connection:
            # Current Balance
            current_balance_query = (
                "SELECT balance FROM bank_account ORDER BY datetime DESC LIMIT 1;"
            )
            current_balance = connection.execute(current_balance_query).fetchall()[0][0]

            # Year Ago Balance
            year_ago_query = f"SELECT * FROM bank_account WHERE datetime < '{year_ago_datetime}' ORDER BY datetime DESC LIMIT 1;"
            year_ago_result = connection.execute(year_ago_query).fetchall()
            if year_ago_result:
                year_ago_balance = year_ago_result[0][1]
            else:
                year_ago_balance = None

            # Month Ago Balance
            month_ago_query = f"SELECT * FROM bank_account WHERE datetime < '{month_ago_datetime}' ORDER BY datetime DESC LIMIT 1;"
            month_ago_result = connection.execute(month_ago_query).fetchall()
            if month_ago_result:
                month_ago_balance = month_ago_result[0][1]
            else:
                month_ago_balance = None

            # Week Ago Balance
            week_ago_query = f"SELECT * FROM bank_account WHERE datetime < '{week_ago_datetime}' ORDER BY datetime DESC LIMIT 1;"
            week_ago_result = connection.execute(week_ago_query).fetchall()
            if week_ago_result:
                week_ago_balance = week_ago_result[0][1]
            else:
                week_ago_balance = None

            # Yesterday's Win/Loss
            yest_win_loss_query = f"""SELECT COALESCE(SUM(bets.bet_profit_loss), 0)
                                FROM game_records
                                FULL OUTER JOIN bets
                                ON game_records.game_id = bets.game_id
                                WHERE game_records.date = '{yesterdays_datetime}'
                                ;"""
            yest_win_loss = connection.execute(yest_win_loss_query).fetchall()[0][0]

            # Active Bets
            active_bets_query = "SELECT COUNT(*) FROM bets WHERE bet_status IN ('Active', 'active', 'ACTIVE');"
            active_bets_count = connection.execute(active_bets_query).fetchall()[0][0]

            # Money in Play
            money_in_play_query = "SELECT COALESCE(SUM(bet_amount), 0) FROM bets WHERE bet_status IN ('Active', 'active', 'ACTIVE');"
            money_in_play = connection.execute(money_in_play_query).fetchall()[0][0]

            # Game Table
            games_query = """
                        SELECT game_records.game_id,
                            date,
                            home,
                            away,
                            home_line,
                            game_score,
                            game_score_direction,
                            game_result,
                            bet_status,
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
                            ORDER BY game_records.date DESC, game_records.game_score DESC
                            LIMIT 100"""
            games = connection.execute(games_query).fetchall()

        # ----- FORMAT DATA -----
        current_balance = round(current_balance)
        starting_balance = 1000
        alltime_diff = round(current_balance - starting_balance)
        alltime_pct_diff = round((alltime_diff / starting_balance) * 100, 1)

        if year_ago_balance:
            year_diff = round(current_balance - year_ago_balance)
            if year_ago_balance != 0:
                year_pct_diff = round((year_diff / year_ago_balance) * 100, 1)
            else:
                year_pct_diff = float("inf")
        else:
            year_diff = "-"
            year_pct_diff = "-"

        if month_ago_balance:
            month_diff = round(current_balance - month_ago_balance)
            if month_ago_balance != 0:
                month_pct_diff = round((month_diff / month_ago_balance) * 100, 1)
            else:
                month_pct_diff = float("inf")
        else:
            month_diff = "-"
            month_pct_diff = "-"

        if week_ago_balance:
            week_diff = round(current_balance - week_ago_balance)
            if week_ago_balance != 0:
                week_pct_diff = round((week_diff / week_ago_balance) * 100, 1)
            else:
                week_pct_diff = float("inf")
        else:
            week_diff = "-"
            week_pct_diff = "-"

        yest_win_loss = round(yest_win_loss)
        money_in_play = round(money_in_play)

        records_df = pd.DataFrame(games)
        records_df["game_score"] = records_df["game_score"].apply(lambda x: round(x))
        records_df["game_result"] = records_df["game_result"].apply(
            lambda x: "-" if pd.isnull(x) else f"{x:.0f}"
        )
        records_df["bet_status"] = records_df["bet_status"].apply(
            lambda x: "-" if pd.isnull(x) else x
        )
        records_df["bet_amount"] = records_df["bet_amount"].apply(
            lambda x: "-" if pd.isnull(x) else f"${x:.0f}"
        )
        records_df["bet_line"] = records_df["bet_line"].apply(
            lambda x: "-" if pd.isnull(x) else x
        )
        records_df["bet_direction"] = records_df["bet_direction"].apply(
            lambda x: "-" if pd.isnull(x) else x
        )
        records_df["bet_price"] = records_df["bet_price"].apply(
            lambda x: "-" if pd.isnull(x) else f"{x:.0f}"
        )
        records_df["bet_location"] = records_df["bet_location"].apply(
            lambda x: "-" if pd.isnull(x) else x
        )
        records_df["bet_profit_loss"] = records_df["bet_profit_loss"].apply(
            lambda x: "-" if pd.isnull(x) else f"${x:.0f}"
        )
        records_df["bet_datetime"] = records_df["bet_datetime"].apply(
            lambda x: "-" if pd.isnull(x) else x
        )

        output_records = list(records_df.to_records(index=False))

        # ----- RETURN DATA -----
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
            "active_bets_count": active_bets_count,
            "money_in_play": money_in_play,
        }

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form["username"]
            password = request.form["password"]
            user = users.get(username)
            if user and user["password"] == password:
                user = User(
                    id=username, username=user["username"], password=user["password"]
                )
                login_user(user)
                return redirect(url_for("home_table"))
            else:
                return render_template(
                    "login.html", error="Invalid username or password"
                )
        else:
            return render_template("login.html")

    @app.route("/logout")
    def logout():
        logout_user()
        return redirect(url_for("home_table"))

    @app.route("/", methods=["POST", "GET"])
    def home_table():
        username = "postgres"
        password = RDS_PASSWORD
        endpoint = RDS_ENDPOINT
        database = "nba_betting"

        engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
        )
        data = nba_data_inbound()
        if request.method == "POST":
            bet_datetime = datetime.datetime.now(
                pytz.timezone("America/Denver")
            ).strftime("%Y-%m-%d %H:%M:%S")

            bet_game_id = request.form["bet_game_id"]
            bet_status = request.form["bet_status"]
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

            upsert_stmt = """
                INSERT INTO bets (game_id, bet_datetime, bet_status, bet_amount, bet_price, bet_location, bet_line, bet_profit_loss, bet_direction)
                VALUES (:bet_game_id, :bet_datetime, :bet_status, :bet_amount, :bet_price, :bet_location, :bet_line, :bet_profit_loss, :bet_direction)
                ON CONFLICT (game_id)
                DO
                    UPDATE
                        SET bet_datetime = :bet_datetime,
                            bet_status = :bet_status,
                            bet_amount = :bet_amount,
                            bet_price = :bet_price,
                            bet_location = :bet_location,
                            bet_line = :bet_line,
                            bet_profit_loss = :bet_profit_loss,
                            bet_direction = :bet_direction
                ;
            """

            bank_account_stmt = """
                INSERT INTO bank_account (datetime, balance)
                VALUES (:bet_datetime, :new_bank_balance);
            """

            params = {
                "bet_game_id": bet_game_id,
                "bet_datetime": bet_datetime,
                "bet_status": bet_status,
                "bet_amount": bet_amount,
                "bet_price": bet_price,
                "bet_location": bet_location,
                "bet_line": bet_line,
                "bet_profit_loss": bet_profit_loss,
                "bet_direction": bet_direction,
            }

            with engine.connect() as connection:
                connection.execute(upsert_stmt, params)
                connection.execute(
                    bank_account_stmt,
                    {"bet_datetime": bet_datetime, "new_bank_balance": new_bank_balance},
                )

            data = nba_data_inbound()
            return flask.render_template("nba_home.html", **data)

        return flask.render_template("nba_home.html", **data)

    @app.route("/nba_dashboard")
    def dashboard():
        dashboard_url = (
            "/private_nba_dashboard/"
            if current_user.is_authenticated
            else "/public_nba_dashboard/"
        )
        return render_template("nba_dashboard.html", dashboard_url=dashboard_url)

    @app.context_processor
    def inject_app():
        return dict(app=app)

    # ----- HOME PAGE PLOT of BANK ACCOUNT BALANCE -----

    @app.route("/home_page_plot.png")
    def plot_png():
        fig = create_figure()
        output = io.BytesIO()
        FigureCanvas(fig).print_png(output)
        return Response(output.getvalue(), mimetype="image/png")

    def create_figure():
        username = "postgres"
        password = RDS_PASSWORD
        endpoint = RDS_ENDPOINT
        database = "nba_betting"

        engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
        )
        figure_data_query = "SELECT datetime, balance from (SELECT *, row_number() OVER (PARTITION BY date_trunc('day', datetime) ORDER BY datetime DESC) r FROM bank_account) T WHERE T.r=1;"
        with engine.connect() as connection:
            figure_records = connection.execute(figure_data_query).fetchall()
        figure_df = pd.DataFrame(figure_records)
        style.use("seaborn-whitegrid")
        fig = Figure(dpi=300, tight_layout=True)
        fig.set_size_inches(16, 5)
        ax = fig.subplots()
        x = figure_df["datetime"]
        y = figure_df["balance"]

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

    return app


app = create_app()

from dashboard.private_dashboard import init_private_dashboard
from dashboard.public_dashboard import init_public_dashboard

private_app = init_private_dashboard(app)
public_app = init_public_dashboard(app)

if __name__ == "__main__":
    pass
    # app.run(debug=True)
