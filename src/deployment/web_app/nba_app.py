import datetime
import io
import os
import sys

import matplotlib.dates as mdates
import matplotlib.style as style
import pandas as pd
import pytz
from dotenv import load_dotenv
from flask import Flask, Response, redirect, render_template, request, url_for
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from sqlalchemy import create_engine, text
from werkzeug.security import check_password_hash, generate_password_hash

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))
from bet_management.bet_decisions import on_demand_predictions
from data_sources.game.odds_api import update_game_data

load_dotenv()
DB_ENDPOINT = os.getenv("DB_ENDPOINT")
DB_PASSWORD = os.getenv("DB_PASSWORD")
WEB_APP_SECRET_KEY = os.getenv("WEB_APP_SECRET_KEY")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ----- ON DEMAND UPDATE -----
def on_demand_update():
    # Load Current Lines
    try:
        update_game_data()
    except Exception as e:
        print(e)
        raise Exception("Error updating game data")
    # Create Predictions
    try:
        on_demand_predictions(current_date=True)
    except Exception as e:
        print(e)
        raise Exception("Error creating predictions")


# ----- UTILITY FUNCTIONS -----
def get_db_connection():
    engine = create_engine(
        f"postgresql+psycopg2://postgres:{DB_PASSWORD}@{DB_ENDPOINT}/nba_betting"
    )
    return engine.connect()


def calculate_diff(current, previous):
    if previous:
        diff = round(current - previous)
        pct_diff = round((diff / previous) * 100, 1) if previous != 0 else float("inf")
    else:
        diff = "-"
        pct_diff = "-"
    return diff, pct_diff


def format_currency(value):
    return f"${value:.0f}" if pd.notnull(value) else "-"


def replace_nan_with_hyphen(value):
    return value if pd.notnull(value) else "-"


def create_app():
    app = Flask(__name__)
    app.jinja_env.globals.update(isinstance=isinstance)
    app.jinja_env.globals.update(str=str)
    app.secret_key = WEB_APP_SECRET_KEY
    login_manager = LoginManager()
    login_manager.init_app(app)

    # ----- USER AUTHENTICATION -----

    class User(UserMixin):
        def __init__(self, id, username, password):
            self.id = id
            self.username = username
            self.password_hash = generate_password_hash(password)

        def check_password(self, password):
            return check_password_hash(self.password_hash, password)

    @login_manager.user_loader
    def load_user(user_id):
        admin_username = "jeff"
        admin_password = os.getenv("DB_PASSWORD")
        if user_id == admin_username:
            return User(
                id=admin_username, username=admin_username, password=admin_password
            )
        return None

    def nba_data_inbound():
        # ----- LOAD DATA -----
        current_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
        todays_datetime = current_datetime.strftime("%Y-%m-%d")
        tomorrows_datetime = (current_datetime + datetime.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        yesterdays_datetime = (current_datetime - datetime.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        week_ago_datetime = (current_datetime - datetime.timedelta(days=7)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        month_ago_datetime = (current_datetime - datetime.timedelta(days=30)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        year_ago_datetime = (current_datetime - datetime.timedelta(days=365)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        with get_db_connection() as connection:
            # Current Balance
            current_balance_query = (
                "SELECT balance FROM betting_account ORDER BY datetime DESC LIMIT 1;"
            )
            result = connection.execute(current_balance_query).fetchone()
            current_balance = result[0] if result else None

            # Year Ago Balance
            year_ago_query = text(
                "SELECT * FROM betting_account WHERE datetime < :year_ago_datetime ORDER BY datetime DESC LIMIT 1;"
            )
            result = connection.execute(
                year_ago_query, {"year_ago_datetime": year_ago_datetime}
            ).fetchone()
            year_ago_balance = result[1] if result else None

            # Month Ago Balance
            month_ago_query = text(
                "SELECT * FROM betting_account WHERE datetime < :month_ago_datetime ORDER BY datetime DESC LIMIT 1;"
            )
            result = connection.execute(
                month_ago_query, {"month_ago_datetime": month_ago_datetime}
            ).fetchone()
            month_ago_balance = result[1] if result else None

            # Week Ago Balance
            week_ago_query = text(
                "SELECT * FROM betting_account WHERE datetime < :week_ago_datetime ORDER BY datetime DESC LIMIT 1;"
            )
            result = connection.execute(
                week_ago_query, {"week_ago_datetime": week_ago_datetime}
            ).fetchone()
            week_ago_balance = result[1] if result else None

            # Yesterday's Win/Loss
            yest_win_loss_query = text(
                """SELECT COALESCE(SUM(bets.bet_profit_loss), 0)
                            FROM games
                            FULL OUTER JOIN bets
                            ON games.game_id = bets.game_id
                            WHERE DATE(games.game_datetime) = :yesterdays_datetime
                            ;"""
            )
            result = connection.execute(
                yest_win_loss_query, {"yesterdays_datetime": yesterdays_datetime}
            ).fetchone()
            yest_win_loss = result[0] if result else 0

            # Active Bets
            active_bets_query = text(
                "SELECT COUNT(*) FROM bets WHERE bet_status IN ('Active', 'active', 'ACTIVE');"
            )
            result = connection.execute(active_bets_query).fetchone()
            active_bets_count = result[0] if result else 0

            # Money in Play
            money_in_play_query = text(
                "SELECT COALESCE(SUM(bet_amount), 0) FROM bets WHERE bet_status IN ('Active', 'active', 'ACTIVE');"
            )
            result = connection.execute(money_in_play_query).fetchone()
            money_in_play = result[0] if result else 0

            # Game Table with Latest Predictions
            game_table_query = text(
                """
                WITH LatestPredictions AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY prediction_datetime DESC) AS rn
                    FROM predictions
                )
                SELECT 
                    games.game_id,
                    games.game_datetime,
                    games.home_team,
                    games.away_team,
                    games.open_line,
                    games.home_score,
                    games.away_score,
                    LatestPredictions.prediction_line_hv,
                    LatestPredictions.directional_game_rating,
                    LatestPredictions.prediction_direction,
                    bets.bet_status,
                    bets.bet_amount,
                    bets.bet_line,
                    bets.bet_direction,
                    bets.bet_price,
                    bets.bet_location,
                    bets.bet_profit_loss,
                    bets.bet_datetime
                FROM games
                FULL OUTER JOIN bets ON games.game_id = bets.game_id
                FULL OUTER JOIN LatestPredictions ON games.game_id = LatestPredictions.game_id
                WHERE (LatestPredictions.rn = 1 OR LatestPredictions.rn IS NULL)
                AND games.game_datetime < :tomorrows_datetime
                AND games.game_datetime >= :start_date
                ORDER BY games.game_datetime DESC, LatestPredictions.directional_game_rating DESC
                LIMIT 100;
            """
            )

            start_date = "2022-09-01"
            game_table = connection.execute(
                game_table_query,
                {"tomorrows_datetime": tomorrows_datetime, "start_date": start_date},
            ).fetchall()

        # ----- FORMAT DATA -----
        current_balance_rounded = round(current_balance)
        starting_balance = 1000

        alltime_diff, alltime_pct_diff = calculate_diff(
            current_balance, starting_balance
        )
        year_diff, year_pct_diff = calculate_diff(current_balance, year_ago_balance)
        month_diff, month_pct_diff = calculate_diff(current_balance, month_ago_balance)
        week_diff, week_pct_diff = calculate_diff(current_balance, week_ago_balance)

        yest_win_loss = round(yest_win_loss)
        money_in_play = round(money_in_play)

        records_df = pd.DataFrame(game_table)

        records_df["game_result"] = records_df.apply(
            lambda row: int(row["home_score"] - row["away_score"])
            if pd.notnull(row["home_score"]) and pd.notnull(row["away_score"])
            else "-",
            axis=1,
        )
        records_df["open_line_hv"] = 0 - records_df["open_line"]
        records_df["game_datetime"] = records_df["game_datetime"].apply(
            lambda x: x.strftime("%Y-%m-%d %H:%M")
        )
        records_df["game_rating"] = records_df["directional_game_rating"].apply(
            lambda x: round(x) if pd.notnull(x) else "-"
        )
        records_df["bet_price"] = records_df["bet_price"].apply(
            lambda x: "-" if pd.isnull(x) else int(x)
        )

        # Currency Formatting
        currency_cols = ["bet_amount", "bet_profit_loss"]
        records_df[currency_cols] = records_df[currency_cols].applymap(format_currency)

        # NAN Formatting
        normal_nan_cols = [
            "prediction_line_hv",
            "prediction_direction",
            "bet_status",
            "bet_line",
            "bet_direction",
            "bet_location",
            "bet_datetime",
        ]
        records_df[normal_nan_cols] = records_df[normal_nan_cols].applymap(
            replace_nan_with_hyphen
        )

        output_records = records_df.to_dict("records")

        # ----- RETURN DATA -----
        return {
            "records": output_records,
            "current_balance": current_balance,
            "current_balance_rounded": current_balance_rounded,
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

            user = load_user(username)

            if user and user.check_password(password):
                login_user(user)
                return redirect(url_for("home_table"))
            else:
                return render_template(
                    "login.html", error="Invalid username or password"
                )
        else:
            return render_template("login.html")

    @app.route("/logout")
    @login_required
    def logout():
        logout_user()
        return redirect(url_for("home_table"))

    @app.route("/", methods=["POST", "GET"])
    def home_table():
        if current_user.is_authenticated:
            on_demand_update()

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
            old_account_balance = float(request.form["accountBalance"])

            diff_bet_profit_loss = bet_profit_loss - old_profit_loss
            new_account_balance = old_account_balance + diff_bet_profit_loss

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

            betting_account_stmt = """
                INSERT INTO betting_account (datetime, balance)
                VALUES (:bet_datetime, :new_account_balance);
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

            with get_db_connection() as connection:
                connection.execute(text(upsert_stmt), params)
                connection.execute(
                    text(betting_account_stmt),
                    {
                        "bet_datetime": bet_datetime,
                        "new_account_balance": new_account_balance,
                    },
                )

            if current_user.is_authenticated:
                on_demand_update()
            data = nba_data_inbound()
            return render_template("nba_home.html", **data)

        return render_template("nba_home.html", **data)

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

    # ----- HOME PAGE PLOT of BETTING ACCOUNT BALANCE -----

    @app.route("/home_page_plot.png")
    def plot_png():
        fig = create_figure()
        output = io.BytesIO()
        FigureCanvas(fig).print_png(output)
        return Response(output.getvalue(), mimetype="image/png")

    def create_figure():
        figure_data_query = """SELECT datetime,
                                    balance
                                FROM (SELECT *,
                                        row_number()
                                        OVER (PARTITION BY date_trunc('day', datetime)
                                            ORDER BY datetime DESC) r
                                        FROM betting_account) T
                                WHERE T.r=1;"""
        with get_db_connection() as connection:
            figure_records = connection.execute(figure_data_query).fetchall()
        figure_df = pd.DataFrame(figure_records)
        style.use("seaborn-v0_8-whitegrid")
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
