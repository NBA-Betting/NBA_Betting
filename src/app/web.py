"""
NBA Betting Web Application

A Flask + Dash application for viewing NBA game predictions and betting analytics.
"""

import datetime
import io
import os

import matplotlib.dates as mdates
import matplotlib.style as style
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, Response, redirect, render_template, request, url_for
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from sqlalchemy import text

from src.predictions import on_demand_predictions
from src.config import STARTING_BALANCE
from src.data_sources.game.odds_api import update_game_data
from src.database import get_engine
from src.utils.timezone import APP_TIMEZONE, get_current_time

load_dotenv()
# Secret key for Flask sessions - auto-generates if not set (fine for local dev)
WEB_APP_SECRET_KEY = os.getenv("WEB_APP_SECRET_KEY")
if not WEB_APP_SECRET_KEY:
    import secrets

    WEB_APP_SECRET_KEY = secrets.token_hex(32)
ODDS_API_KEY = os.getenv("ODDS_API_KEY")


# ----- ON DEMAND UPDATE -----
def on_demand_update():
    """Update game data and predictions on demand."""
    try:
        update_game_data()
    except Exception as e:
        raise RuntimeError(f"Error updating game data: {e}") from e
    try:
        on_demand_predictions(current_date=True)
    except Exception as e:
        raise RuntimeError(f"Error creating predictions: {e}") from e


# ----- UTILITY FUNCTIONS -----
def get_db_connection():
    """Get a database connection using the centralized engine."""
    engine = get_engine()
    return engine.connect()


def calculate_diff(current, previous):
    """Calculate difference and percentage difference between two values."""
    if current is None or previous is None:
        return "-", "-"
    if previous == 0:
        # Can't calculate percentage change from zero
        diff = round(current - previous)
        return diff, "-"
    diff = round(current - previous)
    pct_diff = round((diff / previous) * 100, 1)
    return diff, pct_diff


def format_currency(value):
    """Format a number as currency."""
    return f"${value:.0f}" if pd.notnull(value) else "-"


def replace_nan_with_hyphen(value):
    """Replace NaN/None with hyphen for display."""
    return value if pd.notnull(value) else "-"


def create_app():
    """Application factory for the Flask app."""
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )
    app.jinja_env.globals.update(isinstance=isinstance)
    app.jinja_env.globals.update(str=str)
    app.secret_key = WEB_APP_SECRET_KEY

    def nba_data_inbound():
        """Load and format data for the home page."""
        current_datetime = get_current_time()
        todays_datetime = current_datetime.strftime("%Y-%m-%d")
        tomorrows_datetime = (current_datetime + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        today = current_datetime.strftime("%Y-%m-%d")
        day_after_tomorrow = (current_datetime + datetime.timedelta(days=2)).strftime("%Y-%m-%d")
        yesterdays_datetime = (current_datetime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
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
            current_balance_query = text(
                "SELECT balance FROM betting_account ORDER BY datetime DESC LIMIT 1;"
            )
            result = connection.execute(current_balance_query).fetchone()
            current_balance = result[0] if result else STARTING_BALANCE

            # Year Ago Balance
            year_ago_query = text(
                "SELECT balance FROM betting_account WHERE datetime < :year_ago_datetime ORDER BY datetime DESC LIMIT 1;"
            )
            result = connection.execute(
                year_ago_query, {"year_ago_datetime": year_ago_datetime}
            ).fetchone()
            year_ago_balance = result[0] if result else None

            # Month Ago Balance
            month_ago_query = text(
                "SELECT balance FROM betting_account WHERE datetime < :month_ago_datetime ORDER BY datetime DESC LIMIT 1;"
            )
            result = connection.execute(
                month_ago_query, {"month_ago_datetime": month_ago_datetime}
            ).fetchone()
            month_ago_balance = result[0] if result else None

            # Week Ago Balance
            week_ago_query = text(
                "SELECT balance FROM betting_account WHERE datetime < :week_ago_datetime ORDER BY datetime DESC LIMIT 1;"
            )
            result = connection.execute(
                week_ago_query, {"week_ago_datetime": week_ago_datetime}
            ).fetchone()
            week_ago_balance = result[0] if result else None

            # Yesterday's Win/Loss
            yest_win_loss_query = text("""SELECT COALESCE(SUM(bets.bet_profit_loss), 0)
                    FROM games
                    LEFT JOIN bets ON games.game_id = bets.game_id
                    WHERE DATE(games.game_datetime) = :yesterdays_datetime;""")
            result = connection.execute(
                yest_win_loss_query, {"yesterdays_datetime": yesterdays_datetime}
            ).fetchone()
            yest_win_loss = result[0] if result else 0

            # Active Bets (case-insensitive using LOWER)
            active_bets_query = text(
                "SELECT COUNT(*) FROM bets WHERE LOWER(bet_status) = 'active';"
            )
            result = connection.execute(active_bets_query).fetchone()
            active_bets_count = result[0] if result else 0

            # Money in Play
            money_in_play_query = text(
                "SELECT COALESCE(SUM(bet_amount), 0) FROM bets WHERE LOWER(bet_status) = 'active';"
            )
            result = connection.execute(money_in_play_query).fetchone()
            money_in_play = result[0] if result else 0

            # Game Table with Latest Predictions and Current Lines
            game_table_query = text("""
                WITH LatestPredictions AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY predicted_at DESC) AS rn
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
                    games.game_completed,
                    LatestPredictions.prediction_spread,
                    LatestPredictions.confidence,
                    LatestPredictions.pick,
                    bets.bet_status,
                    bets.bet_amount,
                    bets.bet_line,
                    bets.bet_direction,
                    bets.bet_price,
                    bets.bet_location,
                    bets.bet_profit_loss,
                    bets.bet_datetime
                FROM games
                LEFT JOIN bets ON games.game_id = bets.game_id
                LEFT JOIN LatestPredictions ON games.game_id = LatestPredictions.game_id
                    AND (LatestPredictions.rn = 1 OR LatestPredictions.rn IS NULL)
                WHERE games.game_datetime < :day_after_tomorrow
                AND games.game_datetime >= :start_date
                AND (
                    games.game_completed = 1
                    OR games.game_datetime >= :today
                )
                ORDER BY games.game_datetime DESC, LatestPredictions.confidence DESC
                LIMIT 100;
            """)

            start_date = "2022-09-01"
            game_table = connection.execute(
                game_table_query,
                {
                    "day_after_tomorrow": day_after_tomorrow,
                    "start_date": start_date,
                    "today": today,
                },
            ).fetchall()

        # Format data
        current_balance_rounded = round(current_balance) if current_balance else 0
        starting_balance = STARTING_BALANCE

        alltime_diff, alltime_pct_diff = calculate_diff(current_balance, starting_balance)
        year_diff, year_pct_diff = calculate_diff(current_balance, year_ago_balance)
        month_diff, month_pct_diff = calculate_diff(current_balance, month_ago_balance)
        week_diff, week_pct_diff = calculate_diff(current_balance, week_ago_balance)

        yest_win_loss = round(yest_win_loss)
        money_in_play = round(money_in_play)

        records_df = pd.DataFrame(game_table)

        # Handle empty game table gracefully
        if records_df.empty:
            output_records = []
        else:
            records_df["game_datetime"] = pd.to_datetime(
                records_df["game_datetime"], format="mixed"
            )

            # Result: home score - away score (positive = home won by X)
            # Only show result when game_completed is True (1) to avoid showing in-progress scores
            records_df["result"] = records_df.apply(
                lambda row: (
                    int(row["home_score"] - row["away_score"])
                    if row.get("game_completed") == 1
                    and pd.notnull(row["home_score"])
                    and pd.notnull(row["away_score"])
                    else "-"
                ),
                axis=1,
            )

            # Spread: use open_line (opening spread from Covers/Odds API)
            # Standard betting notation: negative = home favored (e.g., -4 means home favored by 4)
            # Note: We intentionally avoid current_line as it may contain in-game lines
            records_df["spread"] = records_df["open_line"].apply(
                lambda x: round(x, 1) if pd.notnull(x) else "-"
            )

            # Format datetime for display
            # Show time only if it's not midnight (00:00 means time wasn't captured)
            records_df["game_datetime"] = records_df["game_datetime"].apply(
                lambda x: (
                    x.strftime("%Y-%m-%d %H:%M")
                    if x.hour != 0 or x.minute != 0
                    else x.strftime("%Y-%m-%d")
                )
            )

            # Confidence: the model's confidence score (0-100) - already named correctly from query
            records_df["confidence"] = records_df["confidence"].apply(
                lambda x: round(x) if pd.notnull(x) else "-"
            )

            # Pick: already named correctly from query
            records_df["pick"] = records_df["pick"].apply(replace_nan_with_hyphen)

            records_df["bet_price"] = records_df["bet_price"].apply(
                lambda x: "-" if pd.isnull(x) else int(x)
            )

            # Currency Formatting
            currency_cols = ["bet_amount", "bet_profit_loss"]
            records_df[currency_cols] = records_df[currency_cols].apply(
                lambda col: col.map(format_currency)
            )

            # NaN Formatting
            normal_nan_cols = [
                "bet_status",
                "bet_line",
                "bet_direction",
                "bet_location",
                "bet_datetime",
            ]
            records_df[normal_nan_cols] = records_df[normal_nan_cols].apply(
                lambda col: col.map(replace_nan_with_hyphen)
            )

            output_records = records_df.to_dict("records")

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

    @app.route("/", methods=["POST", "GET"])
    def home_table():
        """Home page with game table and betting form."""
        data = nba_data_inbound()

        if request.method == "POST":
            # NOTE: If this app is exposed publicly, consider adding CSRF protection
            # via Flask-WTF or manual token validation.

            # Validate required form fields (balance fetched from DB, not form)
            required_fields = [
                "bet_game_id",
                "bet_status",
                "bet_amount",
                "bet_line",
                "bet_direction",
                "bet_price",
                "bet_location",
                "bet_profitloss",
            ]
            missing_fields = [f for f in required_fields if f not in request.form]
            if missing_fields:
                return f"Missing required fields: {', '.join(missing_fields)}", 400

            bet_datetime = get_current_time().strftime("%Y-%m-%d %H:%M:%S")

            # Validate and parse form data with error handling
            try:
                bet_game_id = request.form["bet_game_id"]
                if not bet_game_id or len(bet_game_id) > 100:
                    return "Invalid game ID", 400

                bet_status = request.form["bet_status"].lower()
                if bet_status not in ("active", "win", "loss", "push"):
                    return "Invalid bet status", 400

                bet_amount = float(request.form["bet_amount"])
                if bet_amount <= 0 or bet_amount > 100000:
                    return "Invalid bet amount (must be between 0 and 100000)", 400

                bet_line = float(request.form["bet_line"])
                if abs(bet_line) > 50:
                    return "Invalid bet line (must be between -50 and 50)", 400

                bet_direction = request.form["bet_direction"].lower()
                if bet_direction not in ("home", "away"):
                    return "Invalid bet direction", 400

                bet_price = int(request.form["bet_price"])
                if abs(bet_price) > 1000:
                    return "Invalid bet price", 400

                bet_location = request.form["bet_location"]
                if len(bet_location) > 100:
                    return "Invalid bet location", 400

                bet_profit_loss = float(request.form["bet_profitloss"])
            except (ValueError, TypeError) as e:
                return f"Invalid form data: {e}", 400

            # SECURITY: Fetch balance and old bet data from database, NOT from form
            # This prevents client-side manipulation of financial data
            with get_db_connection() as conn:
                # Get current account balance from database
                balance_query = text(
                    "SELECT balance FROM betting_account ORDER BY datetime DESC LIMIT 1;"
                )
                balance_result = conn.execute(balance_query).fetchone()
                current_account_balance = balance_result[0] if balance_result else STARTING_BALANCE

                # Get old profit/loss for this bet from database (if bet exists)
                old_bet_query = text("SELECT bet_profit_loss FROM bets WHERE game_id = :game_id;")
                old_bet_result = conn.execute(old_bet_query, {"game_id": bet_game_id}).fetchone()
                old_profit_loss = old_bet_result[0] if old_bet_result and old_bet_result[0] else 0.0

            diff_bet_profit_loss = bet_profit_loss - old_profit_loss
            new_account_balance = current_account_balance + diff_bet_profit_loss

            upsert_stmt = """
                INSERT OR REPLACE INTO bets (game_id, bet_datetime, bet_status, bet_amount, bet_price, bet_location, bet_line, bet_profit_loss, bet_direction)
                VALUES (:bet_game_id, :bet_datetime, :bet_status, :bet_amount, :bet_price, :bet_location, :bet_line, :bet_profit_loss, :bet_direction);
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
                connection.commit()

            data = nba_data_inbound()
            return render_template("nba_home.html", **data)

        return render_template("nba_home.html", **data)

    @app.route("/dashboard")
    def dashboard():
        """Redirect to the Dash analytics dashboard."""
        return redirect("/nba_dashboard/")

    @app.context_processor
    def inject_app():
        return dict(app=app)

    @app.route("/home_page_plot.png")
    def plot_png():
        """Generate and serve the balance history plot."""
        fig = create_figure()
        output = io.BytesIO()
        FigureCanvas(fig).print_png(output)
        return Response(output.getvalue(), mimetype="image/png")

    def create_figure():
        """Create the betting account balance chart."""
        # Get the last balance entry per day
        figure_data_query = text("""SELECT datetime, balance
               FROM (SELECT datetime, balance,
                       row_number() OVER (PARTITION BY date(datetime) ORDER BY datetime DESC) r
                     FROM betting_account) T
               WHERE T.r = 1
               ORDER BY datetime;""")
        with get_db_connection() as connection:
            figure_records = connection.execute(figure_data_query).fetchall()

        style.use("seaborn-v0_8-whitegrid")
        fig = Figure(dpi=150, tight_layout=True)
        fig.set_size_inches(16, 5)
        ax = fig.subplots()

        # Handle empty data gracefully
        if not figure_records:
            ax.text(
                0.5,
                0.5,
                "No balance history yet",
                ha="center",
                va="center",
                fontsize=16,
                transform=ax.transAxes,
            )
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            return fig

        figure_df = pd.DataFrame(figure_records)
        figure_df["datetime"] = pd.to_datetime(figure_df["datetime"])
        x = figure_df["datetime"]
        y = figure_df["balance"]

        # Draw starting balance reference line
        starting_balance = STARTING_BALANCE
        ax.axhline(
            y=starting_balance,
            color="#dc3545",
            linestyle="--",
            linewidth=3,
            alpha=0.6,
            label=f"Starting (${starting_balance:,.0f})",
        )

        # Plot balance line with markers for visibility with few data points
        ax.plot(x, y, color="#0d6efd", linewidth=2, marker="o", markersize=12, label="Balance")

        # Format axes with bold fonts
        ax.tick_params(axis="y", labelsize=20, pad=6)
        ax.yaxis.set_major_formatter("${x:1.0f}")
        ax.tick_params(axis="x", labelsize=20, pad=6)
        for label in ax.get_xticklabels() + ax.get_yticklabels():
            label.set_fontweight("bold")

        # Set x-axis range with padding so first/last points aren't cut off
        if len(figure_df) == 1:
            # Single data point - normalize to start of day, show a week ahead
            single_date = x.iloc[0].normalize()  # Start of day (midnight)
            ax.set_xlim(single_date - pd.Timedelta(hours=6), single_date + pd.Timedelta(days=7))
            ax.xaxis.set_major_locator(mdates.DayLocator())
            # Replot with normalized date so dot sits on the tick
            ax.lines[-1].set_xdata([single_date])
        else:
            # Multiple points - add small padding on both ends
            date_range = x.max() - x.min()
            padding = max(pd.Timedelta(hours=12), date_range * 0.02)
            ax.set_xlim(x.min() - padding, x.max() + padding)
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))

        # Add legend with bold text
        legend = ax.legend(loc="upper left", fontsize=18)
        for legend_text in legend.get_texts():
            legend_text.set_fontweight("bold")

        return fig

    @app.after_request
    def add_header(r):
        """Disable caching for all responses."""
        r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        r.headers["Pragma"] = "no-cache"
        r.headers["Expires"] = "0"
        return r

    # Initialize Dash dashboard inside app factory
    from src.app.dashboard import init_dashboard

    init_dashboard(app)

    return app


app = create_app()

if __name__ == "__main__":
    # Use start_app.py for production; this is for quick testing only
    import os

    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(debug=debug)
