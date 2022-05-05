import pandas as pd
import flask
from sqlalchemy import create_engine


app = flask.Flask(__name__)


def nba_data_inbound():
    # SQL Queries
    records_query = """
                    SELECT game_id,
                        date,
                        time,
                        home,
                        away,
                        home_line,
                        home_line_price,
                        away_line,
                        away_line_price,
                        game_score,
                        rec_bet_direction,
                        rec_bet_amount,
                        predicted_win_loss,
                        game_result,
                        bet_amount,
                        bet_direction,
                        bet_price,
                        bet_location,
                        bet_win_loss,
                        game_info
                        FROM game_records
                        ORDER BY game_id DESC LIMIT 50
                    """
    # Database Calls
    with engine.connect() as connection:
        records = connection.execute(records_query).fetchall()
    # Python/Data Formatting
    records_df = pd.DataFrame(records)
    records_df[2] = records_df[2].apply(lambda x: x.strftime("%H:%M"))
    records_df[9] = records_df[9].apply(lambda x: round(x, 2))
    records_df[12] = records_df[12].apply(lambda x: f"${round(x)}")
    records_df[14] = records_df[14].apply(lambda x: "-" if x is None else x)
    records_df[15] = records_df[15].apply(lambda x: "-" if x is None else x)
    records_df[16] = records_df[16].apply(lambda x: "-" if x is None else x)
    records_df[17] = records_df[17].apply(lambda x: "-" if x is None else x)
    records_df[18] = records_df[18].apply(lambda x: "-" if x is None else x)
    # Return Data
    output_records = list(records_df.to_records(index=False))
    return {"records": output_records}


def nba_dashboard_data_inbound():
    # SQL Queries

    # Database Calls

    # Python/Data Formatting

    # Return Data

    pass


@app.route("/")
def home_table():
    data = nba_data_inbound()
    return flask.render_template("nba_home.html", **data)


@app.route("/nba_dashboard")
def dashboard():
    # data = nba_dashboard_data_inbound()
    return flask.render_template("nba_dashboard.html")


@app.route("/nba_about")
def about():
    return flask.render_template("nba_about.html")


if __name__ == "__main__":
    username = "postgres"
    password = None
    endpoint = None
    database = "nba_betting"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    )

    app.debug = True
    app.run(host="0.0.0.0", port=8000)