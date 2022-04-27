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
                        away
                        FROM game_records
                        ORDER BY game_id DESC LIMIT 50
                    """
    # Database Calls
    with engine.connect() as connection:
        records = connection.execute(records_query).fetchall()
    # Python/Data Formatting

    # Return Data
    return {"records": records}


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
    password = ""
    endpoint = ""
    database = "nba_betting"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    )

    app.debug = True
    app.run(host="0.0.0.0", port=8000)