import datetime
import os

import dash_bootstrap_components as dbc
import flask
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pytz
from dash import Dash, Input, Output, dcc
from dotenv import load_dotenv
from flask import redirect, url_for
from flask_login import current_user
from sqlalchemy import create_engine

load_dotenv()
DB_ENDPOINT = os.getenv("DB_ENDPOINT")
DB_PASSWORD = os.getenv("DB_PASSWORD")


# ----- Dashboard Creation -----


def init_private_dashboard(server):
    private_app = Dash(
        __name__,
        server=server,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        meta_tags=[
            {"name": "viewport", "content": "width=device-width, initial-scale=1"}
        ],
        url_base_pathname="/private_nba_dashboard/",
    )

    private_app.config.suppress_callback_exceptions = True

    # Loading Data

    engine = create_engine(
        f"postgresql+psycopg2://postgres:{DB_PASSWORD}@{DB_ENDPOINT}/nba_betting"
    )
    with engine.connect() as connection:
        df = get_dashboard_data(connection)

    profit_loss_chart, bet_win_loss_chart, spread_error_chart = create_original_charts(
        df
    )

    today = datetime.datetime.now(pytz.timezone("America/Denver"))

    # Dashboard Layout

    private_app.layout = dbc.Container(
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        dbc.RadioItems(
                            options=[
                                {"label": "Actual Bets", "value": "Actual Bets"},
                                {"label": "Simulated Bets", "value": "Simulated Bets"},
                            ],
                            value="Simulated Bets",
                            inline=True,
                            id="actual_simulated_bets",
                            style={"font-size": 24},
                        ),
                        width=6,
                        style={
                            "display": "flex",
                            "justify-content": "center",
                            "align-items": "center",
                        },
                    ),
                    dbc.Col(
                        dbc.Form(
                            [
                                dbc.Label(
                                    "Date Range:  ",
                                    html_for="date_range",
                                    style={
                                        "font-size": 24,
                                        "margin-right": "10px",
                                        "margin-bottom": "0px",
                                    },
                                ),
                                dcc.DatePickerRange(
                                    min_date_allowed=datetime.date(2014, 10, 1),
                                    max_date_allowed=today,
                                    initial_visible_month=today,
                                    start_date=datetime.date(2022, 10, 1),
                                    end_date=today,
                                    minimum_nights=7,
                                    day_size=50,
                                    id="date_range",
                                    style={"font-weight": 500},
                                ),
                            ],
                            style={
                                "display": "flex",
                                "justify-content": "center",
                                "align-items": "center",
                            },
                        ),
                        width=6,
                        style={
                            "display": "flex",
                            "justify-content": "center",
                            "align-items": "center",
                        },
                    ),
                ],
                align="center",
                justify="center",
                style={
                    "border-top": "2px solid #0000001A",
                    "border-bottom": "2px solid #0000001A",
                    "padding-top": "2px",
                    "padding-bottom": "2px",
                },
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            id="Cumulative_Profit_Loss",
                            figure=profit_loss_chart,
                            # responsive=True,
                            style={"height": "46vh", "width": "100%"},
                        )
                    ),
                ],
                justify="center",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            id="Bet_Win_Loss",
                            figure=bet_win_loss_chart,
                            # responsive=True,
                            style={"height": "46vh", "width": "100%"},
                        )
                    ),
                    dbc.Col(
                        dcc.Graph(
                            id="Average_Spread_Error",
                            figure=spread_error_chart,
                            # responsive=True,
                            style={"height": "46vh", "width": "100%"},
                        )
                    ),
                ],
                justify="center",
            ),
        ],
        fluid=True,
    )

    # Callbacks

    @private_app.callback(
        [
            Output("Cumulative_Profit_Loss", "figure"),
            Output("Bet_Win_Loss", "figure"),
            Output("Average_Spread_Error", "figure"),
        ],
        [
            Input("actual_simulated_bets", "value"),
            Input("date_range", "start_date"),
            Input("date_range", "end_date"),
        ],
    )
    def update_output(bet_category, start_date, end_date):
        # Data Set Updating
        new_df = df.copy()
        new_date_mask = (
            new_df["game_date"].apply(
                lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver"))
            )
            >= pd.Timestamp(start_date, tz=pytz.timezone("America/Denver"))
        ) & (
            new_df["game_date"].apply(
                lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver"))
            )
            <= pd.Timestamp(end_date, tz=pytz.timezone("America/Denver"))
        )
        new_df = new_df[new_date_mask]
        new_rec_bet_df = new_df[new_df["directional_game_rating"] >= 90]

        # Cumulative Profit Loss Over Time Line Chart
        new_profit_loss_chart = update_profit_loss_chart(
            new_df, new_rec_bet_df, bet_category
        )

        # Bet Win Loss Bar Chart
        new_bet_win_loss_chart = update_win_loss_chart(
            new_df, new_rec_bet_df, bet_category
        )

        # Spread Error Bar Chart
        new_spread_error_chart = update_spread_error_chart(df, start_date, end_date)

        return new_profit_loss_chart, new_bet_win_loss_chart, new_spread_error_chart

    @server.before_request
    def auth_check():
        if flask.request.path.startswith(private_app.config.url_base_pathname):
            if not current_user.is_authenticated:
                return redirect(url_for("login"))

    return private_app


# ----- Data Loading -----
def get_dashboard_data(connection):
    """
    Fetches and processes data for a dashboard.

    Args:
        connection: A database connection object.

    Returns:
        A pandas DataFrame containing the processed data.
    """
    query = """
        SELECT
            g.game_id,
            g.game_datetime,
            g.home_team,
            g.away_team,
            g.open_line,
            g.home_score,
            g.away_score,
            p.directional_game_rating,
            p.prediction_direction,
            p.ml_reg_pred_1,
            p.dl_reg_pred_1,
            b.bet_status,
            b.bet_amount,
            b.bet_line,
            b.bet_direction,
            b.bet_price,
            b.bet_location,
            b.bet_profit_loss,
            b.bet_datetime
        FROM
            games AS g
        FULL OUTER JOIN
            bets AS b ON g.game_id = b.game_id
        LEFT JOIN (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY prediction_datetime DESC) AS rn
            FROM predictions
        ) AS p ON p.game_id = g.game_id AND p.rn = 1
        WHERE g.game_datetime >= '2010-09-01'
        ORDER BY
            g.game_id DESC;

        """

    df = pd.read_sql(sql=query, con=connection)

    df["game_date"] = (
        df["game_datetime"]
        .apply(lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver")))
        .dt.date
    )
    df["game_result"] = df["home_score"] - df["away_score"]
    df["rec_bet_pl"] = df.apply(calc_rec_bet_profit_loss, axis=1)
    df["rec_bet_win_loss"] = df.apply(calc_rec_bet_win_loss, axis=1)
    df["vegas_error"] = abs(df["game_result"] - -df["open_line"])
    df["ml_error"] = abs(df["game_result"] - df["ml_reg_pred_1"])
    df["dl_error"] = abs(df["game_result"] - df["dl_reg_pred_1"])
    df["bet_profit_loss"] = df["bet_profit_loss"].fillna(0)

    return df


# ----- Chart Creation -----
def create_original_charts(df):
    today = datetime.datetime.now(pytz.timezone("America/Denver"))
    season_start = datetime.datetime(
        2022, 10, 1, tzinfo=pytz.timezone("America/Denver")
    )

    date_mask = (
        df["game_date"].apply(
            lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver"))
        )
        >= pd.to_datetime(season_start)
    ) & (
        df["game_date"].apply(
            lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver"))
        )
        <= pd.to_datetime(today)
    )

    df = df[date_mask]
    rec_bet_df = df[df["directional_game_rating"] >= 90]

    total_counts = df["rec_bet_win_loss"].value_counts()
    simulated_total_win_count = total_counts.get("Win", 0)
    simulated_total_loss_count = total_counts.get("Loss", 0)

    rec_bet_counts = rec_bet_df["rec_bet_win_loss"].value_counts()
    simulated_rec_bet_win_count = rec_bet_counts.get("Win", 0)
    simulated_rec_bet_loss_count = rec_bet_counts.get("Loss", 0)

    profit_loss_chart = create_profit_loss_chart(df, rec_bet_df)
    bet_win_loss_chart = create_win_loss_chart(
        simulated_total_win_count,
        simulated_total_loss_count,
        simulated_rec_bet_win_count,
        simulated_rec_bet_loss_count,
    )
    spread_error_chart = create_spread_error_chart(df)

    return profit_loss_chart, bet_win_loss_chart, spread_error_chart


def create_profit_loss_chart(df, rec_bet_df):
    all_bet_data = (
        df.sort_values("game_id")
        .groupby("game_date")
        .sum(numeric_only=True)["rec_bet_pl"]
        .cumsum()
        .reset_index()
    )
    rec_bet_data = (
        rec_bet_df.sort_values("game_id")
        .groupby("game_date")
        .sum(numeric_only=True)["rec_bet_pl"]
        .cumsum()
        .reset_index()
    )
    profit_loss_chart = px.line(
        all_bet_data,
        x="game_date",
        y="rec_bet_pl",
        title="Bet Cumulative Profit/Loss",
        color_discrete_sequence=["#17408B"],
    )
    profit_loss_rec_bets = px.line(
        rec_bet_data,
        x="game_date",
        y="rec_bet_pl",
        title="Bet Cumulative Profit/Loss",
        color_discrete_sequence=["#B01E23"],
    )
    profit_loss_chart.add_trace(profit_loss_rec_bets.data[0])

    profit_loss_chart.update_layout(
        autosize=True,
        hovermode="x unified",
        showlegend=False,
        plot_bgcolor="rgba(0, 0, 0, 0.1)",
        font={"size": 20, "family": "Arial", "color": "#000000"},
        title={"font": {"size": 28}},
    )
    profit_loss_chart.update_yaxes(
        title="",
        rangemode="tozero",
        visible=True,
        showticklabels=True,
        tickfont={"size": 14},
    )
    profit_loss_chart.update_xaxes(title="", visible=True, showticklabels=True)
    profit_loss_chart.update_traces(mode="markers+lines")

    profit_loss_chart["data"][0].update(
        hovertemplate="All Games Profit/Loss: $ %{y} <extra></extra>",
        mode="markers+lines",
    )
    if len(profit_loss_chart["data"]) > 1:
        profit_loss_chart["data"][1].update(
            hovertemplate="Best Bets Profit/Loss: $ %{y} <extra></extra>",
            mode="markers+lines",
        )

    return profit_loss_chart


def create_win_loss_chart(
    simulated_total_win_count,
    simulated_total_loss_count,
    simulated_rec_bet_win_count,
    simulated_rec_bet_loss_count,
):
    bet_win_loss_chart = go.Figure(
        data=[
            go.Bar(
                x=["Wins", "Losses"],
                y=[
                    simulated_rec_bet_win_count,
                    simulated_rec_bet_loss_count,
                ],
                textposition="auto",
                texttemplate="%{y}",
                marker_color=["#17408B", "#C9082A"],
                name="Best Bets",
            ),
            go.Bar(
                x=["Wins", "Losses"],
                y=[simulated_total_win_count, simulated_total_loss_count],
                textposition="auto",
                texttemplate="%{y}",
                marker_color=["#4566A2", "#D43955"],
                name="All Bets",
            ),
        ]
    )
    bet_win_loss_chart.update_layout(barmode="stack")

    bet_win_loss_chart.update_layout(
        autosize=True,
        title_text="Bet Win/Loss",
        showlegend=False,
        plot_bgcolor="rgba(0, 0, 0, 0.1)",
        font={"size": 20, "family": "Arial", "color": "#000000"},
        title={"font": {"size": 28}},
    )
    bet_win_loss_chart.update_yaxes(
        title="", visible=True, showticklabels=True, tickfont={"size": 14}
    )
    bet_win_loss_chart.update_xaxes(title="", visible=True, showticklabels=True)

    return bet_win_loss_chart


def create_spread_error_chart(df):
    spread_error_x = ["Vegas", "ML Predictions", "DL Predictions"]
    spread_error_y = [
        df["vegas_error"].mean(),
        df["ml_error"].mean(),
        df["dl_error"].mean(),
    ]

    spread_error_chart = go.Figure(
        data=[
            go.Bar(
                x=spread_error_x,
                y=spread_error_y,
                textposition="auto",
                texttemplate="%{y:.2f}",
                marker_color=["#FCBF49", "#44AF69", "#759FBC"],
            )
        ]
    )
    spread_error_chart.update_layout(
        title_text="Average Spread Error (Points Per Game)",
        showlegend=False,
        plot_bgcolor="rgba(0, 0, 0, 0.1)",
        font={"size": 20, "family": "Arial", "color": "#000000"},
        title={"font": {"size": 28}},
    )
    spread_error_chart.update_yaxes(
        title="", visible=True, showticklabels=True, tickfont={"size": 14}
    )
    spread_error_chart.update_xaxes(title="", visible=True, showticklabels=True)

    return spread_error_chart


# ----- Chart Updating -----
def update_spread_error_chart(full_df, start_date, end_date):
    """
    Generate a bar chart of the average spread error for three different prediction methods: Vegas, ML predictions, and DL predictions.

    Parameters:
    full_df (pandas.DataFrame): The full dataframe containing the spread error data.
    start_date (str): A string representing the start date of the date range to filter for, in 'YYYY-MM-DD' format.
    end_date (str): A string representing the end date of the date range to filter for, in 'YYYY-MM-DD' format.

    Returns:
    plotly.graph_objs.Figure: A bar chart of the average spread error for the specified date range.
    """
    # Create date mask to filter full_df for specified date range
    date_mask = (
        full_df["game_date"].apply(
            lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver"))
        )
        >= pd.Timestamp(start_date, tz=pytz.timezone("America/Denver"))
    ) & (
        full_df["game_date"].apply(
            lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver"))
        )
        <= pd.Timestamp(end_date, tz=pytz.timezone("America/Denver"))
    )

    # Filter full_df for specified date range
    spread_error_df = full_df[date_mask]

    # Compute mean spread errors for each prediction method
    spread_error_means = [
        spread_error_df["vegas_error"].mean(),
        spread_error_df["ml_error"].mean(),
        spread_error_df["dl_error"].mean(),
    ]

    # Create bar chart using plotly.graph_objs
    fig = go.Figure(
        data=[
            go.Bar(
                x=["Vegas", "ML Predictions", "DL Predictions"],
                y=spread_error_means,
                textposition="auto",
                texttemplate="%{y:.2f}",
                marker_color=["#FCBF49", "#44AF69", "#759FBC"],
            )
        ]
    )

    # Customize chart layout
    fig.update_layout(
        autosize=True,
        title_text="Average Spread Error (Points Per Game)",
        showlegend=False,
        plot_bgcolor="rgba(0, 0, 0, 0.1)",
        font={"size": 20, "family": "Arial", "color": "#000000"},
        title={"font": {"size": 28}},
    )
    fig.update_yaxes(title="", visible=True, showticklabels=True, tickfont={"size": 14})
    fig.update_xaxes(title="", visible=True, showticklabels=True)

    return fig


def update_win_loss_chart(new_df, new_rec_bet_df, bet_category):
    """Create a bar chart of win/loss information based on the bet category.

    Args:
        new_df (pandas.DataFrame): A DataFrame containing data on the actual or simulated bets.
        new_rec_bet_df (pandas.DataFrame): A DataFrame containing data on the recommended bets.
        bet_category (str): A string indicating the type of bets to display in the chart. Must be either "Actual Bets" or "Simulated Bets".

    Returns:
        plotly.graph_objects.Figure: A bar chart of the win/loss information.

    Raises:
        ValueError: If the bet_category parameter is not set to either "Actual Bets" or "Simulated Bets".
    """

    # Check if the bet_category parameter is valid
    if bet_category not in ["Actual Bets", "Simulated Bets"]:
        raise ValueError(
            "bet_category must be either 'Actual Bets' or 'Simulated Bets'"
        )

    new_bet_win_loss_chart = None
    if bet_category == "Actual Bets":
        # Calculate win and loss counts for actual bets
        new_actual_win_count = new_df[new_df["bet_status"] == "Win"][
            "bet_status"
        ].count()
        new_actual_loss_count = new_df[new_df["bet_status"] == "Loss"][
            "bet_status"
        ].count()

        # Create a bar chart of the win/loss information for actual bets
        new_bet_win_loss_chart = go.Figure(
            data=[
                go.Bar(
                    x=["Wins", "Losses"],
                    y=[new_actual_win_count, new_actual_loss_count],
                    textposition="auto",
                    texttemplate="%{y}",
                    marker_color=["#17408B", "#C9082A"],
                    name="Actual Bets",
                )
            ]
        )
    elif bet_category == "Simulated Bets":
        # Calculate win and loss counts for simulated bets and recommended bets
        new_simulated_total_win_count = new_df[new_df["rec_bet_win_loss"] == "Win"][
            "rec_bet_win_loss"
        ].count()
        new_simulated_total_loss_count = new_df[new_df["rec_bet_win_loss"] == "Loss"][
            "rec_bet_win_loss"
        ].count()
        new_simulated_rec_bet_win_count = new_rec_bet_df[
            new_rec_bet_df["rec_bet_win_loss"] == "Win"
        ]["rec_bet_win_loss"].count()
        new_simulated_rec_bet_loss_count = new_rec_bet_df[
            new_rec_bet_df["rec_bet_win_loss"] == "Loss"
        ]["rec_bet_win_loss"].count()

        # Create a stacked bar chart of the win/loss information for simulated bets and recommended bets
        new_bet_win_loss_chart = go.Figure(
            data=[
                go.Bar(
                    x=["Wins", "Losses"],
                    y=[
                        new_simulated_rec_bet_win_count,
                        new_simulated_rec_bet_loss_count,
                    ],
                    textposition="auto",
                    texttemplate="%{y}",
                    marker_color=["#17408B", "#C9082A"],
                    name="Best Bets",
                ),
                go.Bar(
                    x=["Wins", "Losses"],
                    y=[
                        new_simulated_total_win_count,
                        new_simulated_total_loss_count,
                    ],
                    textposition="auto",
                    texttemplate="%{y}",
                    marker_color=["#4566A2", "#D43955"],
                    name="All Bets",
                ),
            ]
        )
        new_bet_win_loss_chart.update_layout(barmode="stack")

    new_bet_win_loss_chart.update_layout(
        autosize=True,
        title_text="Bet Win/Loss",
        showlegend=False,
        plot_bgcolor="rgba(0, 0, 0, 0.1)",
        font={"size": 20, "family": "Arial", "color": "#000000"},
        title={"font": {"size": 28}},
    )
    new_bet_win_loss_chart.update_yaxes(
        title="", visible=True, showticklabels=True, tickfont={"size": 14}
    )
    new_bet_win_loss_chart.update_xaxes(title="", visible=True, showticklabels=True)

    return new_bet_win_loss_chart


def update_profit_loss_chart(new_df, new_rec_bet_df, bet_category):
    """
    Generate a profit/loss chart over time based on the specified bet category.

    Parameters:
    new_df (pandas.DataFrame): A DataFrame containing data on the actual or simulated bets.
    new_rec_bet_df (pandas.DataFrame): A DataFrame containing data on the recommended bets.
    bet_category (str): A string indicating the type of bets to display in the chart. Must be either "Actual Bets" or "Simulated Bets".

    Returns:
    A Plotly figure object containing the profit/loss chart.

    Raises:
    ValueError: If the bet_category parameter is not "Actual Bets" or "Simulated Bets".
    """
    # Check if the bet_category parameter is valid
    if bet_category not in ["Actual Bets", "Simulated Bets"]:
        raise ValueError(
            "bet_category must be either 'Actual Bets' or 'Simulated Bets'"
        )

    new_profit_loss = None
    if bet_category == "Actual Bets":
        new_actual_bet_data = (
            new_df.sort_values("game_id")
            .groupby("game_date")
            .sum(numeric_only=True)["bet_profit_loss"]
            .cumsum()
            .reset_index()
        )
        new_profit_loss = px.line(
            new_actual_bet_data,
            x="game_date",
            y="bet_profit_loss",
            title="Profit/Loss Over Time",
            color_discrete_sequence=["#17408B"],
        )
    elif bet_category == "Simulated Bets":
        new_all_bet_data = (
            new_df.sort_values("game_id")
            .groupby("game_date")
            .sum(numeric_only=True)["rec_bet_pl"]
            .cumsum()
            .reset_index()
        )
        new_rec_bet_data = (
            new_rec_bet_df.sort_values("game_id")
            .groupby("game_date")
            .sum(numeric_only=True)["rec_bet_pl"]
            .cumsum()
            .reset_index()
        )
        new_profit_loss = px.line(
            new_all_bet_data,
            x="game_date",
            y="rec_bet_pl",
            title="Bet Cumulative Profit/Loss",
            color_discrete_sequence=["#17408B"],
        )
        new_profit_loss_rec_bets = px.line(
            new_rec_bet_data,
            x="game_date",
            y="rec_bet_pl",
            title="Bet Cumulative Profit/Loss",
            color_discrete_sequence=["#B01E23"],
        )
        new_profit_loss.add_trace(new_profit_loss_rec_bets.data[0])

    new_profit_loss.update_layout(
        autosize=True,
        hovermode="x unified",
        showlegend=False,
        plot_bgcolor="rgba(0, 0, 0, 0.1)",
        font={"size": 20, "family": "Arial", "color": "#000000"},
        title={"font": {"size": 28}},
    )
    new_profit_loss.update_yaxes(
        title="",
        rangemode="tozero",
        visible=True,
        showticklabels=True,
        tickfont={"size": 14},
    )
    new_profit_loss.update_xaxes(title="", visible=True, showticklabels=True)

    new_profit_loss["data"][0].update(
        hovertemplate="All Games Profit/Loss: $ %{y} <extra></extra>",
        mode="markers+lines",
    )
    if len(new_profit_loss["data"]) > 1:
        new_profit_loss["data"][1].update(
            hovertemplate="Best Bets Profit/Loss: $ %{y} <extra></extra>",
            mode="markers+lines",
        )

    return new_profit_loss


# ----- Helper Functions -----
def calc_rec_bet_profit_loss(x):
    if x["prediction_direction"] == "Home":
        pl = 91 if x["game_result"] > -x["open_line"] else -100
    elif x["prediction_direction"] == "Away":
        pl = 91 if x["game_result"] < -x["open_line"] else -100
    else:
        pl = 0
    return pl


def calc_rec_bet_win_loss(x):
    if x["prediction_direction"] == "Home":
        win_loss = "Win" if x["game_result"] > -x["open_line"] else "Loss"
    elif x["prediction_direction"] == "Away":
        win_loss = "Win" if x["game_result"] < -x["open_line"] else "Loss"
    else:
        win_loss = "No Bet"
    return win_loss


if __name__ == "__main__":
    pass
