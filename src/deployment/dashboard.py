import datetime
import sys

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pytz
from dash import Dash, Input, Output, dcc, html
from sqlalchemy import create_engine

sys.path.append("../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"},
    ],
)

username = "postgres"
password = RDS_PASSWORD
endpoint = RDS_ENDPOINT
database = "nba_betting"
port = "5432"

connection = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
).connect()

query = """
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
                        ml_reg_prediction,
                        dl_reg_prediction
                        FROM game_records
                        FULL OUTER JOIN bets
                        ON game_records.game_id = bets.game_id
                        ORDER BY game_records.game_id DESC
        """

df = pd.read_sql(sql=query, con=connection)
df = df.loc[:, ~df.columns.duplicated()].copy()

# Data Preparation

today = datetime.datetime.now(pytz.timezone("America/Denver"))
week_ago = today - datetime.timedelta(days=7)
month_ago = today - datetime.timedelta(days=30)
year_ago = today - datetime.timedelta(days=365)


def calc_rec_bet_pl(x):
    if x["game_score_direction"] == "Home":
        pl = 91 if x["game_result"] > -x["home_line"] else -100
    elif x["game_score_direction"] == "Away":
        pl = 91 if x["game_result"] < -x["home_line"] else -100
    else:
        pl = 0
    return pl


def calc_rec_bet_win_loss(x):
    if x["game_score_direction"] == "Home":
        win_loss = "Win" if x["game_result"] > -x["home_line"] else "Loss"
    elif x["game_score_direction"] == "Away":
        win_loss = "Win" if x["game_result"] < -x["home_line"] else "Loss"
    else:
        win_loss = "No Bet"
    return win_loss


df["rec_bet_pl"] = df.apply(calc_rec_bet_pl, axis=1)
df["rec_bet_win_loss"] = df.apply(calc_rec_bet_win_loss, axis=1)
df["vegas_miss"] = abs(df["game_result"] - -df["home_line"])
df["ml_miss"] = abs(df["game_result"] - df["ml_reg_prediction"])
df["dl_miss"] = abs(df["game_result"] - df["dl_reg_prediction"])
df["bet_profit_loss"] = df["bet_profit_loss"].fillna(0)
df_2 = df.copy()

date_mask = (
    df["date"].apply(lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver")))
    >= pd.to_datetime(year_ago)
) & (
    df["date"].apply(lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver")))
    <= pd.to_datetime(today)
)

df = df[date_mask]
pl_data = (
    df.sort_values("game_id")
    .groupby("date")
    .sum(numeric_only=True)["bet_profit_loss"]
    .cumsum()
    .reset_index()
)

win_count = df[df["bet_outcome"] == "Win"]["bet_outcome"].count()
loss_count = df[df["bet_outcome"] == "Loss"]["bet_outcome"].count()

line_miss_x = ["Vegas Miss", "ML Pred Miss", "DL Pred Miss"]
line_miss_y = [df["vegas_miss"].mean(), df["ml_miss"].mean(), df["dl_miss"].mean()]

# Individual Graph Creation

cumulative_profit_loss = px.line(
    pl_data,
    x="date",
    y="bet_profit_loss",
    title="Profit/Loss Over Time",
    color_discrete_sequence=["#17408B"],
)
cumulative_profit_loss.update_layout(
    hovermode="x unified",
    showlegend=False,
    plot_bgcolor="rgba(0, 0, 0, 0.1)",
    font={"size": 20, "family": "Arial", "color": "#000000"},
    title={"font": {"size": 28}},
)
cumulative_profit_loss.update_yaxes(
    title="",
    rangemode="tozero",
    visible=True,
    showticklabels=True,
    tickfont={"size": 14},
)
cumulative_profit_loss.update_xaxes(title="", visible=True, showticklabels=True)
cumulative_profit_loss.update_traces(
    mode="markers+lines", hovertemplate="Cummulative Profit/Loss: $ %{y} <extra></extra>"
)

wl = go.Figure(
    data=[
        go.Bar(
            x=["Wins", "Losses"],
            y=[win_count, loss_count],
            textposition="auto",
            texttemplate="%{y}",
            marker_color=["#17408B", "#C9082A"],
        )
    ]
)
wl.update_layout(
    title_text="Bet Win/Loss",
    showlegend=False,
    plot_bgcolor="rgba(0, 0, 0, 0.1)",
    font={"size": 20, "family": "Arial", "color": "#000000"},
    title={"font": {"size": 28}},
)
wl.update_yaxes(title="", visible=True, showticklabels=True, tickfont={"size": 14})
wl.update_xaxes(title="", visible=True, showticklabels=True)

line_diff = go.Figure(
    data=[
        go.Bar(
            x=line_miss_x,
            y=line_miss_y,
            textposition="auto",
            texttemplate="%{y:.2f}",
            marker_color=["#696969", "#006400", "#008080"],
        )
    ]
)
line_diff.update_layout(
    title_text="Average Line Miss",
    showlegend=False,
    plot_bgcolor="rgba(0, 0, 0, 0.1)",
    font={"size": 20, "family": "Arial", "color": "#000000"},
    title={"font": {"size": 28}},
)
line_diff.update_yaxes(
    title="", visible=True, showticklabels=True, tickfont={"size": 14}
)
line_diff.update_xaxes(title="", visible=True, showticklabels=True)

# Dashboard Layout

app.layout = dbc.Container(
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
                    width=3,
                    style={"justify-content": "center", "display": "flex"},
                ),
                dbc.Col(
                    dcc.DatePickerRange(
                        min_date_allowed=datetime.date(2014, 10, 1),
                        max_date_allowed=today,
                        initial_visible_month=today,
                        start_date=year_ago,
                        end_date=today,
                        minimum_nights=7,
                        day_size=50,
                        id="date_range",
                        style={"font-size": 24},
                    ),
                    width=3,
                    style={"justify-content": "center", "display": "flex"},
                ),
                dbc.Col(
                    dbc.RadioItems(
                        options=[
                            {"label": "All Games", "value": "All Games"},
                            {"label": "Home", "value": "Home"},
                            {"label": "Away", "value": "Away"},
                        ],
                        value="All Games",
                        inline=True,
                        id="all_home_away",
                        style={"font-size": 24},
                    ),
                    width=3,
                    style={"justify-content": "center", "display": "flex"},
                ),
            ],
            align="center",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(id="Cumulative_Profit_Loss", figure=cumulative_profit_loss)
                ),
            ],
            justify="center",
        ),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="Bet_Win_Loss", figure=wl)),
                dbc.Col(dcc.Graph(id="Average_Line_Miss", figure=line_diff)),
            ]
        ),
    ],
    fluid=True,
)

# Callbacks


@app.callback(
    [
        Output("Cumulative_Profit_Loss", "figure"),
        Output("Bet_Win_Loss", "figure"),
        Output("Average_Line_Miss", "figure"),
    ],
    [
        Input("all_home_away", "value"),
        Input("actual_simulated_bets", "value"),
        Input("date_range", "start_date"),
        Input("date_range", "end_date"),
    ],
)
def update_output(home_away, bet_category, start_date, end_date):
    # Data Set Updating
    new_df = None
    if home_away == "All Games":
        new_df = df_2.copy()
    elif home_away == "Home":
        new_df = df_2[df_2["bet_direction"] == "Home"]
    elif home_away == "Away":
        new_df = df_2[df_2["bet_direction"] == "Away"]

    new_date_mask = (
        new_df["date"].apply(
            lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver"))
        )
        >= pd.Timestamp(start_date, tz=pytz.timezone("America/Denver"))
    ) & (
        new_df["date"].apply(
            lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver"))
        )
        <= pd.Timestamp(end_date, tz=pytz.timezone("America/Denver"))
    )
    new_df = new_df[new_date_mask]

    rec_bet_df = new_df[new_df["game_score"] >= 60]

    # Cumulative Profit Loss Over Time Line Chart
    new_pl_data = None
    new_profit_loss = None
    new_profit_loss_rec_bets = None
    if bet_category == "Actual Bets":
        new_pl_data = (
            new_df.sort_values("game_id")
            .groupby("date")
            .sum(numeric_only=True)["bet_profit_loss"]
            .cumsum()
            .reset_index()
        )
        new_profit_loss = px.line(
            new_pl_data,
            x="date",
            y="bet_profit_loss",
            title="Profit/Loss Over Time",
            color_discrete_sequence=["#17408B"],
        )
    elif bet_category == "Simulated Bets":
        new_all_bet_data = (
            new_df.sort_values("game_id")
            .groupby("date")
            .sum(numeric_only=True)["rec_bet_pl"]
            .cumsum()
            .reset_index()
        )
        new_profit_loss = px.line(
            new_all_bet_data,
            x="date",
            y="rec_bet_pl",
            title="Profit/Loss Over Time",
            color_discrete_sequence=["#17408B"],
        )
        new_rec_bet_data = (
            rec_bet_df.sort_values("game_id")
            .groupby("date")
            .sum(numeric_only=True)["rec_bet_pl"]
            .cumsum()
            .reset_index()
        )
        new_profit_loss_rec_bets = px.line(
            new_rec_bet_data,
            x="date",
            y="rec_bet_pl",
            title="Profit/Loss Over Time",
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
    new_profit_loss.update_traces(mode="markers+lines")
    if len(new_profit_loss.data) > 1:
        new_profit_loss.update_traces(
            hovertemplate="Recommended Games Profit/Loss: $ %{y} <extra></extra>",
            selector=dict(index=1),
        )
    new_profit_loss.update_traces(
        hovertemplate="All Games Profit/Loss: $ %{y} <extra></extra>",
        selector=dict(index=0),
    )

    # Bet Win Loss Bar Chart
    new_win_count = 0
    new_loss_count = 0
    if bet_category == "Actual":
        new_win_count = new_df[new_df["bet_outcome"] == "Win"]["bet_outcome"].count()
        new_loss_count = new_df[new_df["bet_outcome"] == "Loss"]["bet_outcome"].count()
    elif bet_category == "Simulated_Bets":
        new_win_count = new_df[new_df["rec_bet_win_loss"] == "Win"][
            "rec_bet_win_loss"
        ].count()
        new_loss_count = new_df[new_df["rec_bet_win_loss"] == "Loss"][
            "rec_bet_win_loss"
        ].count()

    new_wl = go.Figure(
        data=[
            go.Bar(
                x=["Wins", "Losses"],
                y=[new_win_count, new_loss_count],
                textposition="auto",
                texttemplate="%{y}",
                marker_color=["#17408B", "#C9082A"],
            )
        ]
    )
    new_wl.update_layout(
        autosize=True,
        title_text="Bet Win/Loss",
        showlegend=False,
        plot_bgcolor="rgba(0, 0, 0, 0.1)",
        font={"size": 20, "family": "Arial", "color": "#000000"},
        title={"font": {"size": 28}},
    )
    new_wl.update_yaxes(
        title="", visible=True, showticklabels=True, tickfont={"size": 14}
    )
    new_wl.update_xaxes(title="", visible=True, showticklabels=True)

    # Line Miss Bar Chart

    new_line_miss_date_mask = (
        df_2["date"].apply(lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver")))
        >= pd.Timestamp(start_date, tz=pytz.timezone("America/Denver"))
    ) & (
        df_2["date"].apply(lambda x: pd.Timestamp(x, tz=pytz.timezone("America/Denver")))
        <= pd.Timestamp(end_date, tz=pytz.timezone("America/Denver"))
    )
    line_miss_df = df_2[new_line_miss_date_mask]
    new_line_miss_x = ["Vegas Miss", "ML Pred Miss", "DL Pred Miss"]
    new_line_miss_y = [
        line_miss_df["vegas_miss"].mean(),
        line_miss_df["ml_miss"].mean(),
        line_miss_df["dl_miss"].mean(),
    ]

    new_line_diff = go.Figure(
        data=[
            go.Bar(
                x=new_line_miss_x,
                y=new_line_miss_y,
                textposition="auto",
                texttemplate="%{y:.2f}",
                marker_color=["#696969", "#006400", "#008080"],
            )
        ]
    )
    new_line_diff.update_layout(
        autosize=True,
        title_text="Average Line Miss",
        showlegend=False,
        plot_bgcolor="rgba(0, 0, 0, 0.1)",
        font={"size": 20, "family": "Arial", "color": "#000000"},
        title={"font": {"size": 28}},
    )
    new_line_diff.update_yaxes(
        title="", visible=True, showticklabels=True, tickfont={"size": 14}
    )
    new_line_diff.update_xaxes(title="", visible=True, showticklabels=True)

    return new_profit_loss, new_wl, new_line_diff


if __name__ == "__main__":
    app.run_server(debug=True)
