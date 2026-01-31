"""
NBA Betting Analytics Dashboard

A Dash application for visualizing betting performance, win/loss statistics,
and prediction accuracy.
"""

import datetime

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html

from src.database import get_engine
from src.utils.timezone import APP_TIMEZONE


# ----- Helper Functions -----
def filter_by_date_range(df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
    """
    Filter a DataFrame by date range using the game_date column.

    Args:
        df: DataFrame with a 'game_date' column
        start_date: Start date (string or datetime)
        end_date: End date (string or datetime)

    Returns:
        Filtered DataFrame
    """
    if df.empty:
        return df

    start_ts = pd.Timestamp(start_date, tz=APP_TIMEZONE)
    end_ts = pd.Timestamp(end_date, tz=APP_TIMEZONE)

    # Convert game_date to timezone-aware timestamps for comparison
    game_dates = df["game_date"].apply(lambda x: pd.Timestamp(x, tz=APP_TIMEZONE))
    mask = (game_dates >= start_ts) & (game_dates <= end_ts)
    return df[mask]


# ----- Dashboard Creation -----


def init_dashboard(server):
    """Initialize and attach the Dash dashboard to the Flask server."""
    dash_app = Dash(
        __name__,
        server=server,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
        url_base_pathname="/nba_dashboard/",
    )

    dash_app.config.suppress_callback_exceptions = True

    today = datetime.datetime.now(APP_TIMEZONE)

    # Dashboard Layout

    # Use fixed-width container to match main page width
    dash_app.layout = dbc.Container(
        children=[
            # Navigation Header
            dbc.Row(
                [
                    dbc.Col(
                        html.A(
                            [
                                html.Img(
                                    src="/static/img/basketball_hoop.png",
                                    height="50px",
                                    style={"margin-right": "10px"},
                                ),
                                html.Span(
                                    "NBA Betting",
                                    style={"font-size": "1.5rem", "font-weight": "bold"},
                                ),
                            ],
                            href="/",
                            style={
                                "text-decoration": "none",
                                "color": "inherit",
                                "display": "flex",
                                "align-items": "center",
                            },
                        ),
                        width="auto",
                    ),
                    dbc.Col(
                        dbc.Nav(
                            [
                                dbc.NavItem(dbc.NavLink("Home", href="/", external_link=True)),
                                dbc.NavItem(
                                    dbc.NavLink("Dashboard", href="/nba_dashboard/", active=True)
                                ),
                            ],
                            pills=True,
                        ),
                        width="auto",
                        className="ms-auto",
                    ),
                ],
                align="center",
                className="py-3 mb-0",
            ),
            # Controls Row
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
                            style={"font-size": 18},
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
                                        "font-size": 18,
                                        "margin-right": "10px",
                                        "margin-bottom": "0px",
                                    },
                                ),
                                dcc.DatePickerRange(
                                    min_date_allowed=datetime.date(2014, 10, 1),
                                    max_date_allowed=today,
                                    initial_visible_month=today,
                                    start_date=datetime.date(2024, 10, 1),
                                    end_date=today,
                                    minimum_nights=7,
                                    day_size=39,
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
                className="py-2",
                style={
                    "border-top": "1px solid #dee2e6",
                    "border-bottom": "1px solid #dee2e6",
                },
            ),
            # Charts - sized to fit in viewport without scrolling
            # Top chart: ~40% of remaining space
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            id="Cumulative_Profit_Loss",
                            style={"height": "38vh"},
                            config={"displayModeBar": False},
                        )
                    ),
                ],
                className="g-0",
            ),
            # Bottom charts: ~40% of remaining space, side by side
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            id="Bet_Win_Loss",
                            style={"height": "38vh"},
                            config={"displayModeBar": False},
                        )
                    ),
                    dbc.Col(
                        dcc.Graph(
                            id="Average_Spread_Error",
                            style={"height": "38vh"},
                            config={"displayModeBar": False},
                        )
                    ),
                ],
                className="g-0",
            ),
        ],
        fluid=False,  # Fixed-width container to match main page
    )

    # Callbacks

    @dash_app.callback(
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
        # Load fresh data on each callback (fixes stale data issue)
        engine = get_engine()
        with engine.connect() as connection:
            df = get_dashboard_data(connection)

        # Handle empty data
        if df.empty:
            empty_fig = create_empty_chart("No data available")
            return empty_fig, empty_fig, empty_fig

        # Filter by date range using helper function
        filtered_df = filter_by_date_range(df, start_date, end_date)

        if filtered_df.empty:
            empty_fig = create_empty_chart("No data for selected date range")
            return empty_fig, empty_fig, empty_fig

        # Cumulative ROI Over Time Line Chart (uses standard model variant)
        profit_loss_chart = update_profit_loss_chart(filtered_df, bet_category)

        # Bet Win Loss Bar Chart (uses standard model variant)
        win_loss_chart = update_win_loss_chart(filtered_df, bet_category)

        # Spread Error Chart (shows both standard and vegas model variants)
        spread_error_chart = update_spread_error_chart(filtered_df, engine=engine)

        return profit_loss_chart, win_loss_chart, spread_error_chart

    return dash_app


def create_empty_chart(message: str) -> go.Figure:
    """Create an empty chart with a centered message."""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font={"size": 20, "color": "#666666"},
    )
    fig.update_layout(
        xaxis={"visible": False},
        yaxis={"visible": False},
        plot_bgcolor="rgba(0, 0, 0, 0.1)",
    )
    return fig


# ----- Data Loading -----
def get_dashboard_data(connection):
    """
    Fetches and processes data for a dashboard.

    Args:
        connection: A database connection object.

    Returns:
        A pandas DataFrame containing the processed data.
    """
    # SQLite: use LEFT JOIN instead of FULL OUTER JOIN
    # Window functions (ROW_NUMBER OVER PARTITION BY) work in SQLite 3.25+
    # Filter to 'standard' model variant for consistent results (without Vegas line as feature)
    query = """
        SELECT
            g.game_id,
            g.game_datetime,
            g.home_team,
            g.away_team,
            g.open_line,
            g.home_score,
            g.away_score,
            p.confidence,
            p.pick,
            p.predicted_margin,
            p.model_variant,
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
        LEFT JOIN
            bets AS b ON g.game_id = b.game_id
        LEFT JOIN (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY game_id, model_variant ORDER BY predicted_at DESC) AS rn
            FROM predictions
            WHERE model_variant = 'standard'
        ) AS p ON p.game_id = g.game_id AND p.rn = 1
        WHERE g.game_datetime >= '2010-09-01'
        ORDER BY
            g.game_id DESC;

        """

    df = pd.read_sql(sql=query, con=connection)

    if df.empty:
        return df

    df["game_date"] = df["game_datetime"].apply(lambda x: pd.Timestamp(x, tz=APP_TIMEZONE)).dt.date
    df["game_result"] = df["home_score"] - df["away_score"]
    df["rec_bet_pl"] = df.apply(calc_rec_bet_profit_loss, axis=1)
    df["rec_bet_win_loss"] = df.apply(calc_rec_bet_win_loss, axis=1)

    # Fill NaN bet_profit_loss with 0
    df["bet_profit_loss"] = pd.to_numeric(df["bet_profit_loss"], errors="coerce").fillna(0)

    return df


# ----- Chart Creation/Updating -----
# NBA team colors for consistent branding
NBA_BLUE = "#17408B"
NBA_RED = "#C9082A"
NBA_LIGHT_BLUE = "#759FBC"


def update_spread_error_chart(df: pd.DataFrame, engine=None) -> go.Figure:
    """
    Generate a bar chart comparing spread prediction accuracy.

    Shows average absolute error (points per game) for:
    - Vegas spread: |actual_margin - vegas_spread|
    - Model: |actual_margin - model_predicted_spread|

    Lower values indicate more accurate predictions.

    Args:
        df: Pre-filtered DataFrame containing game results and predictions.
        engine: Database engine (unused, kept for API compatibility).

    Returns:
        Plotly Figure with the spread error comparison chart.
    """
    # Filter to completed games with spreads
    games_df = df[df["game_result"].notna() & df["open_line"].notna()].copy()

    if games_df.empty:
        return create_empty_chart("No completed games with spreads")

    # Calculate Vegas spread error: |actual_margin - (-open_line)|
    # open_line is from home team perspective, negative means home favored
    # game_result is home_score - away_score
    # Vegas predicts: home wins by (-open_line), e.g., open_line=-3 means home wins by 3
    games_df["vegas_error"] = abs(games_df["game_result"] - (-games_df["open_line"]))

    # Build chart data
    labels = []
    values = []
    colors = []
    counts = []

    # Vegas spread error (baseline)
    vegas_mean = games_df["vegas_error"].mean()
    vegas_count = len(games_df)
    labels.append("Vegas")
    values.append(vegas_mean)
    colors.append(NBA_BLUE)
    counts.append(vegas_count)

    # Model predictions
    if "predicted_margin" in games_df.columns:
        ml_valid = games_df["predicted_margin"].notna()
        if ml_valid.any():
            ml_error = abs(
                games_df.loc[ml_valid, "game_result"] - games_df.loc[ml_valid, "predicted_margin"]
            )
            ml_mean = ml_error.mean()
            ml_count = ml_valid.sum()
            labels.append("Model")
            values.append(ml_mean)
            # Color based on performance vs Vegas: red if worse, lighter blue if better
            colors.append(NBA_RED if ml_mean > vegas_mean else NBA_LIGHT_BLUE)
            counts.append(ml_count)

    # Calculate total games for subtitle
    total_games = vegas_count

    # Create the bar chart
    fig = go.Figure(
        data=[
            go.Bar(
                x=labels,
                y=values,
                text=[f"{v:.2f}" for v in values],
                textposition="outside",
                textfont={"size": 16, "color": "#000000", "family": "Arial"},
                marker_color=colors,
                width=0.5,
            )
        ]
    )

    # Chart title with game count
    title_text = "Spread Prediction Error"
    subtitle_text = f"<span style='font-size:14px;color:#666666;'>{total_games:,} games • Lower is better</span>"

    fig.update_layout(
        autosize=True,
        showlegend=False,
        plot_bgcolor="rgba(248, 249, 250, 1)",  # Light gray background like README
        paper_bgcolor="white",
        font={"size": 14, "family": "Arial", "color": "#000000"},
        title={
            "text": f"{title_text}<br>{subtitle_text}",
            "font": {"size": 18},
            "x": 0,
            "xanchor": "left",
            "y": 0.95,
        },
        margin={"t": 70, "b": 50, "l": 60, "r": 30},
        bargap=0.3,
    )

    fig.update_yaxes(
        title="Average Error (Points)",
        visible=True,
        showticklabels=True,
        tickfont={"size": 12},
        gridcolor="rgba(0, 0, 0, 0.1)",
        zeroline=True,
        zerolinecolor="rgba(0, 0, 0, 0.2)",
        range=[0, max(values) * 1.2] if values else [0, 15],  # Give room for labels
    )

    fig.update_xaxes(
        title="",
        visible=True,
        showticklabels=True,
        tickfont={"size": 14, "color": "#000000"},
        showgrid=False,
    )

    return fig


def update_win_loss_chart(new_df, bet_category):
    """Create a bar chart of win/loss information based on the bet category.

    Args:
        new_df (pandas.DataFrame): A DataFrame containing data on the actual or simulated bets.
        bet_category (str): A string indicating the type of bets to display in the chart.
            Must be either "Actual Bets" or "Simulated Bets".

    Returns:
        plotly.graph_objects.Figure: A bar chart of the win/loss information.

    Raises:
        ValueError: If the bet_category parameter is not set to either "Actual Bets" or "Simulated Bets".
    """

    # Check if the bet_category parameter is valid
    if bet_category not in ["Actual Bets", "Simulated Bets"]:
        raise ValueError("bet_category must be either 'Actual Bets' or 'Simulated Bets'")

    new_bet_win_loss_chart = None
    if bet_category == "Actual Bets":
        # Calculate win and loss counts for actual bets (case-insensitive)
        bet_status_lower = new_df["bet_status"].str.lower()
        win_count = (bet_status_lower == "win").sum()
        loss_count = (bet_status_lower == "loss").sum()
        chart_title = "Betting Record"
    elif bet_category == "Simulated Bets":
        # Calculate total win and loss counts for all simulated bets
        win_count = (new_df["rec_bet_win_loss"] == "Win").sum()
        loss_count = (new_df["rec_bet_win_loss"] == "Loss").sum()
        chart_title = "Simulated Betting Record"

    # Calculate win percentage
    total_bets = win_count + loss_count
    if total_bets == 0 and bet_category == "Actual Bets":
        return create_empty_chart("No bets recorded yet. Place bets from the home page.")

    win_pct = (win_count / total_bets * 100) if total_bets > 0 else 0

    # Subtitle with game count and break-even reference
    subtitle_text = f"<span style='font-size:14px;color:#666666;'>{total_bets:,} bets • 52.4% needed to break even</span>"

    # Create bar chart with wins (blue) and losses (red)
    new_bet_win_loss_chart = go.Figure(
        data=[
            go.Bar(
                x=["Wins", "Losses"],
                y=[win_count, loss_count],
                textposition="outside",
                texttemplate="%{y:,}",
                textfont={"size": 16, "color": "#000000", "family": "Arial"},
                marker_color=[NBA_BLUE, NBA_RED],
                width=0.5,
            )
        ]
    )
    new_bet_win_loss_chart.update_layout(
        autosize=True,
        showlegend=False,
        plot_bgcolor="rgba(248, 249, 250, 1)",
        paper_bgcolor="white",
        font={"size": 14, "family": "Arial", "color": "#000000"},
        title={
            "text": f"{chart_title} ({win_pct:.1f}%)<br>{subtitle_text}",
            "font": {"size": 18},
            "x": 0,
            "xanchor": "left",
            "y": 0.95,
        },
        margin={"t": 70, "b": 50, "l": 60, "r": 30},
        bargap=0.3,
    )
    new_bet_win_loss_chart.update_yaxes(
        title="",
        visible=True,
        showticklabels=True,
        tickfont={"size": 12},
        gridcolor="rgba(0, 0, 0, 0.1)",
        range=[0, max(win_count, loss_count) * 1.2] if total_bets > 0 else [0, 10],
    )
    new_bet_win_loss_chart.update_xaxes(
        title="",
        visible=True,
        showticklabels=True,
        tickfont={"size": 14, "color": "#000000"},
        showgrid=False,
    )

    return new_bet_win_loss_chart


def update_profit_loss_chart(new_df, bet_category):
    """
    Generate a cumulative ROI chart over time based on the specified bet category.

    Parameters:
    new_df (pandas.DataFrame): A DataFrame containing data on the actual or simulated bets.
    bet_category (str): A string indicating the type of bets to display in the chart.
        Must be either "Actual Bets" or "Simulated Bets".

    Returns:
    A Plotly figure object containing the ROI chart.

    Raises:
    ValueError: If the bet_category parameter is not "Actual Bets" or "Simulated Bets".
    """
    # Check if the bet_category parameter is valid
    if bet_category not in ["Actual Bets", "Simulated Bets"]:
        raise ValueError("bet_category must be either 'Actual Bets' or 'Simulated Bets'")

    new_profit_loss = None
    y_axis_title = "ROI %"
    hover_template = "ROI: %{y:.1f}%<extra></extra>"

    # Minimum bets before showing ROI to avoid early volatility
    MIN_BETS_FOR_ROI = 50

    if bet_category == "Actual Bets":
        # Calculate cumulative ROI for actual bets
        sorted_df = new_df.sort_values("game_id").copy()

        # Filter to actual bets with results (case-insensitive)
        bet_status_lower = sorted_df["bet_status"].str.lower()
        bet_df = sorted_df[bet_status_lower.isin(["win", "loss"])].copy()

        if not bet_df.empty:
            # Calculate cumulative profit and bet count by date
            daily_stats = (
                bet_df.groupby("game_date")
                .agg(
                    daily_profit=("bet_profit_loss", "sum"),
                    daily_wagered=("bet_amount", "sum"),
                    daily_bets=("bet_profit_loss", "count"),
                )
                .reset_index()
            )

            daily_stats["cumulative_profit"] = daily_stats["daily_profit"].cumsum()
            daily_stats["cumulative_wagered"] = daily_stats["daily_wagered"].cumsum()
            daily_stats["cumulative_bets"] = daily_stats["daily_bets"].cumsum()
            daily_stats["roi_pct"] = (
                daily_stats["cumulative_profit"] / daily_stats["cumulative_wagered"]
            ) * 100

            # Skip early volatile period
            daily_stats = daily_stats[daily_stats["cumulative_bets"] >= MIN_BETS_FOR_ROI]

            total_bets = len(bet_df)
            new_profit_loss = px.line(
                daily_stats,
                x="game_date",
                y="roi_pct",
                color_discrete_sequence=[NBA_BLUE],
            )
        else:
            # No actual bets recorded - show helpful message
            return create_empty_chart("No bets recorded yet. Place bets from the home page.")
    elif bet_category == "Simulated Bets":
        # Calculate cumulative ROI over time
        # ROI = cumulative_profit / cumulative_wagered * 100
        # Each bet wagers $100, so cumulative_wagered = num_bets * 100
        sorted_df = new_df.sort_values("game_id").copy()

        # Only include games with actual bets (Win or Loss, not "No Bet")
        bet_df = sorted_df[sorted_df["rec_bet_win_loss"].isin(["Win", "Loss"])].copy()

        if not bet_df.empty:
            # Calculate cumulative profit and bet count by date
            daily_stats = (
                bet_df.groupby("game_date")
                .agg(daily_profit=("rec_bet_pl", "sum"), daily_bets=("rec_bet_pl", "count"))
                .reset_index()
            )

            daily_stats["cumulative_profit"] = daily_stats["daily_profit"].cumsum()
            daily_stats["cumulative_bets"] = daily_stats["daily_bets"].cumsum()
            daily_stats["cumulative_wagered"] = daily_stats["cumulative_bets"] * 100
            daily_stats["roi_pct"] = (
                daily_stats["cumulative_profit"] / daily_stats["cumulative_wagered"]
            ) * 100

            # Skip early volatile period
            daily_stats = daily_stats[daily_stats["cumulative_bets"] >= MIN_BETS_FOR_ROI]

            total_bets = int(daily_stats["cumulative_bets"].iloc[-1]) if len(daily_stats) > 0 else 0
            new_profit_loss = px.line(
                daily_stats,
                x="game_date",
                y="roi_pct",
                color_discrete_sequence=[NBA_BLUE],
            )
        else:
            # No bets to show
            total_bets = 0
            new_profit_loss = px.line(
                pd.DataFrame({"game_date": [], "roi_pct": []}),
                x="game_date",
                y="roi_pct",
                color_discrete_sequence=[NBA_BLUE],
            )

    # Set title based on bet category
    chart_title = "Cumulative ROI" if bet_category == "Actual Bets" else "Simulated Cumulative ROI"
    subtitle_text = f"<span style='font-size:14px;color:#666666;'>{total_bets:,} bets</span>"

    new_profit_loss.update_layout(
        autosize=True,
        hovermode="x",
        showlegend=False,
        plot_bgcolor="rgba(248, 249, 250, 1)",
        paper_bgcolor="white",
        font={"size": 14, "family": "Arial", "color": "#000000"},
        title={
            "text": f"{chart_title}<br>{subtitle_text}",
            "font": {"size": 18},
            "x": 0,
            "xanchor": "left",
            "y": 0.95,
        },
        margin={"t": 70, "b": 50, "l": 60, "r": 30},
    )
    new_profit_loss.update_yaxes(
        title=y_axis_title,
        ticksuffix="%",
        visible=True,
        showticklabels=True,
        tickfont={"size": 12},
        gridcolor="rgba(0, 0, 0, 0.1)",
    )
    new_profit_loss.update_xaxes(
        title="",
        visible=True,
        showticklabels=True,
        tickfont={"size": 12},
        showgrid=False,
    )

    # Fresh approach: Use filled areas for color (no hover) + single line for hover
    if len(new_profit_loss["data"]) > 0 and len(new_profit_loss["data"][0]["y"]) > 0:
        x_values = list(new_profit_loss["data"][0]["x"])
        y_values = list(new_profit_loss["data"][0]["y"])
        y_min, y_max = min(y_values), max(y_values)

        # Clear the original trace - we'll build from scratch
        new_profit_loss.data = []

        # Build separate x/y arrays for positive and negative regions
        # Insert interpolated zero-crossing points to ensure lines meet at y=0
        x_pos, y_pos = [], []
        x_neg, y_neg = [], []

        for i in range(len(y_values)):
            y = y_values[i]
            x = x_values[i]

            # Check if we crossed zero from previous point
            if i > 0:
                prev_y = y_values[i - 1]
                # Crossed from negative to positive
                if prev_y < 0 and y >= 0:
                    # Both traces meet at zero
                    x_pos.append(x)
                    y_pos.append(0)
                    x_neg.append(x)
                    y_neg.append(0)
                # Crossed from positive to negative
                elif prev_y >= 0 and y < 0:
                    # Both traces meet at zero
                    x_pos.append(x)
                    y_pos.append(0)
                    x_neg.append(x)
                    y_neg.append(0)

            # Add the actual point to the appropriate trace
            if y >= 0:
                x_pos.append(x)
                y_pos.append(y)
                x_neg.append(x)
                y_neg.append(None)  # Gap in negative trace
            else:
                x_pos.append(x)
                y_pos.append(None)  # Gap in positive trace
                x_neg.append(x)
                y_neg.append(y)

        # Blue filled area for positive region (no hover)
        new_profit_loss.add_trace(
            go.Scatter(
                x=x_pos,
                y=y_pos,
                fill="tozeroy",
                fillcolor="rgba(23, 64, 139, 0.3)",  # NBA_BLUE with transparency
                line={"width": 2.5, "color": NBA_BLUE},
                mode="lines",
                hoverinfo="skip",
                showlegend=False,
                connectgaps=False,
            )
        )

        # Red filled area for negative region (no hover)
        new_profit_loss.add_trace(
            go.Scatter(
                x=x_neg,
                y=y_neg,
                fill="tozeroy",
                fillcolor="rgba(201, 8, 42, 0.3)",  # NBA_RED with transparency
                line={"width": 2.5, "color": NBA_RED},
                mode="lines",
                hoverinfo="skip",
                showlegend=False,
                connectgaps=False,
            )
        )

        # Single invisible line trace for tooltips (one per date)
        new_profit_loss.add_trace(
            go.Scatter(
                x=x_values,
                y=y_values,
                mode="lines",
                line={"width": 0, "color": "rgba(0,0,0,0)"},
                hovertemplate=hover_template,
                showlegend=False,
            )
        )

        # Zero reference line - draw as a trace so it appears on top of fills
        if y_min < 0 < y_max:
            new_profit_loss.add_trace(
                go.Scatter(
                    x=[x_values[0], x_values[-1]],
                    y=[0, 0],
                    mode="lines",
                    line={"width": 1.5, "color": "#333333", "dash": "dash"},
                    hoverinfo="skip",
                    showlegend=False,
                )
            )
    else:
        new_profit_loss["data"][0].update(
            hovertemplate=hover_template,
            mode="lines",
            line={"width": 2},
        )

    return new_profit_loss


# ----- Helper Functions -----
def calc_rec_bet_profit_loss(row) -> float:
    """
    Calculate simulated profit/loss for a recommended bet.

    Uses the shared calculate_spread_bet_profit function to properly handle:
    - Standard -110 odds (win ~$90.91 on $100 bet)
    - Push detection (returns $0 when game result equals spread)
    - Missing data (returns $0)

    Args:
        row: DataFrame row with 'pick', 'game_result', and 'open_line' columns

    Returns:
        Profit/loss amount (positive for win, negative for loss, 0 for push/no bet)
    """
    from src.betting import calculate_spread_bet_profit

    direction = row.get("pick")
    game_result = row.get("game_result")
    open_line = row.get("open_line")

    # Handle missing data
    if pd.isna(direction) or pd.isna(game_result) or pd.isna(open_line):
        return 0.0

    # Convert direction to bet_direction format
    if direction == "Home":
        bet_direction = "home"
    elif direction == "Away":
        bet_direction = "away"
    else:
        return 0.0

    # Use shared function: open_line is already the spread (negative = home favored)
    return calculate_spread_bet_profit(
        game_result=game_result,
        spread=open_line,
        bet_direction=bet_direction,
        bet_amount=100.0,
        bet_price=-110,
    )


def calc_rec_bet_win_loss(row) -> str:
    """
    Determine win/loss/push status for a simulated bet.

    Uses the shared determine_bet_outcome function for consistent spread logic.

    Args:
        row: DataFrame row with 'pick', 'game_result', and 'open_line' columns

    Returns:
        "Win", "Loss", "Push", or "No Bet"
    """
    from src.betting import determine_bet_outcome

    direction = row.get("pick")
    game_result = row.get("game_result")
    open_line = row.get("open_line")

    # Handle missing data
    if pd.isna(direction) or pd.isna(game_result) or pd.isna(open_line):
        return "No Bet"

    # Convert direction to bet_direction format
    if direction == "Home":
        bet_direction = "home"
    elif direction == "Away":
        bet_direction = "away"
    else:
        return "No Bet"

    # Use shared function
    outcome = determine_bet_outcome(
        game_result=game_result, spread=open_line, bet_direction=bet_direction
    )

    if outcome is True:
        return "Win"
    elif outcome is False:
        return "Loss"
    else:  # outcome is None (push)
        return "Push"


if __name__ == "__main__":
    pass
