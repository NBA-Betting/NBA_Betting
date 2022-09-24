import sys
import pytz
import datetime
from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine

sys.path.append('../../')
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

app = Dash(__name__,
           external_stylesheets=[dbc.themes.BOOTSTRAP],
           meta_tags=[
               {
                   "name": "viewport",
                   "content": "width=device-width, initial-scale=1"
               },
           ])

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
                        rec_bet_direction,
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
    if x['rec_bet_direction'] == 'Home':
        pl = 91 if x['game_result'] > -x['home_line'] else -100
    elif x['rec_bet_direction'] == 'Away':
        pl = 91 if x['game_result'] < -x['home_line'] else -100
    else:
        pl = 0
    return pl


df['rec_bet_pl'] = df.apply(calc_rec_bet_pl, axis=1)
df['vegas_miss'] = abs(df['game_result'] - -df['home_line'])
df['ml_miss'] = abs(df['game_result'] - df['ml_reg_prediction'])
df['dl_miss'] = abs(df['game_result'] - df['dl_reg_prediction'])
df['bet_profit_loss'] = df['bet_profit_loss'].fillna(0)
df_2 = df.copy()

date_mask = (df['date'] >= pd.to_datetime(year_ago)) & (df['date'] <=
                                                        pd.to_datetime(today))
df = df[date_mask]
pl_data = df.sort_values('game_id').groupby(
    'date').sum()['bet_profit_loss'].cumsum().reset_index()

win_count = df[df['bet_outcome'] == 'Win']['bet_outcome'].count()
loss_count = df[df['bet_outcome'] == 'Loss']['bet_outcome'].count()

line_miss_x = ['Vegas Miss', 'ML Pred Miss', 'DL Pred Miss']
line_miss_y = [
    df['vegas_miss'].mean(), df['ml_miss'].mean(), df['dl_miss'].mean()
]

# Individual Graph Creation

cum_pl_ot = px.line(
    pl_data,
    x="date",
    y="bet_profit_loss",
    title='Profit/Loss Over Time',
    color_discrete_sequence=['#17408B'],
)
cum_pl_ot.update_layout(hovermode='x unified',
                        showlegend=False,
                        plot_bgcolor='rgba(0, 0, 0, 0.1)',
                        font={
                            'size': 20,
                            'family': 'Arial',
                            'color': '#000000'
                        },
                        title={'font': {
                            'size': 28
                        }})
cum_pl_ot.update_yaxes(title='',
                       rangemode='tozero',
                       visible=True,
                       showticklabels=True,
                       tickfont={'size': 14})
cum_pl_ot.update_xaxes(title='', visible=True, showticklabels=True)
cum_pl_ot.update_traces(
    mode="markers+lines",
    hovertemplate='Cummulative Profit/Loss: $ %{y} <extra></extra>')

wl = go.Figure(data=[
    go.Bar(x=['Wins', 'Losses'],
           y=[win_count, loss_count],
           textposition='auto',
           texttemplate='%{y}',
           marker_color=['#17408B', '#C9082A'])
])
wl.update_layout(title_text='Bet Win/Loss',
                 showlegend=False,
                 plot_bgcolor='rgba(0, 0, 0, 0.1)',
                 font={
                     'size': 20,
                     'family': 'Arial',
                     'color': '#000000'
                 },
                 title={'font': {
                     'size': 28
                 }})
wl.update_yaxes(title='',
                visible=True,
                showticklabels=True,
                tickfont={'size': 14})
wl.update_xaxes(title='', visible=True, showticklabels=True)

line_diff = go.Figure(data=[
    go.Bar(x=line_miss_x,
           y=line_miss_y,
           textposition='auto',
           texttemplate='%{y:.2f}',
           marker_color='#17408B')
])
line_diff.update_layout(title_text='Average Line Miss',
                        showlegend=False,
                        plot_bgcolor='rgba(0, 0, 0, 0.1)',
                        font={
                            'size': 20,
                            'family': 'Arial',
                            'color': '#000000'
                        },
                        title={'font': {
                            'size': 28
                        }})
line_diff.update_yaxes(title='',
                       visible=True,
                       showticklabels=True,
                       tickfont={'size': 14})
line_diff.update_xaxes(title='', visible=True, showticklabels=True)

# Dashboard Layout

app.layout = dbc.Container(children=[
    dbc.Row([
        dbc.Col(dcc.DatePickerRange(min_date_allowed=datetime.date(
            2014, 10, 1),
                                    max_date_allowed=today,
                                    initial_visible_month=today,
                                    start_date=year_ago,
                                    end_date=today,
                                    minimum_nights=7,
                                    day_size=50,
                                    id='date_range',
                                    style={'font-size': 24}),
                style={
                    'justify-content': 'flex-end',
                    'display': 'flex'
                }),
        dbc.Col(dbc.RadioItems(options=[
            {
                "label": "All Games",
                "value": 'All Games'
            },
            {
                "label": "Home",
                "value": 'Home'
            },
            {
                "label": "Away",
                "value": 'Away'
            },
        ],
                               value='All Games',
                               inline=True,
                               id="all_home_away",
                               style={'font-size': 24}),
                style={
                    'justify-content': 'flex-start',
                    'display': 'flex'
                })
    ],
            align='center',
            justify='center'),
    dbc.Row([
        dbc.Col(dcc.Graph(id='Cum_PL_Over_Time', figure=cum_pl_ot)),
    ],
            justify='center'),
    dbc.Row([
        dbc.Col(dcc.Graph(id='Bet_Win_Loss', figure=wl)),
        dbc.Col(dcc.Graph(id='Average_Line_Miss', figure=line_diff))
    ])
],
                           fluid=True)

# Callbacks


@app.callback(Output('Cum_PL_Over_Time', 'figure'),
              Output('Bet_Win_Loss', 'figure'),
              Output('Average_Line_Miss', 'figure'),
              Input('all_home_away', 'value'), Input('date_range',
                                                     'start_date'),
              Input('date_range', 'end_date'))
def update_output(home_away, start_date, end_date):
    if home_away == 'All Games':
        new_df = df_2.copy()
    elif home_away == 'Home':
        new_df = df_2[df_2['bet_direction'] == 'Home']
    elif home_away == 'Away':
        new_df = df_2[df_2['bet_direction'] == 'Away']

    new_date_mask = (new_df['date'] >= pd.to_datetime(start_date)) & (
        new_df['date'] <= pd.to_datetime(end_date))
    new_df = new_df[new_date_mask]
    new_pl_data = new_df.sort_values('game_id').groupby(
        'date').sum()['bet_profit_loss'].cumsum().reset_index()

    new_win_count = new_df[new_df['bet_outcome'] ==
                           'Win']['bet_outcome'].count()
    new_loss_count = new_df[new_df['bet_outcome'] ==
                            'Loss']['bet_outcome'].count()

    new_line_miss_date_mask = (df_2['date'] >= pd.to_datetime(start_date)) & (
        df_2['date'] <= pd.to_datetime(end_date))
    line_miss_df = df_2[new_line_miss_date_mask]
    new_line_miss_x = ['Vegas Miss', 'ML Pred Miss', 'DL Pred Miss']
    new_line_miss_y = [
        line_miss_df['vegas_miss'].mean(), line_miss_df['ml_miss'].mean(),
        line_miss_df['dl_miss'].mean()
    ]

    new_cum_pl_ot = px.line(
        new_pl_data,
        x="date",
        y="bet_profit_loss",
        title='Profit/Loss Over Time',
        color_discrete_sequence=['#17408B'],
    )
    new_cum_pl_ot.update_layout(autosize=True,
                                hovermode='x unified',
                                showlegend=False,
                                plot_bgcolor='rgba(0, 0, 0, 0.1)',
                                font={
                                    'size': 20,
                                    'family': 'Arial',
                                    'color': '#000000'
                                },
                                title={'font': {
                                    'size': 28
                                }})
    new_cum_pl_ot.update_yaxes(title='',
                               rangemode='tozero',
                               visible=True,
                               showticklabels=True,
                               tickfont={'size': 14})
    new_cum_pl_ot.update_xaxes(title='', visible=True, showticklabels=True)
    new_cum_pl_ot.update_traces(
        mode="markers+lines",
        hovertemplate='Cummulative Profit/Loss: $ %{y} <extra></extra>')

    new_wl = go.Figure(data=[
        go.Bar(x=['Wins', 'Losses'],
               y=[new_win_count, new_loss_count],
               textposition='auto',
               texttemplate='%{y}',
               marker_color=['#17408B', '#C9082A'])
    ])
    new_wl.update_layout(autosize=True,
                         title_text='Bet Win/Loss',
                         showlegend=False,
                         plot_bgcolor='rgba(0, 0, 0, 0.1)',
                         font={
                             'size': 20,
                             'family': 'Arial',
                             'color': '#000000'
                         },
                         title={'font': {
                             'size': 28
                         }})
    new_wl.update_yaxes(title='',
                        visible=True,
                        showticklabels=True,
                        tickfont={'size': 14})
    new_wl.update_xaxes(title='', visible=True, showticklabels=True)

    new_line_diff = go.Figure(data=[
        go.Bar(x=new_line_miss_x,
               y=new_line_miss_y,
               textposition='auto',
               texttemplate='%{y:.2f}',
               marker_color='#17408B')
    ])
    new_line_diff.update_layout(autosize=True,
                                title_text='Average Line Miss',
                                showlegend=False,
                                plot_bgcolor='rgba(0, 0, 0, 0.1)',
                                font={
                                    'size': 20,
                                    'family': 'Arial',
                                    'color': '#000000'
                                },
                                title={'font': {
                                    'size': 28
                                }})
    new_line_diff.update_yaxes(title='',
                               visible=True,
                               showticklabels=True,
                               tickfont={'size': 14})
    new_line_diff.update_xaxes(title='', visible=True, showticklabels=True)

    return new_cum_pl_ot, new_wl, new_line_diff


if __name__ == '__main__':
    app.run_server(debug=True)
