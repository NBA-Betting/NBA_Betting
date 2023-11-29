import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../../"))

from src.data_sources.game.odds_api import update_game_data

load_dotenv()
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")

# Define the DAG
dag = DAG(
    "Odds_Api_Daily_Update",
    default_args={
        "owner": "Jeff",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email": [EMAIL_ADDRESS],
        "email_on_failure": True,
        "email_on_retry": True,
    },
    description="A DAG to run the Odds Api odds and scores update daily",
    schedule_interval="45 15 * * *",  # 9:45am MT
    catchup=False,
)


@task(dag=dag)
def run_odds_api_daily_update():
    update_game_data(past_games=True)


# Set the task to be executed by the DAG
run_odds_api_daily_update()
