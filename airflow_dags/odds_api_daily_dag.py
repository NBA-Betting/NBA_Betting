import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, ".."))

from src.data_sources.game.odds_api import update_game_data

# Define the DAG
dag = DAG(
    "Odds_Api_Daily_Update",
    default_args={
        "owner": "Jeff",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email_on_failure": True,
        "email_on_retry": True,
    },
    description="A DAG to run the Odds Api odds and scores update daily",
    schedule_interval="0 16 * * *",  # 10am MT
    catchup=False,
)

# Define the task using the PythonOperator
run_odds_api = PythonOperator(
    task_id="run_odds_api",
    python_callable=update_game_data,
    op_kwargs={"past_games": True},
    dag=dag,
)
