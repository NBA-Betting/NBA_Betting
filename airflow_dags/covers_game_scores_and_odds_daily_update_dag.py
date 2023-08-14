import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the DAG
dag = DAG(
    "Covers_Game_Scores_And_Odds_Daily_Update",
    default_args={
        "owner": "Jeff",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email_on_failure": True,
        "email_on_retry": True,
    },
    description="A DAG to run the Covers game scores and odds spider daily",
    schedule="0 16 * * *",  # 10am MT
    catchup=False,
)


if os.environ.get("ENVIRONMENT") == "EC2":
    command = f"cd /home/ubuntu/NBA_Betting/src/data_sources/game && scrapy crawl game_covers_historic_scores_and_odds_spider -a dates=daily_update -a save_data=True"
else:
    command = f"cd ~/Documents/NBA_Betting/src/data_sources/game && scrapy crawl game_covers_historic_scores_and_odds_spider -a dates=daily_update -a save_data=True"

BashOperator(
    task_id=f"run_covers_game_scores_and_odds_spider",
    bash_command=command,
    dag=dag,
)
