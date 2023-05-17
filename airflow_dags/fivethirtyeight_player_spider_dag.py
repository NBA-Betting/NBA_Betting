import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Add the workspace root to the Python path
workspace_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(workspace_root))

# Define the DAG
dag = DAG(
    "Daily_Fivethirtyeight_Player_Spider_Run",
    default_args={
        "owner": "Jeff",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    description="A DAG to run the Fivethirtyeight Player Spider daily",
    schedule="0 14 * * *",  # 8am MT
    catchup=False,
)

if os.environ.get("ENVIRONMENT") == "EC2":
    command = "cd /home/ubuntu/nba_betting_player_game/src/data_feeds/data_sources && scrapy crawl fivethirtyeight_player_spider -a dates=daily_update -a save_data=True"
else:
    command = "cd /workspaces/NBA_Betting/src/data_feeds/data_sources && scrapy crawl fivethirtyeight_player_spider -a dates=daily_update -a save_data=True"
# Define the task using the BashOperator
run_fivethirtyeight_player_spider = BashOperator(
    task_id="run_fivethirtyeight_player_spider",
    bash_command=command
    dag=dag,
)
