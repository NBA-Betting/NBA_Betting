import os
import sys
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
    "NBAStats_Player_Boxscores_Inbound",
    default_args={
        "owner": "Jeff",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    description="A DAG to run the NBA Stats Player Boxscores Spiders daily",
    schedule="0 15 * * *",  # 9am MT
    catchup=False,
)

spiders = [
    "nba_stats_boxscores_traditional_spider",
    "nba_stats_boxscores_adv_advanced_spider",
    "nba_stats_boxscores_adv_misc_spider",
    "nba_stats_boxscores_adv_scoring_spider",
    "nba_stats_boxscores_adv_traditional_spider",
    "nba_stats_boxscores_adv_usage_spider",
]


for spider in spiders:
    if os.environ.get("ENVIRONMENT") == "EC2":
        command = f"cd /home/ubuntu/nba_betting_player_game/src/data_feeds/data_sources && scrapy crawl {spider} -a dates=daily_update -a save_data=True"
    else:
        command = f"cd /workspaces/NBA_Betting/src/data_feeds/data_sources && scrapy crawl {spider} -a dates=daily_update -a save_data=True"

    BashOperator(
        task_id=f"run_{spider}",
        bash_command=command,
        dag=dag,
    )
