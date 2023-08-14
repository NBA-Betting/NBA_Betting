import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the DAG
dag = DAG(
    "NBAStats_Team_Daily_Update",
    default_args={
        "owner": "Jeff",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email_on_failure": True,
        "email_on_retry": True,
    },
    description="A DAG to run the NBA Stats Team Spiders daily",
    schedule="0 15 * * *",  # 9am MT
    catchup=False,
)

spiders = [
    "team_nbastats_general_traditional_spider_zyte",
    "team_nbastats_general_advanced_spider_zyte",
    "team_nbastats_general_fourfactors_spider_zyte",
    "team_nbastats_general_opponent_spider_zyte",
]


for spider in spiders:
    if os.environ.get("ENVIRONMENT") == "EC2":
        command = f"cd ~/home/ubuntu/NBA_Betting/src/data_sources/team && scrapy crawl {spider} -a dates=daily_update -a save_data=True -a view_data=False"
    else:
        command = f"cd ~/Documents/NBA_Betting/src/data_sources/team && scrapy crawl {spider} -a dates=daily_update -a save_data=True -a view_data=False"

    BashOperator(
        task_id=f"run_{spider}",
        bash_command=command,
        dag=dag,
    )
