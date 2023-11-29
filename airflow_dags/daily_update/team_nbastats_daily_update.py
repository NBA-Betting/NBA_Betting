import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from dotenv import load_dotenv

load_dotenv()
NBA_BETTING_BASE_DIR = os.getenv("NBA_BETTING_BASE_DIR")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")

# Define the DAG
dag = DAG(
    "Team_NBAStats_Daily_Update",
    default_args={
        "owner": "Jeff",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email": [EMAIL_ADDRESS],
        "email_on_failure": True,
        "email_on_retry": True,
    },
    description="A DAG to run the NBA Stats Team Spiders daily",
    schedule_interval="30 16 * * *",  # 10:30am MT (4:30 PM UTC)
    catchup=False,
)

spiders = [
    "team_nbastats_general_traditional_spider_zyte",
    "team_nbastats_general_advanced_spider_zyte",
    "team_nbastats_general_fourfactors_spider_zyte",
    "team_nbastats_general_opponent_spider_zyte",
]


for spider in spiders:
    command = f"cd {NBA_BETTING_BASE_DIR}/src/data_sources/team && scrapy crawl {spider} -a dates=daily_update -a save_data=True -a view_data=True"

    BashOperator(
        task_id=f"run_{spider}",
        bash_command=command,
        dag=dag,
    )
