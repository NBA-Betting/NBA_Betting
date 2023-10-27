import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../../"))

from src.data_sources.team.fivethirtyeight_games import update_all_data_538

# Define default arguments
default_args = {
    "owner": "Jeff",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": pendulum.datetime(2023, 5, 1),
    "email": ["jeffjohannsen7@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
}

# Initialize the DAG
dag = DAG(
    "Team_538_Daily_Update",
    default_args=default_args,
    description="A DAG to run the 538 team stats update daily",
    schedule_interval="15 16 * * *",  # 10:15am MT
    catchup=False,
)


@task(dag=dag)
def run_538_team_stats():
    try:
        username = "postgres"
        password = os.getenv("RDS_PASSWORD")
        endpoint = os.getenv("RDS_ENDPOINT")
        database = "nba_betting"
        engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
        )

        update_all_data_538(engine)
        print("-----538 Data Update Successful-----")
    except Exception as e:
        print("-----538 Data Update Failed-----")
        raise e


# Set the task to be executed by the DAG
run_538_team_stats()
