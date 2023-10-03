import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../../"))

from src.etl.main_etl import ETLPipeline

# Define default arguments
default_args = {
    "owner": "Jeff",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": pendulum.datetime(2023, 5, 1),
    "email": ["jeffjohannsen7@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
}

# Initialize the DAG
dag = DAG(
    "ETL_Daily_Update",
    default_args=default_args,
    description="A DAG to run the ETL update daily",
    schedule_interval="00 17 * * *",  # 11:00am MT
    catchup=False,
)


@task(dag=dag)
def run_etl_daily_update():
    start_date = "2023-09-01"
    ETL = ETLPipeline(start_date)

    ETL.load_features_data(
        [
            "team_fivethirtyeight_games",
            "team_nbastats_general_traditional",
            "team_nbastats_general_advanced",
            "team_nbastats_general_fourfactors",
            "team_nbastats_general_opponent",
        ]
    )

    ETL.prepare_all_tables()

    ETL.feature_creation_pre_merge()

    ETL.merge_features_data()

    ETL.feature_creation_post_merge()

    ETL.clean_and_save_combined_features()


# Set the task to be executed by the DAG
run_etl_daily_update()
