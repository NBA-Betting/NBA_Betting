import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../../"))

from src.bet_management.bet_decisions import main_predictions

load_dotenv()
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")

# Define default arguments
default_args = {
    "owner": "Jeff",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": pendulum.datetime(2023, 5, 1),
    "email": [EMAIL_ADDRESS],
    "email_on_failure": True,
    "email_on_retry": True,
}

# Initialize the DAG
dag = DAG(
    "Predictions_Daily_Update",
    default_args=default_args,
    description="A DAG to run the predictions daily",
    schedule_interval="30 17 * * *",  # 11:30am MT
    catchup=False,
)


@task(dag=dag)
def run_predictions_daily_update():
    main_predictions(True, None, None)


# Set the task to be executed by the DAG
run_predictions_daily_update()
