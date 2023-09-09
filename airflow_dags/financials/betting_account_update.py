import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../../"))
from src.bet_management.financials import update_betting_account_balance

# Define the DAG
dag = DAG(
    "Betting_Account_Balance_Daily_Update",
    default_args={
        "owner": "Jeff",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email_on_failure": True,
        "email_on_retry": True,
    },
    description="A DAG to update betting account balance daily",
    schedule="0 6 * * *",  # 12am MT
    catchup=False,
)


# Define the task using the TaskFlow API
@task(dag=dag)
def run_update_balance():
    update_betting_account_balance()


# Set the task to be executed by the DAG
run_update_balance()
