import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.decorators import task

# Add the workspace root to the Python path
workspace_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(workspace_root))
from src.bet_management.financials import BankAccount, update_bank_account_balance

# Define the DAG
dag = DAG(
    "Daily_Bank_Account_Balance_Update",
    default_args={
        "owner": "Jeff",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    description="A DAG to update bank account balance daily",
    schedule="0 6 * * *",  # 12am MT
    catchup=False,
)


# Define the task using the TaskFlow API
@task(dag=dag)
def update_balance_task():
    update_bank_account_balance()


# Set the task to be executed by the DAG
update_balance_task()
