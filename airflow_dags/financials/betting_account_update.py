# Import necessary modules
import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv

# Get the current directory
here = os.path.dirname(os.path.realpath(__file__))

# Add the parent directory to the sys path
sys.path.append(os.path.join(here, "../../"))

# Import the function to update the betting account balance
from src.bet_management.financials import update_betting_account_balance

# Load environment variables from .env file
load_dotenv()

# Get the email address from the environment variables
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")

# Define the DAG
dag = DAG(
    "Betting_Account_Balance_Daily_Update",
    default_args={
        "owner": "Jeff",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email": [EMAIL_ADDRESS],
        "email_on_failure": True,
        "email_on_retry": True,
    },
    description="A DAG to update betting account balance daily",
    schedule="0 6 * * *",  # Run at 12am MT
    catchup=False,
)


# Define the task using the TaskFlow API
@task(dag=dag)
def run_update_balance():
    update_betting_account_balance()


# Set the task to be executed by the DAG
run_update_balance()
