import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the DAG
dag = DAG(
    "ETL_Daily_Update",
    default_args={
        "owner": "Jeff",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email_on_failure": True,
        "email_on_retry": True,
    },
    description="A DAG to run the ETL update daily",
    schedule="0 16 * * *",  # 11 am MT. After inbound data is updated
    catchup=False,
)

if os.environ.get("ENVIRONMENT") == "EC2":
    command = "cd /home/ubuntu/NBA_Betting/src/etl && python main_etl.py"
else:
    command = "cd ~/Documents/NBA_Betting/src/etl && python main_etl.py"
# Define the task using the BashOperator
run_fivethirtyeight_player_spider = BashOperator(
    task_id="run_etl",
    bash_command=command,
    dag=dag,
)
