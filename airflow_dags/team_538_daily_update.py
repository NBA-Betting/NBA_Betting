import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the DAG
dag = DAG(
    "Team_538_Daily_Update",
    default_args={
        "owner": "Jeff",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": pendulum.datetime(2023, 5, 1),
        "email_on_failure": True,
        "email_on_retry": True,
    },
    description="A DAG to run the 538 team stats update daily",
    schedule="0 16 * * *",  # 10am MT
    catchup=False,
)

if os.environ.get("ENVIRONMENT") == "EC2":
    command = "cd /home/ubuntu/NBA_Betting/src/data_sources/team && python fivethirtyeight_games.py"
else:
    command = "cd ~/Documents/NBA_Betting/src/data_sources/team && python fivethirtyeight_games.py"
# Define the task using the BashOperator
run_fivethirtyeight_player_spider = BashOperator(
    task_id="run_538_team_stats",
    bash_command=command,
    dag=dag,
)
