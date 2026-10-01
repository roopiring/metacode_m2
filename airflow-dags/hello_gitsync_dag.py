"""Verify that Kubernetes Airflow loads and executes DAGs from GitHub."""
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG('hello_gitsync', start_date=pendulum.datetime(2026, 8, 1, tz='Asia/Seoul'),
         schedule=None, catchup=False, tags=['gitsync', 'Q4', '김병필']) as dag:
    say_hello = BashOperator(task_id='say_hello',
                            bash_command='echo "Hello git-sync! collector=김병필"; date -u')
