"""Wait for the same monthly Bronze run, then submit a real Spark application."""
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    'silver_realestate_transform',
    start_date=pendulum.datetime(2026, 8, 1, tz='Asia/Seoul'),
    schedule='@monthly', catchup=True, max_active_runs=1,
    default_args={'owner': '김병필', 'retries': 1, 'retry_delay': timedelta(minutes=1)},
    tags=['realestate', 'silver', '김병필'],
) as dag:
    wait_bronze = ExternalTaskSensor(
        task_id='wait_bronze', external_dag_id='bronze_realestate_collect',
        external_task_id=None, allowed_states=['success'], failed_states=['failed'],
        check_existence=True, mode='reschedule', poke_interval=15, timeout=3600,
    )
    transform = BashOperator(
        task_id='spark_submit_silver',
        bash_command='''
        set -euo pipefail
        /opt/spark/bin/spark-submit --master 'local[2]' \
          --name silver_realestate_transform --driver-memory 2g \
          --conf spark.ui.port=4040 --conf spark.sql.shuffle.partitions=4 \
          /opt/airflow/scripts/Q2/silver_spark.py \
          --month '{{ data_interval_start.in_timezone("Asia/Seoul").strftime("%Y%m") }}' \
          --ui-hold-seconds '{{ dag_run.conf.get("ui_hold_seconds", 0) }}'
        ''',
        execution_timeout=timedelta(minutes=20),
    )
    wait_bronze >> transform
