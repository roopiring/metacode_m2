"""Monthly Silver -> Gold Spark aggregation and PostgreSQL validation."""
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

TABLES = ('gold_realestate_district_avg', 'gold_realestate_top10',
          'gold_realestate_size_dist', 'gold_realestate_age_avg',
          'gold_realestate_mom_change')


def verify_gold():
    hook = PostgresHook(postgres_conn_id='realestate_postgres')
    for table in TABLES:
        count = hook.get_first(f'SELECT COUNT(*) FROM {table}')[0]
        if count <= 0:
            raise ValueError(f'{table} is empty')
        print(f'{table}: row_count={count}')


with DAG(
    'gold_realestate_aggregate', start_date=pendulum.datetime(2026, 8, 1, tz='Asia/Seoul'),
    schedule='@monthly', catchup=True, max_active_runs=1,
    default_args={'owner': '김병필', 'retries': 1, 'retry_delay': timedelta(minutes=1)},
    tags=['realestate', 'gold', '김병필'],
) as dag:
    wait_silver = ExternalTaskSensor(
        task_id='wait_silver', external_dag_id='silver_realestate_transform',
        external_task_id=None, allowed_states=['success'], failed_states=['failed'],
        check_existence=True, mode='reschedule', poke_interval=15, timeout=3600,
    )
    aggregate = BashOperator(
        task_id='spark_submit_gold',
        bash_command='''
        set -euo pipefail
        /opt/spark/bin/spark-submit --master 'local[2]' \
          --name gold_realestate_aggregate --driver-memory 2g \
          --conf spark.sql.shuffle.partitions=4 \
          /opt/airflow/scripts/Q3/gold_spark_sql.py \
          --month '{{ data_interval_start.in_timezone("Asia/Seoul").strftime("%Y%m") }}'
        ''',
        execution_timeout=timedelta(minutes=20),
    )
    verify = PythonOperator(task_id='verify_gold_tables', python_callable=verify_gold)
    wait_silver >> aggregate >> verify
