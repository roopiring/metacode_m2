"""Monthly apartment sales collection: preserve source XML in real AWS S3."""
import os
import base64
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import unquote
import xml.etree.ElementTree as ET

import boto3
from botocore.config import Config
import pendulum
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

REGIONS = ('11680', '11650', '11710', '11440', '11170', '11200')
ENDPOINT = 'https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade'


def collect(lawd_cd, **context):
    print(f'collector={os.environ["COLLECTOR_NAME"]}, time={datetime.now()}, lawd={lawd_cd}')
    month = context['data_interval_start'].in_timezone('Asia/Seoul').strftime('%Y%m')
    folder = Path('/opt/airflow/data/bronze') / context['run_id'].replace('/', '_')
    folder.mkdir(parents=True, exist_ok=True)
    try:
        response = requests.get(ENDPOINT, params={
            'serviceKey': unquote(os.environ['DATA_GO_KR_API_KEY']),
            'LAWD_CD': lawd_cd, 'DEAL_YMD': month,
            'pageNo': 1, 'numOfRows': 10000,
        }, timeout=(10, 90))
    except requests.RequestException:
        raise AirflowException(f'API network failure, region={lawd_cd}') from None
    # Do not log response URLs: they contain the API key.
    if response.status_code != 200:
        raise AirflowException(f'API HTTP status {response.status_code}, region={lawd_cd}')
    path = folder / f'{lawd_cd}.xml'
    path.write_bytes(response.content)
    try:
        xml = ET.fromstring(response.content)
    except ET.ParseError:
        return {'valid': False, 'reason': 'XML parsing failed', 'region': lawd_cd}
    code = xml.findtext('.//resultCode')
    if code not in ('00', '000'):
        raise AirflowException(f'API resultCode={code!r}, region={lawd_cd}')
    count = int(xml.findtext('.//totalCount') or '0')
    if count > 10000:
        raise AirflowException(f'Pagination required: {count} records, region={lawd_cd}')
    return {'valid': count > 0, 'count': count, 'path': str(path),
            'key': f'bronze/{month}/{lawd_cd}.xml', 'region': lawd_cd}


def choose_branch(**context):
    results = [context['ti'].xcom_pull(task_ids=f'collect_group.collect_{r}') for r in REGIONS]
    invalid = [region for region, result in zip(REGIONS, results) if not result or not result.get('valid')]
    print(f'Regions with no records or invalid XML: {invalid}')
    return 'skip_upload' if invalid else 'upload_group'


def upload(lawd_cd, **context):
    item = context['ti'].xcom_pull(task_ids=f'collect_group.collect_{lawd_cd}')
    body = Path(item['path']).read_bytes()
    client = boto3.client('s3', config=Config(request_checksum_calculation='when_required'))
    client.put_object(Bucket=os.environ['S3_BUCKET'], Key=item['key'], Body=body,
                      ContentLength=len(body), ContentType='application/xml',
                      ContentMD5=base64.b64encode(hashlib.md5(body).digest()).decode())
    print(f'Uploaded region={lawd_cd}, records={item["count"]}, key={item["key"]}')


def upload_all(**context):
    for region in REGIONS:
        upload(region, **context)


with DAG(
    dag_id='bronze_realestate_collect',
    start_date=pendulum.datetime(2026, 8, 1, tz='Asia/Seoul'),
    schedule='@monthly', catchup=True, max_active_runs=1,
    default_args={'owner': '김병필', 'retries': 2, 'retry_delay': timedelta(minutes=1)},
    tags=['realestate', 'bronze', '김병필'],
) as dag:
    with TaskGroup('collect_group') as collection:
        for region in REGIONS:
            PythonOperator(task_id=f'collect_{region}', python_callable=collect, op_kwargs={'lawd_cd': region})
    branch = BranchPythonOperator(task_id='branch_after_collect', python_callable=choose_branch)
    uploads = PythonOperator(task_id='upload_group', python_callable=upload_all)
    skipped = EmptyOperator(task_id='skip_upload')
    done = EmptyOperator(task_id='summary_done', trigger_rule='none_failed_min_one_success')
    collection >> branch >> [uploads, skipped] >> done
