import logging
import os
from datetime import datetime, timezone

import boto3
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

BUCKET = os.environ["S3_BUCKET"]
LOCAL_CSV = "/opt/airflow/tmp/netflix_titles.csv"
TRANSFORM_SCRIPT = "/opt/airflow/dags/jobs/transform.py"


def local_parquet_dir(run_id: str) -> str:
    # 실행마다(run_id) 별도 경로를 써서 동시 실행 시 서로 덮어쓰지 않게 함
    return f"/opt/airflow/tmp/genre_counts/{run_id.replace(':', '-')}"


def download_csv(**context):
    s3 = boto3.client("s3")
    os.makedirs(os.path.dirname(LOCAL_CSV), exist_ok=True)
    s3.download_file(BUCKET, "bronze/netflix_titles.csv", LOCAL_CSV)
    log.info("downloaded s3://%s/bronze/netflix_titles.csv -> %s", BUCKET, LOCAL_CSV)


def upload_silver(**context):
    ds = context.get("ds") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    local_parquet = local_parquet_dir(context["run_id"])
    prefix = f"silver/{ds}/"
    s3 = boto3.client("s3")

    for root, _, files in os.walk(local_parquet):
        for name in files:
            if name.startswith(".") or name.endswith(".crc"):
                continue
            local_path = os.path.join(root, name)
            key = prefix + name
            s3.upload_file(local_path, BUCKET, key)
            log.info("uploaded %s -> s3://%s/%s", local_path, BUCKET, key)

    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    objects = resp.get("Contents", [])
    for obj in objects:
        log.info("  %s  %s bytes", obj["Key"], obj["Size"])
    log.info("silver object count = %d", len(objects))


with DAG(
    dag_id="weekly_pipeline_seungsoohan",
    schedule="@weekly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    params={"min_year": 2015},
    tags=["q9"],
) as dag:
    download_csv_task = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
    )

    transform_task = BashOperator(
        task_id="transform",
        bash_command=(
            f"spark-submit {TRANSFORM_SCRIPT} "
            f"--input {LOCAL_CSV} "
            "--output /opt/airflow/tmp/genre_counts/{{ run_id | replace(':', '-') }} "
            "--min-year {{ params.min_year }}"
        ),
    )

    upload_silver_task = PythonOperator(
        task_id="upload_silver",
        python_callable=upload_silver,
    )

    download_csv_task >> transform_task >> upload_silver_task
