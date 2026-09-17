import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

OUTPUT_DIR = Path("/opt/airflow/backfill_output")


def write_daily_file(**context):
    ds = context["ds"]
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"daily_{ds}.txt"
    path.write_text(f"backfill run for {ds}\n")
    log.info("wrote %s", path)


def verify_file(**context):
    ds = context["ds"]
    path = OUTPUT_DIR / f"daily_{ds}.txt"
    content = path.read_text()
    log.info("verified %s: %s", path, content.strip())


with DAG(
    dag_id="backfill_demo_seungsoohan",
    schedule="@daily",
    start_date=datetime(2026, 9, 11),
    catchup=True,
    tags=["q7"],
) as dag:
    write_daily_file_task = PythonOperator(
        task_id="write_daily_file",
        python_callable=write_daily_file,
    )
    verify_file_task = PythonOperator(
        task_id="verify_file",
        python_callable=verify_file,
    )

    write_daily_file_task >> verify_file_task
