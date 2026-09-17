import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)


def count_lines(**context):
    ti = context["ti"]
    if ti.try_number == 1:
        raise RuntimeError("intentional failure on first attempt")
    dag_files = [f for f in os.listdir(os.path.dirname(__file__)) if f.endswith(".py")]
    return len(dag_files)


def use_value(**context):
    ti = context["ti"]
    value = ti.xcom_pull(task_ids="count_lines")
    doubled = value * 2
    log.info("received from XCom: %s", value)
    log.info("doubled = %s", doubled)


with DAG(
    dag_id="xcom_demo_seungsoohan",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["q6"],
) as dag:
    count_lines_task = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,
        retry_delay=timedelta(seconds=15),
    )
    use_value_task = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    count_lines_task >> use_value_task
