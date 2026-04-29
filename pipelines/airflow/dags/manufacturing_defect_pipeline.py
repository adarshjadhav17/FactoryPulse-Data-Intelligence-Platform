"""Airflow DAG for the manufacturing defect sample pipeline."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_ROOT = Path(__file__).resolve().parents[3]


with DAG(
    dag_id="manufacturing_defect_sample_pipeline",
    description="Profile Bosch samples, validate Kafka ingestion, and load Snowflake raw tables.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["manufacturing", "kafka", "snowflake"],
) as dag:
    start = EmptyOperator(task_id="start")

    profile_samples = BashOperator(
        task_id="profile_samples",
        bash_command="PYTHONPATH=src scripts/run_profile.sh 1000",
        cwd=str(PROJECT_ROOT),
    )

    run_unit_tests = BashOperator(
        task_id="run_unit_tests",
        bash_command="PYTHONPATH=src python -m unittest discover -s tests",
        cwd=str(PROJECT_ROOT),
    )

    kafka_smoke_check = BashOperator(
        task_id="kafka_smoke_check",
        bash_command="PYTHONPATH=src scripts/run_phase3_kafka_check.sh",
        cwd=str(PROJECT_ROOT),
    )

    load_snowflake_raw = BashOperator(
        task_id="load_snowflake_raw",
        bash_command="PYTHONPATH=src scripts/run_snowflake_raw_load.sh",
        cwd=str(PROJECT_ROOT),
    )

    finish = EmptyOperator(task_id="finish")

    start >> profile_samples >> run_unit_tests
    run_unit_tests >> kafka_smoke_check >> load_snowflake_raw >> finish
