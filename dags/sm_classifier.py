from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from threephi_framework import SMClassifier
from utils import load_dag_config


def run_sm_classifier():
    with SMClassifier(config=load_dag_config(__file__)) as app:
        app.run()


default_args = {
    "owner": "inilab",
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="sm_classifier",
    description="Classify Smart Meters based on topology and data quality",
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
) as dag:
    PythonOperator(
        task_id="Classify_Smart_Meters",
        python_callable=run_sm_classifier,
        doc_md="""
        ## Classify Smart Meters

        Evaluates per-meter data quality, statistics, and connectivity and writes the
        results to the `meta.meter` JSONB columns.

        Implemented by `threephi_framework.SMClassifier`; configured via
        `configs/sm_classifier_config.yaml`.
        """,
    )
