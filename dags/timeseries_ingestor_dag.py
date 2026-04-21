import datetime

from utils import load_dag_config

from threephi_framework import TimeseriesIngestor
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def timeseries_ingestion():
    config = load_dag_config(__file__)
    with TimeseriesIngestor(config) as app:
        app.run()


# Default DAG args
default_args = {
    "owner": "inilab",
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

# Define DAG
with DAG(
    dag_id="TimeseriesIngestorDAG",
    description="Ingest Timeseries Data from CSV to partitioned parquet file storage",
    default_args=default_args,
    start_date=datetime.datetime.now(),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs, protect from Data inconsistencies
) as dag:
    timeseries_ingest_task = PythonOperator(
        task_id="load_meter_data",
        python_callable=timeseries_ingestion,
        doc_md="""
        ## CSV --> Partitioned Parquet
        
        Load CSV files and transform into partitioned parquet files.
        
        **Input**: CSV files matching pattern set in config, e.g.: 'phase_measurements_*.csv'
        **Output**: Parquet files on bucket storage
        **Idempotent**: Yes, uses workflow tracking to prevent duplicate runs (unless manual override is used)
        """,
    )

    timeseries_ingest_task
