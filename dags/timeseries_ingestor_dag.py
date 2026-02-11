import datetime
import os
from pathlib import Path

from threephi_framework import TimeseriesIngestor
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import yaml


def timeseries_ingestion():
    config_name = Path(__file__).stem
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs", f"{config_name}_config.yaml")) as file:
        config = yaml.safe_load(file)
    with TimeseriesIngestor(config) as app:
        app.run()

# Default DAG args
default_args = {
    'owner': 'inilab',
    'retries': 0,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define DAG
with DAG(
        dag_id='TimeseriesIngestorDAG',
        description='Ingest Timeseries Data from CSV to partitioned parquet file storage',
        default_args=default_args,
        start_date=datetime.datetime.now(),
        catchup=False,
        max_active_runs=1,  # Prevent concurrent runs, protect from Data inconsistencies
) as dag:

    timeseries_ingest_task = PythonOperator(
        task_id='load_meter_data',
        python_callable=timeseries_ingestion,
        doc_md="""
        ## CSV --> Partitioned Parquet
        
        Load CSV files and transform into partitioned parquet files.
        
        **Input**: CSV files matching pattern set in config, e.g.: 'phase_measurements_*.csv'
        **Output**: Parquet files on bucket storage
        **Idempotent**: Yes, uses workflow tracking to prevent duplicate runs (unless manual override is used)
        """
    )

    timeseries_ingest_task