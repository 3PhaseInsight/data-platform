import datetime
import os

import yaml
from dask.distributed import Client, get_client
from threephi_framework import DataExtractor
from airflow import DAG
from airflow.operators.python import PythonOperator

PHASE_MEASUREMENTS_READY_PATH = "phase_measurements/raw"

def csv_to_parquet():
    # Set up dask client
    try:
        get_client()
    except ValueError:
        Client("tcp://dask-scheduler:8786")

    config_file = "csv_to_parquet_config_partitioned.yaml"

    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', config_file), 'r') as file:
        pipeline_config = yaml.safe_load(file)

    override = pipeline_config["override"]

    data_extractor = DataExtractor()

    workflow = "timeseries_csv_to_parquet_partitions"
    if not data_extractor.db_connector.is_workflow_completed(workflow) and override == False:
        data_extractor.v1_csv_to_parquet_partitions(
            csv_path = "/opt/airflow/data",
            csv_file_pattern = "phase_measurements_*.csv",
            bucket_dest_path = f"{data_extractor.s3_base}/{PHASE_MEASUREMENTS_READY_PATH}",
        )
    data_extractor.db_connector.complete_workflow(workflow)


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
        dag_id='csv_to_partitioned_parquet',
        description='Ingest Timeseries Data from CSV to partitioned parquet file storage',
        default_args=default_args,
        start_date=datetime.datetime.now(),
        catchup=False,
        max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
) as dag:

    csv_ingest_task = PythonOperator(
        task_id='load_meter_data',
        python_callable=csv_to_parquet,
        provide_context=True,
        doc_md="""
        ## CSV --> Partitioned Parquet
        
        Load CSV files and transform into partitioned parquet files..
        
        **Input**: CSV files matching pattern `phase_measurements_*.csv`
        **Output**: Parquet files on bucket storage
        **Idempotent**: Yes, uses workflow tracking to prevent duplicate runs (unless manual override is used)
        """
    )

    csv_ingest_task