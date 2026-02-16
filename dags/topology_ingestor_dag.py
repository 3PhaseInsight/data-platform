from datetime import datetime
import logging
import os
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import yaml

from threephi_framework import TopologyIngestor

def topology_ingestion():
    logging.info("Updated DAG")
    config_name = Path(__file__).stem
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs", f"{config_name}_config.yaml")) as file:
        config = yaml.safe_load(file)
    with TopologyIngestor(config) as app:
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
        dag_id='TopologyIngestorDAG',
        description='Ingest Timeseries Data from CSV to partitioned parquet file storage',
        default_args=default_args,
        start_date=datetime.now(),
        catchup=False,
        max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
) as dag:

    ingest_topology_task = PythonOperator(
        task_id='ingest_topology',
        python_callable=topology_ingestion,
        doc_md="""
        ## Ingest Topology
        
        Load Topology Info from CSV files, clean it and insert it into the DB.
        
        **Input**: CSV files `lv_topology.csv` and `meter_cabinet_connection.csv`
        **Output**: -
        **Idempotent**: No, every run will create a new version in the DB.
        """
    )

    ingest_topology_task