from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dask.distributed import Client, get_client
from threephi_framework import TopologyIngestor

def ingest_topology():
    # Set up dask client
    try:
        get_client()
    except ValueError:
        Client("tcp://dask-scheduler:8786")
    ingestor = TopologyIngestor(
        "/opt/airflow/data/lv_topology.csv",
        "/opt/airflow/data/meter_cabinet_connection.csv"
    )
    # read and clean topology
    topology_ddf = ingestor.read_topology()
    topology_ddf = ingestor.clean_topology_dask(topology_ddf)
    # read and clean sm_cab
    sm_cab_ddf = ingestor.read_sm_cab()
    sm_cab_ddf = ingestor.clean_meter_and_cabinet_connection_dask(sm_cab_ddf)
    # store in DB
    ingestor.topology_to_db(topology_ddf, sm_cab_ddf)

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
        dag_id='ingest_topology',
        description='Ingest Timeseries Data from CSV to partitioned parquet file storage',
        default_args=default_args,
        start_date=datetime.now(),
        catchup=False,
        max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
) as dag:

    ingest_topology_task = PythonOperator(
        task_id='ingest_topology',
        python_callable=ingest_topology,
        doc_md="""
        ## Ingest Topology
        
        Load Topology Info from CSV files, clean it and insert it into the DB.
        
        **Input**: CSV files `lv_topology.csv` and `meter_cabinet_connection.csv`
        **Output**: -
        **Idempotent**: No, every run will create a new version in the DB.
        """
    )

    ingest_topology_task