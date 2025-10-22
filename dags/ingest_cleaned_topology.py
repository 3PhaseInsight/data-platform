from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dask.distributed import Client, get_client
import threephi_framework.db.db as threephi_db
from threephi_framework.controllers.topology import TopologyController

def ingest_topology():
    # Set up dask client
    try:
        get_client()
    except ValueError:
        Client("tcp://dask-scheduler:8786")

    topology_controller = TopologyController(threephi_db.new_session)
    # read and clean topology
    topology_ddf = topology_controller.read_topology("/opt/airflow/data/lv_topology.csv")
    # topology_ddf = ingestor.clean_topology_dask(topology_ddf)
    # read and clean sm_cab
    sm_cab_ddf = topology_controller.read_sm_cab("/opt/airflow/data/meter_cabinet_connection.csv")
    # sm_cab_ddf = ingestor.clean_meter_and_cabinet_connection_dask(sm_cab_ddf)
    # store in DB
    topology_controller.ingest(topology_ddf, sm_cab_ddf)

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