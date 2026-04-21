import datetime
import logging
import os
import time

from dask.distributed import Client, get_client
from threephi_framework import DataExtractor, TopologyController
from threephi_framework.db.db import new_session
from threephi_framework.controllers.meta import MetaController
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from pandas import DataFrame

from utils import load_dag_config

PHASE_MEASUREMENTS_READY_PATH = "phase_measurements/raw"

logger = logging.getLogger()


def test_and_time_workflows():
    # Set up dask client
    try:
        get_client()
    except ValueError:
        Client("tcp://dask-scheduler:8786")

    pipeline_config = load_dag_config(__file__)

    meter_id = pipeline_config["get_single_meter"]["id"]

    data_extractor = DataExtractor(
        phase_measurements_dir=pipeline_config["data_dir_path"]
    )
    meta_controller = MetaController(new_session)
    topology_controller = TopologyController(new_session)

    """ Testing workflow to get timeseries data """
    workflow = "get_timeseries_data"
    logger.info(f"Start {workflow}")
    start_time = time.time()
    result = data_extractor.v1_get_timeseries_info()
    logger.info(f"""
    Timeseries Data:
    Earliest: {result["min_timestamp"]},
    Latest: {result["max_timestamp"]},
    List of Meter ID Length: {len(result["id_list_of_sms_with_data"])}
    """)
    logger.info(f"--- {workflow}: {time.time() - start_time} seconds ---")

    """ Get single meter data """
    workflow = "get_single_meter_data"
    logger.info(f"Start {workflow}")
    start_time = time.time()
    ddf = data_extractor.v1_get_single_meter_data(meter_id)
    logger.info(f"Dask DataFrame columns: {list(ddf.columns)}")
    logger.info(f"Number of partitions: {ddf.npartitions}")
    logger.info(f"Dtypes:\n{ddf.dtypes}")
    row_count = ddf.shape[0].compute()
    logger.info(f"Total number of rows: {row_count}")
    logger.info(f"Describe stats:\n{ddf.describe().compute()}")

    logger.info(f"--- {workflow}: {time.time() - start_time} seconds ---")

    """ Test SM Characterization Data"""
    workflow = "get_sm_characterization_data"
    logger.info(f"Start {workflow}")
    start_time = time.time()
    meter_id = 100066
    data = {
        "Topology": {
            "Secondary Substation ID": "302894",
            "Transformer ID": "31510",
            "Feeder ID": "313533",
            "Cabinet ID": "296903",
        },
        "Dataset Availability": {
            "Available": True,
            "Contains Data": True,
            "Relative Length": 0.6648469487229479,
            "Absolute Length": 17050,
        },
        "Data Quality": {
            "L1": {
                "V": {
                    "Summary": "Good",
                    "Detailed": {
                        "NaN frac": 0.0005278592375366569,
                        "Zero frac": 0.0,
                        "Below Vlim frac": 0.00093841642228739,
                        "Frozen frac": 0.004164222873900294,
                        "Total corruption frac": 0.00563049853372434,
                    },
                },
                "P14": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
                "P23": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
                "Q12": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
                "Q34": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
            },
            "L2": {
                "V": {
                    "Summary": "Good",
                    "Detailed": {
                        "NaN frac": 0.0005278592375366569,
                        "Zero frac": 0.0,
                        "Below Vlim frac": 0.0002932551319648094,
                        "Frozen frac": 0.009853372434017595,
                        "Total corruption frac": 0.010674486803519062,
                    },
                },
                "P14": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
                "P23": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
                "Q12": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
                "Q34": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
            },
            "L3": {
                "V": {
                    "Summary": "Good",
                    "Detailed": {
                        "NaN frac": 0.0005278592375366569,
                        "Zero frac": 0.0,
                        "Below Vlim frac": 0.00035190615835777126,
                        "Frozen frac": 0.007859237536656892,
                        "Total corruption frac": 0.00873900293255132,
                    },
                },
                "P14": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
                "P23": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
                "Q12": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
                "Q34": {
                    "Summary": "Good",
                    "Detailed": {"NaN frac": 0.0005278592375366569},
                },
            },
        },
        "Data Statistics": {
            "L1": {
                "V": {
                    "Min": 203.0,
                    "Max": 249.0,
                    "Mean": 234.90933227539062,
                    "Std": 10.440539360046387,
                },
                "P14": {
                    "Min": 0.0,
                    "Max": 726.0,
                    "Mean": 107.0615005493164,
                    "Std": 108.94902801513672,
                },
                "P23": {"Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0},
                "Q12": {
                    "Min": 0.0,
                    "Max": 6.0,
                    "Mean": 0.01185376476496458,
                    "Std": 0.14323671162128448,
                },
                "Q34": {
                    "Min": 21.0,
                    "Max": 161.0,
                    "Mean": 73.14940643310547,
                    "Std": 17.01614761352539,
                },
            },
            "L2": {
                "V": {
                    "Min": 206.0,
                    "Max": 249.0,
                    "Mean": 236.00721740722656,
                    "Std": 10.518004417419434,
                },
                "P14": {
                    "Min": 0.0,
                    "Max": 1216.0,
                    "Mean": 140.78135681152344,
                    "Std": 131.04580688476562,
                },
                "P23": {"Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0},
                "Q12": {
                    "Min": 0.0,
                    "Max": 210.0,
                    "Mean": 0.08878587186336517,
                    "Std": 3.6756131649017334,
                },
                "Q34": {
                    "Min": 1.0,
                    "Max": 9.0,
                    "Mean": 8.218942642211914,
                    "Std": 1.0214864015579224,
                },
            },
            "L3": {
                "V": {
                    "Min": 205.0,
                    "Max": 250.0,
                    "Mean": 236.0614471435547,
                    "Std": 10.227823257446289,
                },
                "P14": {"Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0},
                "P23": {"Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0},
                "Q12": {"Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0},
                "Q34": {
                    "Min": 3.0,
                    "Max": 7.0,
                    "Mean": 6.081039905548096,
                    "Std": 0.688140869140625,
                },
            },
        },
        "Connectivity": {
            "Connected Phases": ["L1", "L2", "L3"],
            "Connection Error": [],
            "Switching Phases": [],
        },
    }
    meta_controller.update_sm_characterization(meter_id, data)
    sm_char = meta_controller.get_sm_characterization(meter_id)
    logger.info(sm_char)

    logger.info(f"--- {workflow}: {time.time() - start_time} seconds ---")

    """ Testing workflow to export topology """
    workflow = "export_topology"
    logger.info(f"Start {workflow}")
    start_time = time.time()
    topology: DataFrame = topology_controller.export_topology()  # type: ignore
    sm_cab: DataFrame = topology_controller.export_sm_cabinet()  # type: ignore

    topology.to_csv(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "data",
            "lv_topology_export.csv",
        ),
        index=False,
    )
    topology["transformer_capacity"] = topology["transformer_capacity"].astype("Int64")
    topology["lv_feeder_fuse_size"] = topology["lv_feeder_fuse_size"].astype("Int64")
    topology["phase_size"] = topology["phase_size"].astype("Int64")
    topology["cable_capacity"] = topology["cable_capacity"].astype("Int64")

    sm_cab.loc[sm_cab["cabinet"].notna(), "lv_feeder"] = None
    sm_cab.loc[sm_cab["lv_feeder"].notna(), "cabinet"] = None
    sm_cab["service_fuse_size"] = sm_cab["service_fuse_size"].astype("Int64")
    sm_cab.to_csv(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "data",
            "meter_cabinet_connection_export.csv",
        ),
        index=False,
    )

    logger.info(f"--- {workflow}: {time.time() - start_time} seconds ---")


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
    dag_id="test_and_time_workflows",
    description="DAG to test and time workflows.",
    default_args=default_args,
    start_date=datetime.datetime.now(),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
) as dag:
    test_task = PythonOperator(
        task_id="test_workflows",
        python_callable=test_and_time_workflows,
    )

    test_task
