import logging
import os
import yaml

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

import threephi_framework.db.db as threephi_db
from threephi_framework import TopologyController
from threephi_framework.resources.topology.assets.meter import MeterResource
from threephi_framework.data_apps.base import BaseDataApp


class TopologyTester(BaseDataApp):
    def __init__(self, config):
        super().__init__(config)
        self.substation_id = config["substation_id"]
        self.delivery_point_id = config["delivery_point_id"]
        self.cabinet_id = config["cabinet_id"]
        self.feeder_id = config["feeder_id"]
        self.topology_controller = TopologyController(threephi_db.new_session)

    def run(self):
        # Test get_meters_for_substation
        meters = self.topology_controller.get_meters_for_substation(self.substation_id)
        logging.info(f"Meters for substation {self.substation_id}: {meters}")

        # Test get_meters
        meters = self.topology_controller.get_meters(True, True)
        logging.info(f"get_meters(has_heat_pump=True, has_solar_panel=True): {meters}")


        # Test get_meters_for_node (delivery_point)
        meters = self.topology_controller.get_meters_for_node(self.delivery_point_id, "delivery_point")
        logging.info(f"get_meters_for_node(node_id={self.delivery_point_id}, \"delivery_point\"): {meters}")

        # Test get_meters_for_node (cabinet)
        meters = self.topology_controller.get_meters_for_node(self.cabinet_id, "cabinet")
        logging.info(f"get_meters_for_node(node_id={self.cabinet_id}, \"cabinet\"): {meters}")

        # Test get_meters_for_node (feeder)
        meters = self.topology_controller.get_meters_for_node(self.feeder_id, "lv_feeder")
        logging.info(f"get_meters_for_node(node_id={self.feeder_id}, \"lv_feeder\"): {meters}")


config_file = "test_topology.yaml"
with open(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs", config_file),
    "r",
) as file:
    pipeline_config = yaml.safe_load(file)
    with TopologyTester(pipeline_config) as app:
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
            dag_id="test_topology",
            description="Test Topology Data",
            default_args=default_args,
            start_date=datetime.now(),
            catchup=False,
            max_active_runs=1,  # Prevent concurrent runs, protect from DB inconsistencies
        ) as dag:
            test_topology_task = PythonOperator(
                task_id="test_topology",
                python_callable=app.run,
                doc_md="""
                ## Test Topology
                """,
            )

            test_topology_task
