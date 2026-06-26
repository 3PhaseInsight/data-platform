import os
import re
import sys
import json
import yaml
import logging
from time import time
import numpy as np
import pandas as pd
import importlib

from dask import delayed, compute
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import threephi_framework.db.db as threephi_db
from threephi_framework import TopologyController
from threephi_framework import TimeSeriesController
from threephi_framework import MetaController
from threephi_framework.object_storage.s3_connector import S3Connector

from threephi_framework import StatLabeler
from threephi_framework import ElectricHeatingIdentifier
from threephi_framework import PhaseConnector

from utils import load_dag_config



class PhaseConnectorService():

    def __init__(self, config):
        self.config = config
        self.topology_processing_level = config.get("topology_processing_level", "raw")
        self.sm_id = config.get("sm_id", None)
        self.profile_processing_level = config.get("profile_processing_level", "raw")
        self.save_results = config.get("save_results", False)
        self.topology_controller = TopologyController(threephi_db.new_session)
        self.data_dir_path = config.get("data_dir_path", "phase_measurements")
        self.s3_connector = S3Connector(data_dir_path=self.data_dir_path)
        self.timeseries_controller = TimeSeriesController(self.s3_connector)
        self.meta_controller = MetaController(threephi_db.new_session)
        self.ML_algorithm = config.get("ML_algorithm", "label_propagation")

        # Phase scoring settings — all parameters come from config
        ps = config.get('phase_scoring', {})
        self.appliance_type   = ps.get('appliance_type', 'hp')
        self.label_columns    = ps.get('label_columns', {
            'hp': 'predicted_hp',
            'ev': 'predicted_ev_phase',
            'pv': 'predicted_pv',
        })
        self.weights = ps.get('weights', {
            'C1_feeder_balance':    40,
            'C2_type_concentration': 30,
            'C3_household_balance': 30,
        })

        # Internal state
        self.feeder_labels_by_feeder = None
        self.labels_for_used_sm_ids  = None
        self.current_feeder_id       = None
        self._sm_ids_of_feeder       = None

    def _validate_config(self):
        if self.appliance_type not in {'hp', 'ev', 'pv'}:
            raise ValueError(f"Invalid appliance_type: {self.appliance_type}. Must be one of 'hp', 'ev', 'pv'.")

        if not isinstance(self.weights, dict) or not all(k in self.weights for k in ['C1_feeder_balance', 'C2_type_concentration', 'C3_household_balance']):
            raise ValueError("Weights must be a dict with keys: 'C1_feeder_balance', 'C2_type_concentration', 'C3_household_balance'.")

        if self.sm_id is None:
            raise ValueError("No smart meter ID provided.")
        elif isinstance(self.sm_id, int):
            logging.warning("sm_id provided as a integer. Converting to a string for processing.")
            self.sm_id = str(self.sm_id)
        # If sm_ids is a list of integers, convert them to strings
        elif isinstance(self.sm_id, list):
            logging.warning("sm_ids provided as a list. Converting to a strings.")
            self.sm_id = str(self.sm_id)
        # Or if there are multiple entries in a list, tuple, or set, then select only the first one and log a warning
        elif isinstance(self.sm_id, (list, tuple, set)) and len(self.sm_id) > 1:
            logging.warning("Multiple sm_ids provided. Only the first one will be processed.")
            self.sm_id = str(list(self.sm_id)[0])
        

    def _find_feeder_sm_ids(self) -> pd.DataFrame:
        """Load feeder time-series data. Label loading is handled separately via _load_labels()."""

        chain = self.topology_controller.get_topology_chain_for_meter(int(self.sm_id))
        feeder_id = chain[0].feeder_id if chain else None

        if feeder_id is None:
            raise ValueError(f"Could not determine feeder for SM {self.sm_id}. Check topology mapping.")
        self.current_feeder_id  = feeder_id

        meters_dict= self.topology_controller.get_meters_for_node(node_id=feeder_id, node_type='lv_feeder')
        
        meters_for_feeder = [str(m['id']) for m in meters_dict]

        return meters_for_feeder

    def recommend_phase_for_installing(self): #TODO: add types of appliances as an input, like: (self, appliance_type=None)


        self._validate_config()

        self.sm_ids = self._find_feeder_sm_ids()

        # Run StatLabeler to ensure ground truths
        # Modify the self.sm_ids in the StatLabeler to only include the sm_ids of the feeder
        stat_labeler_cfg = {
            **self.config,                          # top-level keys (dask, data_dir_path, temp_data_path, etc.)
            **self.config.get("StatLabeler", {}),   # StatLabeler section overrides/adds on top
            "sm_ids": self.sm_ids,                  # always override sm_ids with feeder meters
        }

        with StatLabeler(config=stat_labeler_cfg) as stat_labeler:
            stat_labeler.stat_label_sm()

        ehi_cfg = {
            **self.config,                          # top-level keys (dask, data_dir_path, temp_data_path, etc.)
            **self.config.get("EH_identifier", {}),   # StatLabeler section overrides/adds on top
            "sm_ids": self.sm_ids,                  # always override sm_ids with feeder meters
        }

        with ElectricHeatingIdentifier(config=ehi_cfg) as electric_heating_identifier:
            electric_heating_identifier.identify_EH_applicances()

        phase_connector_cfg = {
            **self.config,
            **self.config.get("PhaseConnector", {}),
            "sm_ids": self.sm_id,
            "HP_ML_algorithm": ehi_cfg["ML_algorithm"],
        }

        with PhaseConnector(config=phase_connector_cfg) as phase_connector:
            results = phase_connector.identify_optimal_phase_connection()

        logging.info("Phase connection service run completed.")
        logging.info(f"Recommended phase connection for SM {self.sm_id}: {results}")
    
    def run(self):
        self.recommend_phase_for_installing()


def run_recommend_phase():
    app = PhaseConnectorService(config=load_dag_config(__file__))
    app.run()

default_args = {
    "owner": "inilab",
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="phase_connector",
    description="Recommend phase allocation of new electric appliances for Smart Meters",
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="Recommend_Phase",
        python_callable=run_recommend_phase,
        doc_md="## Recommend Phase DAG",
    )
