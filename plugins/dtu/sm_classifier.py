
# import matplotlib
# matplotlib.use("Agg")
# from matplotlib import pyplot as plt
# from airflow.providers.standard.operators.python import 



def _populate_result_summary(result_summary, sm_id, sm_id_results):

    # Append current sm_id to list of all SM IDs
    result_summary["All_SMs"].append(sm_id)

    # Append sm_id if has dataset and dataset contains data
    if sm_id_results["Dataset Availability"]["Available"] and sm_id_results["Dataset Availability"]["Contains Data"]:
        result_summary["SMs_with_dataset_containing_data"].append(sm_id)

    # Appends sm_id if has dataset but dataset contains no data
    if sm_id_results["Dataset Availability"]["Available"] and not sm_id_results["Dataset Availability"]["Contains Data"]:
        result_summary["SMs_with_dataset_containing_no_data"].append(sm_id)

    # Append if sm_id has no dataset
    if not sm_id_results["Dataset Availability"]["Available"]:
        result_summary["SMs_without_dataset"].append(sm_id)

    # Append if sm_id has incomplete topology info (e.g., no associated trafo id)
    if any(pd.isna(v) for v in sm_id_results["Topology"].values()):
        result_summary["SMs_with_incomplete_topology_info"].append(sm_id)

    # Check the data quality of all variables of current sm_id
    all_good = True
    any_medium_or_bad = False
    any_bad = False
    for phase, phase_data in sm_id_results["Data Quality"].items():
        for key, entry in phase_data.items():
            if isinstance(entry, dict) and "Summary" in entry:
                summary_value = entry["Summary"]
                if summary_value == "Bad":
                    any_bad = True
                    any_medium_or_bad = True
                elif summary_value == "Medium":
                    any_medium_or_bad = True
                elif summary_value != "Good":
                    all_good = False

    # Append if sm_id has only good data quality in all variables (V, ...)
    if all_good:
        result_summary["SMs_with_only_good_data_quality"].append(sm_id)

    # Append if sm_id has any variables (V, ...) with medium or bad data quality
    if any_medium_or_bad:
        result_summary["SMs_with_any_medium_or_bad_data_quality"].append(sm_id)

    # Append if sm_id has any variables (V, ...) with bad data quality
    if any_bad:
        result_summary["SMs_with_any_bad_data_quality"].append(sm_id)

    # Check how many phases are connected for current sm_id and append accordingly
    if isinstance(sm_id_results["Connectivity"]["Connected Phases"], list):
        if len(sm_id_results["Connectivity"]["Connected Phases"]) == 3:
            result_summary["SMs_with_3-phase_connection"].append(sm_id)
        if len(sm_id_results["Connectivity"]["Connected Phases"]) == 2:
            result_summary["SMs_with_2-phase_connection"].append(sm_id)
        if len(sm_id_results["Connectivity"]["Connected Phases"]) == 1:
            result_summary["SMs_with_1-phase_connection"].append(sm_id)

    # Check if any phase of sm_id has connection error (value constantly < v_lim) and then append
    if isinstance(sm_id_results["Connectivity"]["Connection Error"], list) and len(sm_id_results["Connectivity"]["Connection Error"]) > 0:
        result_summary["SMs_with_connection_error"].append(sm_id)

    # Check if any phase of sm_id has On/Off switch and then append
    if isinstance(sm_id_results["Connectivity"]["Switching Phases"], list) and len(sm_id_results["Connectivity"]["Switching Phases"]) > 0:
        result_summary["SMs_with_on_off_switch"].append(sm_id)

    return result_summary


# def _save_sm_plot(self, sm_id, sm_df, result_summary):

#     # Determine under which directories the plot has to be saved according to user settings and data characteristics
#     dirs_to_save = []

#     if self.plot_cfg["SM_selection"]["All_(with_dataset_containing_data)"] and sm_id in result_summary["SMs_with_dataset_containing_data"]:
#         dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "All_SMs_with_dataset_containing_data"))

#     if self.plot_cfg["SM_selection"]["With_only_good_data_quality"] and sm_id in result_summary["SMs_with_only_good_data_quality"]:
#         dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_only_good_data_quality"))

#     if self.plot_cfg["SM_selection"]["With_any_medium_or_bad_data_quality"] and sm_id in result_summary["SMs_with_any_medium_or_bad_data_quality"]:
#         dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_any_medium_or_bad_data_quality"))

#     if self.plot_cfg["SM_selection"]["With_1-phase_connection"] and (sm_id in result_summary["SMs_with_1-phase_connection"]):
#         dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_1-phase_connection"))

#     if self.plot_cfg["SM_selection"]["With_2-phase_connection"] and (sm_id in result_summary["SMs_with_2-phase_connection"]):
#         dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_2-phase_connection"))

#     if self.plot_cfg["SM_selection"]["With_connection_error"] and sm_id in result_summary["SMs_with_connection_error"]:
#         dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_connection_error"))

#     if self.plot_cfg["SM_selection"]["With_on_off_switch"] and sm_id in result_summary["SMs_with_on_off_switch"]:
#         dirs_to_save.append(os.path.join("Results", f"{self.run_name}", "Plots", "SMs_with_on_off_switch"))

#     # If plot belongs in None of the categories, skip plotting
#     if not dirs_to_save:
#         return

#     # Determine the rows needed for the selected variables and phases
#     rows = []
#     if "V" in self.selected_variables:
#         rows.extend([f"{self.voltage_col}{phase}" for phase in self.selected_phases])
#     if any(var in self.selected_variables for var in ["P14", "P23"]):
#         rows.extend([f"{self.active_power_p14_col[:13]}{phase}" for phase in self.selected_phases])
#     if any(var in self.selected_variables for var in ["Q12", "Q34"]):
#         rows.extend([f"{self.reactive_power_q12_col[:15]}{phase}" for phase in self.selected_phases])

#     # Set up the figure with the correct number of subplots
#     n_rows = len(rows)
#     fig, axes = plt.subplots(n_rows, 1, sharex=True, figsize=(10, 2 * n_rows))
#     if n_rows == 1:
#         axes = [axes]  # Ensure axes is iterable for a single row

#     # Plot each variable for each selected phase
#     row_index = 0
#     for row in rows:
#         phase = row.split("_")[-1]

#         # Set grid for each subplot
#         axes[row_index].grid(True, which='both', axis='both', linestyle='--', linewidth=0.5)

#         # Plot voltage if selected
#         if row.startswith(self.voltage_col) and "V" in self.selected_variables:
#             col_name = f"{self.voltage_col}{phase}_{sm_id}"
#             axes[row_index].plot(sm_df[col_name], label=col_name)
#             axes[row_index].set_ylabel("V [V]")
#             axes[row_index].set_title(f"Voltage - Phase {phase.upper()}")
#             # Set x-axis limits to data range
#             axes[row_index].set_xlim(sm_df.index.min(), sm_df.index.max())

#         # Plot active power if selected (P14 and P23 together in the same subplot)
#         elif row.startswith(self.active_power_p14_col[:13]) and any(var in self.selected_variables for var in ["P14", "P23"]):
#             if "P23" in self.selected_variables:
#                 col_name_p23 = f"{self.active_power_p23_col}{phase}_{sm_id}"
#                 axes[row_index].plot(-sm_df[col_name_p23], label="Production")
#             if "P14" in self.selected_variables:
#                 col_name_p14 = f"{self.active_power_p14_col}{phase}_{sm_id}"
#                 axes[row_index].plot(sm_df[col_name_p14], label="Consumption")
#             axes[row_index].set_ylabel("P [W]")
#             axes[row_index].set_title(f"Active Power - Phase {phase.upper()}")
#             # Set legend only if both P14 and P23 are present
#             if "P14" in self.selected_variables and "P23" in self.selected_variables:
#                 axes[row_index].legend(loc="upper right")
#             # Set x-axis limits to data range
#             axes[row_index].set_xlim(sm_df.index.min(), sm_df.index.max())

#         # Plot reactive power if selected (Q12 and Q34 together in the same subplot)
#         elif row.startswith(self.reactive_power_q12_col[:15]) and any(var in self.selected_variables for var in ["Q12", "Q34"]):
#             if "Q12" in self.selected_variables:
#                 col_name_q12 = f"{self.reactive_power_q12_col}{phase}_{sm_id}"
#                 axes[row_index].plot(sm_df[col_name_q12], label="Inductive")
#             if "Q34" in self.selected_variables:
#                 col_name_q34 = f"{self.reactive_power_q34_col}{phase}_{sm_id}"
#                 axes[row_index].plot(-sm_df[col_name_q34], label="Capacitive")
#             axes[row_index].set_ylabel("Q [Var]")
#             axes[row_index].set_title(f"Reactive Power - Phase {phase.upper()}")
#             # Set legend only if both Q12 and Q34 are present
#             if "Q12" in self.selected_variables and "Q34" in self.selected_variables:
#                 axes[row_index].legend(loc="upper right")
#             # Set x-axis limits to data range
#             axes[row_index].set_xlim(sm_df.index.min(), sm_df.index.max())

#         # Increment row index
#         row_index += 1

#     # Set the title
#     fig.text(0.5, 0.98, f"Smart Meter Data for {sm_id}", ha='center', fontsize=16, fontweight='bold')

#     # Adjust layout to prevent overlap
#     plt.tight_layout(rect=(0, 0, 1, 0.98))

#     # Save the plot to all applicable directories
#     for d in dirs_to_save:
#         _dir = os.path.join(self.dir_of_current_run_results, d)
#         os.makedirs(_dir, exist_ok=True)
#         plt.savefig(os.path.join(_dir, f'SM_{sm_id}_plot.svg'))

#     plt.close(fig)

# @staticmethod
# def progress_bar(iterable, total=None, prefix='', suffix='', length=50):
#     total = total or len(iterable)
#     for i, item in enumerate(iterable, start=1):
#         percent = i / total
#         filled_length = int(length * percent)
#         bar = '█' * filled_length + '-' * (length - filled_length)
#         print(f'\r{prefix} |{bar}| {i}/{total} {suffix}', end='', flush=True)
#         yield item  # Yield the current item to the loop
#     print()  # Newline after completion


def _meter_evaluation(sm_ids, cfg):

    import pandas as pd
    import numpy as np
    import logging
    from tqdm import tqdm
    from time import time, sleep
    from datetime import datetime
    from threephi_framework import DataExtractor
    from threephi_framework.controllers.meta import MetaController
    import threephi_framework.db.db as threephi_db

    # Initialize detailed result dict
    sm_classification_chunk = {}

    # Initialize result summary dict
    result_summary_chunk = {"All_SMs": [],
                            "SMs_with_dataset_containing_data": [],
                            "SMs_with_dataset_containing_no_data": [],
                            "SMs_without_dataset": [],
                            "SMs_with_incomplete_topology_info": [],
                            "SMs_with_only_good_data_quality": [],
                            "SMs_with_any_medium_or_bad_data_quality": [],
                            "SMs_with_any_bad_data_quality": [],
                            "SMs_with_3-phase_connection": [],
                            "SMs_with_2-phase_connection": [],
                            "SMs_with_1-phase_connection": [],
                            "SMs_with_connection_error": [],
                            "SMs_with_on_off_switch": []
                            }
    
    dataextractor = DataExtractor(phase_measurements_dir = cfg['data_dir_path'])
    metacontroller = MetaController(threephi_db.new_session)
    
    # Get the topology-SM mapping dict (if done before it returns existing one, otherwise creates new)
    # logging.info(f"Classifying Smart Meters based on {cfg['topology_processing_level']} topology")
    # if cfg['topology_processing_level'] == "raw":
    #     sm_topology_mapping = dataextractor.create_raw_sm_topology_mapping(overwrite=cfg.get('overwrite_topology_info', False), overwrite_timeseries_info=cfg.get('overwrite_timeseries_info', False))
    # elif cfg['topology_processing_level'] == "cleaned":
    #     sm_topology_mapping = dataextractor.create_cleaned_sm_topology_mapping(overwrite=cfg.get('overwrite_topology_info', False), overwrite_timeseries_info=cfg.get('overwrite_timeseries_info', False))
    # elif cfg['topology_processing_level'] == "cleaned_and_corrected":
    #     sm_topology_mapping = dataextractor.create_cleaned_and_corrected_sm_topology_mapping(overwrite=cfg.get('overwrite_topology_info', False), overwrite_timeseries_info=cfg.get('overwrite_timeseries_info', False))
    # else:
    #     err_msg = "topology_processing_level has to be 'raw', 'cleaned' or 'cleaned_and_corrected'."
    #     raise ValueError(err_msg)

    # Loop over the list of SM IDs, create results of that SM and add it to the total detailed and summary result dict
    for sm_id in tqdm(sm_ids, desc='Classifying smart meters'):


        # Initialize results dict for current sm_id
        sm_id_results = {"Topology": {"Secondary Substation ID": None, "Transformer ID": None, "Feeder ID": None, "Cabinet ID": None},
                            "Dataset Availability": {"Available": False, "Contains Data": None, "Relative Length": None, "Absolute Length": None},
                            "Data Quality": {f"L{p}": {"V": {"Summary": None , "Detailed": {"NaN frac": None,
                                                                                        "Zero frac": None,
                                                                                        "Below Vlim frac": None,
                                                                                        "Frozen frac": None,
                                                                                        "Total corruption frac": None}},
                                                "P14": {"Summary": None , "Detailed": {"NaN frac": None}},
                                                "P23": {"Summary": None , "Detailed": {"NaN frac": None}},
                                                "Q12": {"Summary": None , "Detailed": {"NaN frac": None}},
                                                "Q34": {"Summary": None , "Detailed": {"NaN frac": None}}} for p in [1,2,3]},
                            "Data Statistics": {f"L{p}": {"V": {"Min": None , "Max": None, "Mean": None, "Std": None},
                                                        "P14": {"Min": None , "Max": None, "Mean": None, "Std": None},
                                                        "P23": {"Min": None , "Max": None, "Mean": None, "Std": None},
                                                        "Q12": {"Min": None , "Max": None, "Mean": None, "Std": None},
                                                        "Q34": {"Min": None , "Max": None, "Mean": None, "Std": None}} for p in [1,2,3]},
                            "Connectivity": {"Connected Phases": None,
                                            "Connection Error": None,
                                            "Switching Phases": None}}

        # # Add the topology info to the results dict of current sm_id
        # topology = sm_topology_mapping.get(sm_id, {"Cabinet ID": np.nan, "Feeder ID": np.nan,
        #                                                 "Transformer ID": np.nan, "Secondary Substation ID": np.nan})

        # sm_id_results["Topology"] = {"Secondary Substation ID": topology["Secondary Substation ID"],
        #                                 "Transformer ID": topology["Transformer ID"],
        #                                 "Feeder ID": topology["Feeder ID"],
        #                                 "Cabinet ID": topology["Cabinet ID"],}

        # If there is a dataset for the current sm_id, evaluate it to get the remaining results

        # Load dataset for current sm_id and save it if it doesn't exist yet
        sm_df = dataextractor.v1_get_single_meter_data(sm_id)

        # Create pandas dataframe
        sm_df = sm_df.compute()

        # Get maximum recording period (sm_df all are of this length and just have nan in beginning and end)
        cfg["max_rec_period"] = len(sm_df) if cfg["max_rec_period"] is None else cfg["max_rec_period"]

        # Add the dataset availability information to the results dict of current sm_id
        sm_id_results["Dataset Availability"]["Available"] = True

        # Check if the dataset contains any meaningful data
        # if sm_df.empty or (sm_df.isna().all(axis=1)).all() or (sm_df.eq(0).all(axis=1)).all():
        #     sm_id_results["Dataset Availability"]["Contains Data"] = False
        # else:
        #     sm_id_results["Dataset Availability"]["Contains Data"] = True

        # # Add data length info to Dataset Availability
        # has_data_full_set = sm_df.notna() & (sm_df != 0)
        # has_data_full_set = has_data_full_set.any(axis=1)
        # last_valid_idx = sm_df.index.get_loc(has_data_full_set[::-1].idxmax())
        # first_valid_idx = sm_df.index.get_loc(has_data_full_set.idxmax())
        # length = last_valid_idx - first_valid_idx + 1
        # sm_id_results["Dataset Availability"]["Relative Length"] = length/cfg["max_rec_period"]
        # sm_id_results["Dataset Availability"]["Absolute Length"] = length
        has_data_full_set = (sm_df.notna() & (sm_df != 0)).any(axis=1)

        if not has_data_full_set.any():
            # no valid data
            sm_id_results["Dataset Availability"]["Contains Data"] = False
            sm_id_results["Dataset Availability"]["Relative Length"] = 0.0
            sm_id_results["Dataset Availability"]["Absolute Length"] = 0
            continue  # or handle without continue
        else:
            pos = np.flatnonzero(has_data_full_set.to_numpy())
            first_valid_idx = int(pos[0])
            last_valid_idx = int(pos[-1])
            length = last_valid_idx - first_valid_idx + 1
            sm_id_results["Dataset Availability"]["Relative Length"] = length / cfg["max_rec_period"]
            sm_id_results["Dataset Availability"]["Absolute Length"] = length
        
        # Extract part of the dataset which contains data (required for data quality assessment further down)
        sm_df_with_data = sm_df.iloc[first_valid_idx:last_valid_idx + 1]

        logging.info(f"Classifying Smart Meter {sm_id}: with columns {sm_df_with_data.columns.tolist()}")
        # Calculate nan fractions for each of the variables of the current sm_id (required for data quality assess.)
        nan_fractions = sm_df_with_data.isna().mean()

        # Overwrite nan in Connectivity with empty list since data is available. Lists are populated further down
        sm_id_results["Connectivity"]["Connected Phases"] = []
        sm_id_results["Connectivity"]["Connection Error"] = []
        sm_id_results["Connectivity"]["Switching Phases"] = []

        for phase in cfg["phases"]:

            """ 
            TODO: Instead of "voltage_", maybe use DataExtractor.voltage_col to be consistent. 
            You however need to correct {phase}, in that chase, since {phase} contain "l"+number, instead of just number
            """
            
            # Check data quality for V
            is_nan = sm_df_with_data[f"voltage_{phase}"].isna()  # which entries are nan (True/False)
            is_zero = (sm_df_with_data[f"voltage_{phase}"] == 0).fillna(False)  # which entries are 0 (True/False)
            is_below_vlim = (sm_df_with_data[f"voltage_{phase}"] < cfg["v_lim"]).fillna(False)  # which entries are below v_lim
            is_frozen = sm_df_with_data[f"voltage_{phase}"] == sm_df_with_data[f"voltage_{phase}"].shift(1)
            for i in range(2, cfg["frozen_range"]):  # which entries are frozen for frozen_range consecutive entries
                is_frozen &= sm_df_with_data[f"voltage_{phase}"] == sm_df_with_data[f"voltage_{phase}"].shift(i)
            is_frozen = is_frozen.fillna(False)
            is_corrupted = is_nan | is_zero | is_below_vlim | is_frozen  # which entries are nan,0,below lim or frozen
            has_data = ~is_nan & ~is_zero & ~is_frozen  # which entries are NOT nan and NOT 0 and NOT frozen
            offset_data = is_below_vlim & has_data  # which entries are NOT nan and NOT 0 and NOT frozen and below lim

            # Data quality assessment for V based on total corruption level
            if is_corrupted.mean() < cfg["good_data_limit"]:
                summary = "Good"
            elif cfg["medium_data_limit"] > is_corrupted.mean() >= cfg["good_data_limit"]:
                summary = "Medium"
            else:
                summary = "Bad"

            # Overwrite summary if very little data, which is considered as "not connected" instead of bad quality
            if has_data.sum() < cfg["no_data_limit"]*cfg["max_rec_period"]:
                summary = "NaN"

            # Add detailed and summarized data quality info to results dict of current sm_id
            sm_id_results["Data Quality"][phase.upper()]["V"]["Detailed"]["NaN frac"] = float(is_nan.mean())
            sm_id_results["Data Quality"][phase.upper()]["V"]["Detailed"]["Zero frac"] = float(is_zero.mean())
            sm_id_results["Data Quality"][phase.upper()]["V"]["Detailed"]["Below Vlim frac"] = float(is_below_vlim.mean())
            sm_id_results["Data Quality"][phase.upper()]["V"]["Detailed"]["Frozen frac"] = float(is_frozen.mean())
            sm_id_results["Data Quality"][phase.upper()]["V"]["Detailed"]["Total corruption frac"] = float(is_corrupted.mean())
            sm_id_results["Data Quality"][phase.upper()]["V"]["Summary"] = summary

            # Check data quality for P14, P23, Q12, Q34 (which is only nan frac at the moment)
            for variable in cfg["variables"][1:]:

                # Extract the associated column name of current variable
                col_name = [col for col in sm_df_with_data.columns if (phase in col) and (variable in col)][0]

                # Get the nan fraction for current variable from nan_fractions calculated above
                nan_frac = nan_fractions[col_name]

                # Summarize the data quality of current variable based on nan fraction
                if nan_frac < cfg["good_data_limit"]:
                    summary = "Good"
                elif cfg["medium_data_limit"] > nan_frac >= cfg["good_data_limit"]:
                    summary = "Medium"
                else:
                    summary = "Bad"

                # Overwrite summary if very little data, which is considered as "not connected" instead of bad quality
                if has_data.sum() < cfg["no_data_limit"]*cfg["max_rec_period"]:
                    summary = "NaN"

                # Add detailed and summarized data quality info to result dict of current sm_id
                sm_id_results["Data Quality"][phase.upper()][variable.upper()]["Detailed"]["NaN frac"] = float(nan_frac)
                sm_id_results["Data Quality"][phase.upper()][variable.upper()]["Summary"] = summary

            # Add the connectivity information to the results dict of current sm_id
            if has_data.sum() > cfg["no_data_limit"]*cfg["max_rec_period"]:

                # Check which phases are connected
                sm_id_results["Connectivity"]["Connected Phases"].append(phase.upper())

                # Check which phases are poorly connected and show constant voltage offset
                if offset_data.mean()/has_data.mean() > cfg["offset_threshold"]:
                    sm_id_results["Connectivity"]["Connection Error"].append(phase.upper())

                # Check which phases do switch On/Off
                has_data_pd = pd.Series(has_data)
                has_no_data_pd = pd.Series(is_zero)
                consecutive_off = has_no_data_pd.astype(int).groupby((has_no_data_pd != has_no_data_pd.shift()).cumsum()).transform('sum')
                consecutive_on = has_data_pd.astype(int).groupby((has_data_pd != has_data_pd.shift()).cumsum()).transform('sum')
                if (consecutive_on >= cfg["cons_period_threshold"]).any() & (consecutive_off >= cfg["cons_period_threshold"]).any():
                    sm_id_results["Connectivity"]["Switching Phases"].append(phase.upper())

            # Add data statistics for P14, P23, Q12, Q34 and V
            for variable in cfg["variables"]:
                col_name = [col for col in sm_df_with_data.columns if (phase in col) and (variable in col)][0]
                sm_id_results["Data Statistics"][phase.upper()][variable.upper()]["Min"] = float(val) if pd.notna(val := sm_df_with_data[col_name].min(skipna=True)) else None
                sm_id_results["Data Statistics"][phase.upper()][variable.upper()]["Max"] = float(val) if pd.notna(val := sm_df_with_data[col_name].max(skipna=True)) else None
                sm_id_results["Data Statistics"][phase.upper()][variable.upper()]["Mean"] = float(val) if pd.notna(val := sm_df_with_data[col_name].mean(skipna=True)) else None
                sm_id_results["Data Statistics"][phase.upper()][variable.upper()]["Std"] = float(val) if pd.notna(val := sm_df_with_data[col_name].std(skipna=True)) else None

        # Add results of current sm_id to the result summary dict
        # result_summary_chunk = _populate_result_summary(result_summary_chunk, sm_id, sm_id_results)  # TODO

        # TODO: Check how plotting can be implemented later
        # # Save plot on request (with option to create plots only for certain criteria like has connection error)
        # if cfg["save_plots"] and sm_id in sm_ids:
        #     _save_sm_plot(sm_id, sm_df, result_summary_chunk)

        # TODO: Check how to use the method Chris mentioned here
        # Add results of current sm_id to final detailed sm_classification results dict
        # Save results to S3
        dataextractor.s3_connector.write_json(path = f"{cfg['results_dir']}/sm_classification_{sm_id}.json", data = sm_id_results)
 
        sm_classification_chunk[str(sm_id)] = sm_id_results
        metacontroller.update_sm_characterization(meter_id = sm_id, data = sm_id_results)
    
    return sm_classification_chunk

    # return result_summary_chunk, sm_classification_chunk