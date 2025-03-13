import json
import os

import pandas as pd
import streamlit as st


def load_json(file_path):
    """
     Function to load JSON file
    """
    with open(file_path, 'r') as f:
        return json.load(f)


def save_json(data, file_path):
    """
    Function to save JSON file
    """
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)


def load_dfs_with_progress_bar(results_input_path, scenario=''):
    """
    Loads all dataframes with a progress bar
    Args:
        results_input_path
        scenario
    """
    progress_bar = st.sidebar.progress(0, text="")
    result_files = [filename.replace(".csv", "") for filename in os.listdir(results_input_path) if ".csv" in filename]
    loaded_dfs = dict()
    for i, file_name in enumerate(result_files):
        percent_loaded = i / len(result_files)
        progress_bar.progress(percent_loaded, text=f"Loading {file_name}...")
        file_path = os.path.join(results_input_path, file_name)
        loaded_dfs[file_name] = pd.read_csv(file_path + ".csv", header=0)
    progress_bar.empty()

    st.session_state[f"loaded_dfs{scenario}"] = loaded_dfs
