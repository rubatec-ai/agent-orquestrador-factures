import getpass
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from dotenv import load_dotenv
import streamlit as st


class ConfigurationManager:
    """
    Manages execution configurations.

    Attributes:
        _config (dict): Execution configuration.
        _main_path (str): Path to the main directory for data.
        _data_directory (str): Path to the data directory.
        _logs_directory (str): Path to the logs directory.
        _export_directory (str): Path to the export directory.
        _transform_export_directory (str): Path to the transformed data directory.
        _data_control_filepath (str): Path to the data control file.
        _data_control_sources (str): Tab name for data control sources.
        _data_control_fields (str): Tab name for data control fields.
    """

    def __init__(self, streamlit: bool = False):
        # Load environment variables from .env
        load_dotenv()

        root_path = self.get_project_root()

        if streamlit:
            self.config_filepath = st.session_state["json_file_path"]
        else:
            self.config_filepath = os.path.join(root_path, 'config/config.json')

        self._timestamp = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")

        self._config = self.read_config()

        base_path = Path("C:/Users") / getpass.getuser()

        self._main_path = base_path / self.get_value("directories.main_path")
        self._data_directory = self._main_path / self.get_value("directories.data_directory")
        self._pdf_directory = self._data_directory  / 'pdf'
        self._transform_export_directory = self._main_path / self.get_value("directories.transform_export_directory")
        self._logs_directory = self._main_path / self.get_value("directories.logs_directory")
        self._export_directory = self._main_path / self.get_value("directories.export_directory")

        self._data_control_filepath = self._data_directory / self.get_value('data_control.filename')
        self._data_control_sources = self.get_value('data_control.sources_tab')
        self._data_control_fields = self.get_value('data_control.fields_tab')

        self._run_name = self.get_value('execution.run_name')
        self._run_etl = self.get_value('execution.run_etl')
        self._run_solver = self.get_value('execution.run_solver')
        self._run_post_process = self.get_value('execution.run_post_process')

        self._export_etl = self.get_value('export.etl')
        self._export_solution = self.get_value('export.solution')
        self._export_post_process = self.get_value('export.post_process')

        self._logger_debug = self.get_value('logger.debug')

        self._etl_param_use_pdf_from_directory = self.get_value('etl_param.pdf_from_directory')
        self._etl_param_use_debug_scope = self.get_value('etl_param.use_debug_scope')
        self._etl_param_use_default_param = self.get_value('etl_param.use_default_param')
        self._etl_param_use_tables = self.get_value('etl_param.use_tables')
        self._etl_param_chunk_tokens = self.get_value('etl_param.chunk_tokens')
        self._etl_param_min_chunk_tokens = self.get_value('etl_param.min_chunk_tokens')
        self._etl_param_min_paragraph_logging = self.get_value('etl_param.min_paragraph_logging')
        self._etl_param_embedding_model = self.get_value('etl_param.embedding_model')
        self._etl_param_embedding_tokens = self.get_value('etl_param.embedding_tokens')

        self._agent_model = self.get_value('ai_summarizer.agent.model')
        self._agent_api_key = os.getenv("OPENAI_API_KEY")
        self._agent_temperature = self.get_value('ai_summarizer.agent.temperature')
        self._agent_max_tokens = self.get_value('ai_summarizer.agent.max_tokens')

        self._semantic_search_cosine_similarity_filter = self.get_value(
            'ai_summarizer.semantic_search.cosine_similarity_filter')
        self._semantic_search_similarity_threshold = self.get_value(
            'ai_summarizer.semantic_search.similarity_threshold')
        self._semantic_search_topn_similar = self.get_value('ai_summarizer.semantic_search.topn_similar')
        self._semantic_search_all_against_all = self.get_value('ai_summarizer.semantic_search.all_against_all')

        self._streamlit = streamlit

        self._scenario_name = f"{self.get_value('directories.scenario_name')}_{self._timestamp}"

    @staticmethod
    def get_project_root() -> Path:
        """Retrieve the root directory of the project."""
        return Path(__file__).parent.parent

    def get_value(self, key: str) -> Any:
        """
        Retrieve a value from the configuration using dot notation.

        Args:
            key (str): Dot-separated key to access nested configuration values.

        Returns:
            Any: The value from the configuration file, or None if not found.
        """
        keys = key.split('.')
        data = self._config
        for k in keys:
            if k in data:
                data = data[k]
            else:
                return None
        return data

    def read_config(self) -> Dict[str, Any]:
        """
        Read a configuration file containing configuration parameters.

        Returns:
            dict: configuration
        """
        try:
            with open(self.config_filepath, 'r') as config_file:
                return json.load(config_file)
        except FileNotFoundError:
            print("Config file not found. Make sure it exists at the specified path.")
            return {}

    # Properties for accessing configuration attributes
    @property
    def main_path(self):
        return self._main_path

    @property
    def data_directory(self):
        return self._data_directory

    @property
    def pdf_directory(self):
        return self._pdf_directory

    @property
    def transform_export_directory(self):
        return self._transform_export_directory

    @property
    def logs_directory(self):
        return self._logs_directory

    @property
    def export_directory(self):
        return self._export_directory

    @property
    def data_control_filepath(self):
        return self._data_control_filepath

    @property
    def data_control_sources(self):
        return self._data_control_sources

    @property
    def data_control_fields(self):
        return self._data_control_fields

    @property
    def scenario_name(self):
        return self._scenario_name

    @property
    def run_name(self):
        return self._run_name

    @property
    def run_etl(self):
        return self._run_etl

    @property
    def run_solver(self):
        return self._run_solver

    @property
    def run_post_process(self):
        return self._run_post_process

    @property
    def logger_debug(self):
        return self._logger_debug

    @property
    def export_etl(self):
        return self._export_etl

    @property
    def export_solution(self):
        return self._export_solution

    @property
    def export_post_process(self):
        return self._export_post_process

    @property
    def etl_param_use_pdf_from_directory(self):
        return self._etl_param_use_pdf_from_directory

    @property
    def etl_param_use_debug_scope(self):
        return self._etl_param_use_debug_scope

    @property
    def etl_param_use_default_param(self):
        return self._etl_param_use_default_param

    @property
    def etl_param_use_tables(self):
        return self._etl_param_use_tables

    @property
    def etl_param_chunk_tokens(self):
        return self._etl_param_chunk_tokens

    @property
    def etl_param_min_chunk_tokens(self):
        return self._etl_param_min_chunk_tokens

    @property
    def etl_param_min_paragraph_logging(self):
        return self._etl_param_min_paragraph_logging

    @property
    def etl_param_embedding_model(self):
        return self._etl_param_embedding_model

    @property
    def etl_param_embedding_tokens(self):
        return self._etl_param_embedding_tokens

    @property
    def agent_model(self):
        return self._agent_model

    @property
    def agent_api_key(self):
        return self._agent_api_key

    @property
    def agent_temperature(self):
        return self._agent_temperature

    @property
    def agent_max_tokens(self):
        return self._agent_max_tokens

    @property
    def semantic_search_cosine_similarity_filter(self):
        return self._semantic_search_cosine_similarity_filter

    @property
    def semantic_search_similarity_threshold(self):
        return self._semantic_search_similarity_threshold

    @property
    def semantic_search_topn_similar(self):
        return self._semantic_search_topn_similar

    @property
    def semantic_search_all_against_all(self):
        return self._semantic_search_all_against_all

    @property
    def streamlit(self):
        return self._streamlit
