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
        _main_path (Path): Main directory path.
        _logs_directory (Path): Logs directory path.
        _transform_export_directory (Path): Transformed data directory path.
        _export_directory (Path): Output directory path.
        _timestamp (str): Timestamp when the configuration was loaded.
        _run_name (str): Name of the run.
        _run_etl (bool): Flag to run ETL.
        _run_solver (bool): Flag to run solver.
        _run_post_process (bool): Flag to run post process.
        _export_etl (bool): Flag to export ETL.
        _export_solution (bool): Flag to export solution.
        _export_post_process (bool): Flag to export post process.
        _logger_debug (bool): Debug flag for logging.
        _agent_model (str): Model for the AI parser.
        _agent_api_key (str): API key for the agent.
        _agent_temperature (float): Temperature for the AI parser.
        _agent_max_tokens (int): Max tokens for the AI parser.
        _google_credentials_json (str): Path to the Google service account JSON.
        _google_drive_scopes (list): Scopes for Google Drive.
        _google_gmail_scopes (list): Scopes for Gmail.
        _gmail_user_email (str): Gmail user email for delegation.
        _documentai_project_id (str): Google project ID for Document AI.
        _documentai_location (str): Location for Document AI.
        _documentai_processor_id (str): Processor ID for Document AI.
        _google_sheets_scopes (list): Scopes for Google Sheets.
        _sheet_id (str): Google Sheet ID.
        _sage_api_key (str): API key for Sage.
        _sage_endpoint (str): Endpoint URL for Sage.
        _api_clients (dict): Additional API clients parameters.
        _streamlit (bool): Flag indicating if se usa streamlit.
        _scenario_name (str): Scenario name with timestamp.
    """

    def __init__(self, streamlit: bool = False):
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
        self._transform_export_directory = self._main_path / self.get_value("directories.transform_export_directory")
        self._logs_directory = self._main_path / self.get_value("directories.logs_directory")
        self._export_directory = self._main_path / self.get_value("directories.export_directory")

        self._run_name = self.get_value('execution.run_name')
        self._run_etl = self.get_value('execution.run_etl')
        self._run_solver = self.get_value('execution.run_solver')

        self._export_etl = self.get_value('export.etl')
        self._export_solution = self.get_value('export.solution')

        self._logger_debug = self.get_value('logger.debug')

        # Parámetros para el ai_parser
        self._agent_model = self.get_value('ai_parser.agent.model')
        self._agent_api_key = os.getenv("OPENAI_API_KEY")
        self._agent_temperature = self.get_value('ai_parser.agent.temperature')
        self._agent_max_tokens = self.get_value('ai_parser.agent.max_tokens')

        # Parámetros ETL
        self._google_credentials_json= self._data_directory /self.get_value("etl.google.credentials_json")
        self._google_drive_scopes = self.get_value("etl.google.drive.scopes")
        self._google_drive_folder_id = self.get_value("etl.google.drive.drive_folder_id")
        self._google_image_folder_id = self.get_value("etl.google.drive.image_folder_id")
        self._google_gmail_scopes = self.get_value("etl.google.gmail.scopes")
        self._google_gmail_client_secret_file =self._data_directory / self.get_value("etl.google.gmail.client_secret_file")
        self._google_gmail_token_file = self._data_directory / self.get_value("etl.google.gmail.gmail_token_file")
        self._google_gmail_save_pdf_attachments_folder = self.get_value("etl.google.gmail.save_pdf_attachments_folder")
        self._google_gmail_start_date = self.get_value("etl.google.gmail.start_date")
        self._google_gmail_label = self.get_value("etl.google.gmail.label")

        self._auto_claim_canal = self.get_value("etl.google.gmail.auto_claim_canal")

        self._gmail_user_email = self.get_value("etl.google.gmail.gmail_user_email")
        self._documentai_project_id = self.get_value("etl.google.documentai.project_id")
        self._documentai_location = self.get_value("etl.google.documentai.location")
        self._documentai_processor_id = self.get_value("etl.google.documentai.processor_id")
        self._google_sheets_scopes = self.get_value("etl.google.sheets.scopes")
        self._sheet_id = self.get_value("etl.google.sheets.sheet_id")
        self._sheet_name_registro = self.get_value("etl.google.sheets.sheet_name_registro")
        self._sage_api_key = self.get_value("etl.sage.api_key")
        self._sage_endpoint = self.get_value("etl.sage.endpoint")

        self._api_clients = self.get_value("api_clients.param_group")

        self._streamlit = streamlit
        self._scenario_name = f"{self.get_value('directories.scenario_name')}_{self._timestamp}"

    @staticmethod
    def get_project_root() -> Path:
        return Path(__file__).parent.parent

    def get_value(self, key: str) -> Any:
        keys = key.split('.')
        data = self._config
        for k in keys:
            if k in data:
                data = data[k]
            else:
                return None
        return data

    def read_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_filepath, 'r') as config_file:
                return json.load(config_file)
        except FileNotFoundError:
            print("Config file not found. Make sure it exists at the specified path.")
            return {}

    @property
    def main_path(self):
        return self._main_path

    @property
    def data_directory(self):
        return self._data_directory

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
    def logger_debug(self):
        return self._logger_debug

    @property
    def export_etl(self):
        return self._export_etl

    @property
    def export_solution(self):
        return self._export_solution

    @property
    def agent_model(self):
        return self._agent_model

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
    def google_credentials_json(self):
        return self._google_credentials_json

    @property
    def google_drive_scopes(self):
        return self._google_drive_scopes

    @property
    def google_drive_folder_id(self):
        return self._google_drive_folder_id

    @property
    def google_image_folder_id(self):
        return self._google_image_folder_id

    @property
    def google_gmail_scopes(self):
        return self._google_gmail_scopes

    @property
    def google_gmail_client_secret_file(self):
        return self._google_gmail_client_secret_file

    @property
    def google_gmail_token_file(self):
        return self._google_gmail_token_file

    @property
    def gmail_save_pdf_attachments_folder(self):
        return self._google_gmail_save_pdf_attachments_folder

    @property
    def gmail_start_date(self):
        return self._google_gmail_start_date

    @property
    def gmail_label(self):
        return self._google_gmail_label

    @property
    def auto_claim_canal(self):
        return self._auto_claim_canal

    @property
    def gmail_user_email(self):
        return self._gmail_user_email

    @property
    def documentai_project_id(self):
        return self._documentai_project_id

    @property
    def documentai_location(self):
        return self._documentai_location

    @property
    def documentai_processor_id(self):
        return self._documentai_processor_id

    @property
    def google_sheets_scopes(self):
        return self._google_sheets_scopes

    @property
    def sheet_id(self):
        return self._sheet_id

    @property
    def sheet_name_registro(self):
        return self._sheet_name_registro

    @property
    def sage_api_key(self):
        return self._sage_api_key

    @property
    def sage_endpoint(self):
        return self._sage_endpoint

    @property
    def api_clients(self):
        return self._api_clients

    @property
    def streamlit(self):
        return self._streamlit
