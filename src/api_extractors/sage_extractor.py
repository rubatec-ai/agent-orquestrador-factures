from typing import Dict
import pandas as pd
from logging import Logger
from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor


class GSheetsExtractor(BaseExtractor):
    """
    Extracts and processes data from the Google Sheets API using service account credentials.

    This class loads credentials from a JSON file, creates a Sheets API client,
    fetches raw data from a specific Google Sheet, and cleans it into a DataFrame.
    """

    def __init__(self, config: ConfigurationManager, logger: Logger) -> None:
        self._credentials_path = config.google_credentials_json
        self._scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path, scopes=self._scopes
        )
        self._service = build('sheets', 'v4', credentials=credentials)
        self._sheet_id = config.sheet_id  # Google Sheet ID.
        logger.name = "GSheetsExtractor"
        super().__init__(config, logger)

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetches raw data from a Google Sheet using the Sheets API.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary containing raw Sheets data under the key "sheet_data".
        """
        try:
            # Define the range to fetch. Adjust the range as needed.
            range_name = 'Sheet1!A:Z'
            result = self._service.spreadsheets().values().get(
                spreadsheetId=self._sheet_id,
                range=range_name
            ).execute()
            values = result.get('values', [])
            if values:
                header = values[0]
                data = values[1:]
                df = pd.DataFrame(data, columns=header)
            else:
                df = pd.DataFrame()
            return {"sheet_data": df}
        except Exception as e:
            raise Exception(f"Failed to fetch Sheets data: {str(e)}")

    def clean_input_data(self):
        """
        Processes and cleans the raw Sheets data.

        For example, normalizes column names and filters out irrelevant rows.
        """
        df = self._raw_inputs.get("sheet_data")
        if df is not None and not df.empty:
            df.columns = [col.strip().lower() for col in df.columns]
            self._clean_inputs["sheet_data"] = df