from typing import Dict
import pandas as pd
from logging import Logger
from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor


class GSheetsExtractor(BaseExtractor):
    """
    Extracts data from a specific Google Sheet using the Sheets API with service account credentials.

    This class loads credentials from a JSON file, creates a Sheets API client,
    reads data from a defined range (default "Sheet1!A:Z") of the specified sheet,
    and returns the data as a pandas DataFrame.
    """

    def __init__(self, config: ConfigurationManager, logger: Logger) -> None:
        self._credentials_path = config.google_credentials_json
        self._sheets_scopes = config.google_sheets_scopes
        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path, scopes=self._sheets_scopes
        )

        self._service = build('sheets', 'v4', credentials=credentials)
        self._sheet_id = config.sheet_id
        logger.name = "GSheetsExtractor"
        super().__init__(config, logger)

    def read(self) -> pd.DataFrame:
        """
        Reads data from the specified Google Sheet and returns it as a DataFrame.

        The default range is "Sheet1!A:Z", which can be adjusted if needed.
        """
        try:
            sheet = self._service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=self._sheet_id,
                range="Sheet1!A:Z"
            ).execute()
            values = result.get("values", [])
            if not values:
                return pd.DataFrame()
            header = values[0]
            data = values[1:]
            df = pd.DataFrame(data, columns=header)
            df.columns = [col.strip().lower() for col in df.columns]
            return df
        except Exception as e:
            raise Exception(f"Failed to fetch Sheets data: {str(e)}")
