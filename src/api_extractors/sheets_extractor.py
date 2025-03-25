from typing import Dict
import pandas as pd
import requests
from logging import Logger
from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor


class GSheetsExtractor(BaseExtractor):
    """
    Extracts and processes data from the Google Sheets API.

    This class fetches raw data from a specific Google Sheet and cleans it into a DataFrame.
    """

    def __init__(self, config: ConfigurationManager, logger: Logger) -> None:
        self._api_key = config.sheets_api_key  # Example: API key for Google Sheets.
        self._sheet_id = config.sheet_id  # The ID of the Google Sheet.
        self._endpoint = "https://sheets.googleapis.com/v4/spreadsheets"
        self._headers = {"Authorization": f"Bearer {self._api_key}"}
        logger.name = "SheetsExtractor"
        super().__init__(config, logger)

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetches raw data from the Google Sheet.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary containing raw Sheets data, e.g., under the key "sheet_data".
        """
        # Example: Fetch data from a specific range in the first sheet.
        url = f"{self._endpoint}/{self._sheet_id}/values/Sheet1!A:Z"
        response = requests.get(url, headers=self._headers)
        if response.status_code == 200:
            json_response = response.json()
            values = json_response.get("values", [])
            if values:
                # Assume the first row contains headers.
                header = values[0]
                data = values[1:]
                df = pd.DataFrame(data, columns=header)
                return {"sheet_data": df}
            else:
                return {"sheet_data": pd.DataFrame()}
        else:
            raise Exception(
                f"Failed to fetch Sheets data. "
                f"Status Code: {response.status_code}, Response: {response.text}"
            )

    def clean_input_data(self):
        """
        Processes and cleans the raw Sheets data.

        For example, renaming columns or filtering out irrelevant rows.
        """
        df = self._raw_inputs.get("sheet_data")
        if df is not None and not df.empty:
            # Example: Normalize column names by stripping and converting to lowercase.
            df.columns = [col.strip().lower() for col in df.columns]
            # Additional cleaning logic can be added here.
            self._clean_inputs["sheet_data"] = df
