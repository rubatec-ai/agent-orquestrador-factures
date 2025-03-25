from typing import Dict
import pandas as pd
import requests
from logging import Logger
from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor


class SageExtractor(BaseExtractor):
    """
    Extracts and processes data from the Sage API using service credentials.

    This class performs a GET request to a specified Sage API endpoint,
    converts the JSON response into a DataFrame, and cleans the data by
    selecting and renaming columns according to custom logic.

    The cleaned data is stored under the key "sagedata" in the clean_inputs dictionary.
    """

    def __init__(self, config: ConfigurationManager, logger: Logger) -> None:
        # Retrieve parameters from configuration
        self._api_key = config.sage_api_key
        self._endpoint = config.sage_endpoint
        self._headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json"
        }
        logger.name = "SageExtractor"
        # Initialize the base extractor (this may call get_input_data and clean_input_data)
        super().__init__(config, logger)

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetches raw data from the Sage API endpoint and converts it into a DataFrame.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary with key "sagedata" containing the raw data as a DataFrame.
        """
        try:
            response = requests.get(self._endpoint, headers=self._headers)
            if response.status_code == 200:
                data = response.json()
                # Assuming the JSON is a list of records; adjust as necessary.
                df = pd.DataFrame(data)
                return {"sagedata": df}
            else:
                raise Exception(
                    f"Failed to fetch Sage data. Status Code: {response.status_code}, Response: {response.text}")
        except Exception as e:
            raise Exception(f"Error fetching Sage data: {e}")

    def clean_input_data(self):
        """
        Processes and cleans the raw Sage data by selecting and renaming columns.

        For example, if the raw data includes columns: 'id', 'date', 'amount', 'customer',
        we can rename them to: 'invoice_id', 'invoice_date', 'invoice_amount', 'customer_name'.
        Adjust the selection and renaming as needed.
        """
        df = self._raw_inputs.get("sagedata")
        if df is not None and not df.empty:
            # Customize this selection and renaming according to the actual JSON structure from Sage
            # Ejemplo: selecciona y renombra las columnas
            df = df[['id', 'date', 'amount', 'customer']]
            df.rename(columns={
                'id': 'invoice_id',
                'date': 'invoice_date',
                'amount': 'invoice_amount',
                'customer': 'customer_name'
            }, inplace=True)
            self._clean_inputs["sagedata"] = df

    @property
    def clean_inputs(self):
        return self._clean_inputs
