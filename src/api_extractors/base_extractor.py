from typing import Dict
import pandas as pd
import requests
from src.config import ConfigurationManager
from logging import Logger

class BaseExtractor:
    """
    Base class for API extractors that manages input data retrieval and cleaning operations.

    Attributes:
        _config (ConfigurationManager): An instance of configuration settings.
        _logger (Logger): Logging object for recording events and messages.
        _raw_inputs (Dict[str, pd.DataFrame]): Raw input data fetched from the API.
        _clean_inputs (Dict[str, pd.DataFrame]): Cleaned input data.
    """

    def __init__(self, config: ConfigurationManager) -> None:
        self._config = config
        self._raw_inputs: Dict[str, pd.DataFrame] = {}
        self._clean_inputs: Dict[str, pd.DataFrame] = {}

        # Fetch and then clean input data upon initialization.
        self._raw_inputs = self.get_input_data()
        self.clean_input_data()

    def make_request(self, url: str, params: Dict = None) -> pd.DataFrame:
        """
        Makes a GET request to the specified URL and returns the result as a DataFrame.

        Args:
            url (str): The URL to make the request to.
            params (Dict, optional): Optional parameters for the GET request.

        Returns:
            pd.DataFrame: DataFrame representation of the JSON response.

        Raises:
            Exception: If the request fails.
        """
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        else:
            raise Exception(
                f"Failed to fetch data from {url}. "
                f"Status Code: {response.status_code}, Response: {response.text}"
            )

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Retrieves raw input data from the API.
        **Subclasses must implement this method.**

        Returns:
            Dict[str, pd.DataFrame]: A dictionary of DataFrames containing raw data.
        """
        raise NotImplementedError("Subclasses must implement the get_input_data method.")

    def clean_input_data(self):
        """
        Processes and cleans the raw input data.
        **Subclasses must implement this method.**
        """
        raise NotImplementedError("Subclasses must implement the clean_input_data method.")

    @property
    def clean_inputs(self) -> Dict[str, pd.DataFrame]:
        """
        Provides access to the cleaned input data.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary containing cleaned DataFrames.
        """
        return self._clean_inputs

    def extract(self):
        self._raw_inputs = self.get_input_data()
        self.clean_input_data()