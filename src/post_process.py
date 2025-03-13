from logging import Logger
from typing import Dict

import pandas as pd

from src.config import ConfigurationManager
from src.io_methods import IOHandler


class PostProcessor:
    """
    Handles the post-processing of scheduling or solution data.

    Attributes:
        _data_model (Dict[str, pd.DataFrame]): Data model containing solution and related data.
        _config (ConfigurationManager): Configuration settings instance.
        _logger (Logger): Logger for tracking post-processing steps.
        _io (IOHandler): Handler for reading and writing data.
        _exports (Dict[str, pd.DataFrame] or None): Dictionary to store prepared data for export.
    """

    def __init__(self, data_model: Dict[str, pd.DataFrame], config: ConfigurationManager,
                 logger: Logger, io_handler: IOHandler):
        self._data_model = data_model
        self._config = config
        self._logger = logger
        self._logger.name = 'PostProcessor'
        self._io = io_handler
        self._exports = {}

    def run(self):
        """
        Execute the post-processing pipeline.

        Prepares and exports the necessary metrics or reports based on the data model.
        """
        self._logger.info("Starting post-processing.")

        # Example post-processing steps:
        self._exports['processed_output_1'] = self._prepare_example_data()

        if self._config.export_post_process:
            self._io.write_post_process(self._exports)

        self._logger.info("Post-processing completed. Exporting results.")



    def _prepare_example_data(self) -> pd.DataFrame:
        """
        Example method for preparing specific post-processed data.

        Returns:
            pd.DataFrame: Prepared DataFrame ready for export.
        """
        self._logger.debug("Preparing example post-processed data.")

        # Example logic (to be replaced with actual post-processing logic):
        df = self._data_model.get('solution', pd.DataFrame())
        if df.empty:
            self._logger.warning("Solution data is empty. Returning an empty DataFrame.")
            return pd.DataFrame()

        # Example processing: Add a new column based on existing data.
        df = df.copy()
        df['test_post_proces'] = 'test'

        self._logger.debug("Example post-processed data prepared successfully.")

        return df

    @property
    def exports(self) -> Dict[str, pd.DataFrame]:
        return self._exports
