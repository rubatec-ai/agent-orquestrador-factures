from logging import Logger
from typing import Dict

import pandas as pd

from src.config import ConfigurationManager
from src.io_methods import IOHandler
from src.transformations.transform_default import transform_default
from src.transformations.transform_files import transform_files
from src.transformations.transform_invoices import transform_invoices
from src.transformations.transform_register import transform_register


class Transformer:
    """
    Handles transformations to refine inputs and generate the data model.

    Attributes:
        _config (ConfigurationManager): Instance of the configuration settings.
        _io (IOHandler): Handler for input/output operations.
        _logger (Logger): Logger for tracking transformation processes.
        _clean_inputs (Dict[str, pd.DataFrame]): Cleaned input data tables.
        _data_model (Dict[str, pd.DataFrame]): Transformed data model tables.
    """

    def __init__(self, config: ConfigurationManager, io_handler: IOHandler, logger: Logger,
                 clean_inputs: Dict[str, pd.DataFrame]):
        self._config = config
        self._io = io_handler
        self._logger = logger
        self._logger.name = 'Transformer'
        self._clean_inputs = clean_inputs
        self._transformations = {
            'invoices': transform_invoices,
            'files': transform_files,
            'register' : transform_register
        }
        self._data_model = {}

    def run(self) -> Dict[str, pd.DataFrame]:

        """
        Perform transformations to clean inputs and generate the data model.

        Returns:
            Dict[str, pd.DataFrame]: The resulting data model as a dictionary of DataFrames.
        """
        self._logger.info("Starting transformations on clean inputs.")

        self._data_model = {
            key: self._transformations.get(key, transform_default)(
                df, self._clean_inputs, self._logger
            )
            for key, df in self._clean_inputs.items()
        }

        self._logger.info("Transformations completed.")
        return self._data_model

    @property
    def data_model(self) -> Dict[str, pd.DataFrame]:
        return self._data_model
