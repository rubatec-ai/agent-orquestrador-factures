from logging import Logger
from typing import Dict

import pandas as pd
import spacy

from src.config import ConfigurationManager
from src.io_methods import IOHandler
from src.transformations.generate_scope import generate_scope_from_directory
from src.transformations.transform_paragraph import transform_paragraph
from src.transformations.transform_param import transform_param
from src.utils.embeddings import ensure_array


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
        self._data_model = {}

    def run(self) -> Dict[str, pd.DataFrame]:
        """
        Perform transformations to clean inputs and generate the data model.

        Returns:
            Dict[str, pd.DataFrame]: The resulting data model as a dictionary of DataFrames.
        """
        self._logger.info("Starting transformations on clean inputs.")

        # Load language models
        nlp_catalan = spacy.load("ca_core_news_sm")
        nlp_spanish = spacy.load("es_core_news_sm")

        # Handle scope transformation
        self._handle_scope_transformation(nlp_catalan, nlp_spanish)

        # Handle parameter transformation
        self._handle_param_transformation(nlp_catalan, nlp_spanish)

        self._logger.info("Transformations completed.")
        return self._data_model

    def _handle_scope_transformation(self, nlp_catalan, nlp_spanish):
        """
        Handles the transformation of the scope data.
        """
        if self._config.etl_param_use_debug_scope:
            self._data_model['text_master'] = self._clean_inputs['processed_scope']
            self._data_model['text_master'].loc[:, 'embedding'] = self._data_model['text_master']['embedding'].apply(
                ensure_array)
        else:
            if self._config.etl_param_use_pdf_from_directory:
                self._clean_inputs['scope'] = generate_scope_from_directory(
                    directory=self._config.pdf_directory,
                    logger=self._logger
                )
            self._data_model['text_master'] = transform_paragraph(
                df=self._clean_inputs['scope'],
                nlp_ca=nlp_catalan,
                nlp_es=nlp_spanish,
                config=self._config,
                logger=self._logger
            )

    def _handle_param_transformation(self, nlp_catalan, nlp_spanish):
        """
        Handles the transformation of the parameter data.
        """
        if self._config.etl_param_use_default_param:
            self._data_model['param_master'] = self._clean_inputs['processed_queries']
            self._data_model['param_master'].loc[:, 'embedding_ca'] = self._data_model['param_master'][
                'embedding_ca'].apply(ensure_array)
            self._data_model['param_master'].loc[:, 'embedding_es'] = self._data_model['param_master'][
                'embedding_es'].apply(ensure_array)
        else:
            self._data_model['param_master'] = transform_param(
                df=self._clean_inputs['query'],
                nlp_ca=nlp_catalan,
                nlp_es=nlp_spanish,
                config=self._config,
                logger=self._logger
            )

    @property
    def data_model(self) -> Dict[str, pd.DataFrame]:
        return self._data_model
