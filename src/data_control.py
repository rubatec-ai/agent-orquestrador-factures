import logging
from logging import Logger
from typing import Dict

import pandas as pd

from src.config import ConfigurationManager
from src.io_methods import IOHandler


class DataController:
    """
    Manages input tables, field selection, and renaming operations.

    Attributes:
        _config (ConfigurationManager): Configuration settings instance.
        _io (IOHandler): Handler for input/output operations.
        _logger (Logger): Logger for tracking events and messages.
        _data_control_sources (pd.DataFrame): DataFrame containing metadata about input sources.
        _data_control_fields (pd.DataFrame): DataFrame containing metadata about input fields.
        _raw_inputs (Dict[str, pd.DataFrame]): Raw input data by table name.
        _processed_inputs (Dict[str, pd.DataFrame]): Cleaned and processed input data by table name.
    """

    def __init__(self, config: ConfigurationManager, io_handler: IOHandler, logger: Logger) -> None:
        self._config = config
        self._io = io_handler
        self._logger = logger
        self._logger.name = 'DataController'

        self._data_control_sources = None
        self._data_control_fields = None
        self._raw_inputs = {}
        self._processed_inputs = {}

        self._load_data_control()

    def _load_data_control(self) -> None:
        """
        Load metadata about sources and fields using the IO handler.

        This method initializes the attributes `_data_control_sources` and `_data_control_fields`
        with the corresponding data from the configuration.
        """
        self._data_control_sources = self._io.read_data_control(self._config.data_control_sources)
        self._data_control_fields = self._io.read_data_control(self._config.data_control_fields)

    def load_inputs(self) -> None:
        """
        Load raw input data and process it to create cleaned inputs.

        This method populates `_raw_inputs` with unprocessed data and `_processed_inputs`
        with data that has been filtered and renamed according to the metadata.
        """
        self._raw_inputs = self._load_raw_inputs()
        self._processed_inputs = self._process_raw_inputs()

    def _load_raw_inputs(self) -> Dict[str, pd.DataFrame]:
        """
        Load raw input tables specified in the sources metadata.

        Returns:
            Dict[str, pd.DataFrame]: Raw input tables by table name.
        """
        self._logger.info("Loading raw input tables.")

        raw_inputs = {
            row['table_id']: self._load_table(row)
            for _, row in self._data_control_sources.iterrows()
        }

        return raw_inputs

    def _load_table(self, row: pd.Series) -> pd.DataFrame:
        """
        Load a single table from its source.

        Args:
            row (pd.Series): Metadata about the table to load.

        Returns:
            pd.DataFrame: Loaded table as a DataFrame.
        """
        self._logger.debug(f"Loading table: {row['table_id']} from {row['source_file']}.")

        filepath = (f"{self._config.data_directory}/{row['source_file']}.{row['source_type']}")

        if row['source_type'] == 'csv':
            return self._io.read_csv(filepath, sep=row['source_file_delimiter'], skiprows=int(row['skiprows']),
                                     types=str)
        elif 'xls' in row['source_type']:
            return self._io.read_excel(filepath, sheet_name=row['sheet_name'], skiprows=int(row['skiprows']), types=str)
        elif row['source_type'] == 'parquet':
            return self._io.read_parquet(filepath)
        elif row['source_type'] == 'pkl':
            return self._io.read_pickle(filepath)
        else:
            raise ValueError(f"Unsupported file type: {row['source_type']}.")

    def _process_raw_inputs(self) -> Dict[str, pd.DataFrame]:
        """
        Process raw inputs by filtering and renaming fields according to the metadata.

        Returns:
            Dict[str, pd.DataFrame]: Processed input tables by table name.
        """
        self._logger.info("Processing raw input tables.")

        processed_inputs = {
            table_id: self._process_table(table_id, df)
            for table_id, df in self._raw_inputs.items()
        }

        return processed_inputs

    def _process_table(self, table_id: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter and rename fields for a specific table.

        Args:
            table_id (str): Identifier of the table to process.
            df (pd.DataFrame): Raw input DataFrame.

        Returns:
            pd.DataFrame: Processed DataFrame.
        """
        fields_metadata = self._data_control_fields[
            (self._data_control_fields['table_id'] == table_id) &
            (self._data_control_fields['selected_field'] == 'True')
            ]

        renaming_map = dict(zip(fields_metadata['source_field'], fields_metadata['field_id']))
        selected_fields = fields_metadata['source_field'].tolist()

        df = df[selected_fields].rename(columns=renaming_map)
        df = self._convert_field_types(df, fields_metadata)

        return df

    @staticmethod
    def _convert_field_types(df: pd.DataFrame, fields_metadata: pd.DataFrame) -> pd.DataFrame:
        """
        Convert DataFrame fields to the specified types.

        Args:
            df (pd.DataFrame): DataFrame to process.
            fields_metadata (pd.DataFrame): Metadata defining field types.

        Returns:
            pd.DataFrame: DataFrame with converted field types.
        """
        type_mapping = dict(zip(fields_metadata['field_id'], fields_metadata['field_type']))

        for column, field_type in type_mapping.items():
            if field_type=='skip':
                continue
            if field_type == 'date':
                date_format = fields_metadata[fields_metadata['field_id'] == column]['datetime_format'].item()
                df[column] = pd.to_datetime(df[column], format=date_format, errors='coerce')
            elif field_type in ['int', 'float']:
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif field_type == 'bool':
                df[column] = df[column].str.lower().map({'true': True, 'false': False})
            else:
                df[column] = df[column].astype(field_type)

        return df

    @property
    def raw_inputs(self) -> Dict[str, pd.DataFrame]:
        return self._raw_inputs

    @property
    def processed_inputs(self) -> Dict[str, pd.DataFrame]:
        return self._processed_inputs
