import os
from logging import Logger
from typing import List, Union, Dict

import pandas as pd

from src.config import ConfigurationManager


class IOHandler:
    """
    Manages input and output operations, primarily interacting with the file system.

    Attributes:
        _config (ConfigurationManager): Instance of the configuration settings.
    """

    def __init__(self, config: ConfigurationManager):
        self._config = config

    def read_data_control(self, tab_name: str) -> pd.DataFrame:
        """
        Read a specific tab from the data control file.

        Args:
            tab_name (str): Name of the tab to read.

        Returns:
            pd.DataFrame: Loaded data as a DataFrame.
        """
        filepath = self._config.data_control_filepath
        return self.read_excel(filepath=filepath, sheet_name=tab_name, types=str)

    @staticmethod
    def read_csv(filepath: str, sep: str = ",", parse_dates: List[str] = None,
                 types: Union[type, Dict[str, type]] = None,
                 skiprows: int = None) -> pd.DataFrame:
        """
        Load a CSV file into a DataFrame.

        Args:
            filepath (str): Path to the CSV file.
            sep (str): Delimiter to use.
            parse_dates (list): List of columns to parse as dates.
            types (dict): Column types.
            skiprows (int): Number of rows to skip at the start.

        Returns:
            pd.DataFrame: Loaded DataFrame.
        """
        return pd.read_csv(filepath, sep=sep, parse_dates=parse_dates, dtype=types, skiprows=skiprows)

    @staticmethod
    def read_excel(filepath: str, sheet_name: str, parse_dates: List[str] = None,
                   types: Union[type, Dict[str, type]] = None,
                   skiprows: int = None) -> pd.DataFrame:
        """
        Load an Excel sheet into a DataFrame.

        Args:
            filepath (str): Path to the Excel file.
            sheet_name (str): Name of the sheet to read.
            parse_dates (list): List of columns to parse as dates.
            types (dict): Column types.
            skiprows (int): Number of rows to skip at the start.

        Returns:
            pd.DataFrame: Loaded DataFrame.
        """
        return pd.read_excel(filepath, sheet_name=sheet_name, parse_dates=parse_dates, dtype=types, skiprows=skiprows)

    @staticmethod
    def write_to_csv(df: pd.DataFrame, filepath: str) -> None:
        """
        Write a DataFrame to a CSV file with UTF-8 BOM encoding.

        Args:
            df (pd.DataFrame): DataFrame to write.
            filepath (str): Destination path.
        """
        if df.empty:
            raise ValueError("Cannot write an empty DataFrame to CSV.")
        df.to_csv(filepath, index=False, encoding='utf-8-sig')

    @staticmethod
    def write_to_excel(df: pd.DataFrame, filepath: str, sheet_name: str) -> None:
        """
        Write a DataFrame to an Excel file.

        Args:
            df (pd.DataFrame): DataFrame to write.
            filepath (str): Destination path.
            sheet_name (str): Sheet name.
        """
        df.to_excel(filepath, sheet_name=sheet_name, index=False)


    def write_post_process(self, data: Dict[str, pd.DataFrame]) -> None:
        """
        Save post-processed data to the appropriate directory.

        Args:
            data (Dict[str, pd.DataFrame]): Dictionary of DataFrames to write.
        """
        directory = os.path.join(self._config.export_directory, self._config.scenario_name)
        os.makedirs(directory, exist_ok=True)

        for name, df in data.items():
            filepath = os.path.join(directory, f'{name}.csv')
            self.write_to_csv(df, filepath)

    def read_solution(self, logger: Logger) -> pd.DataFrame:
        """
        Load specific "Solution" for a specific "run_name" (previously done).

        Args:
            logger (Logger): Logger instance for logging messages.

        Returns:
            Dict[str, pd.DataFrame]: Dictionary of loaded DataFrames.
        """
        directory = os.path.join(self._config.export_directory, self._config.run_name)
        file_name = 'solution.csv'
        df = self.read_csv(str(os.path.join(directory, file_name)))

        return df

    def write_solution(self, df: pd.DataFrame) -> None:
        """
        Save the solution data to the appropriate directory in both CSV and Excel formats.

        Args:
            df (pd.DataFrame): Solution DataFrame.
        """
        # Crear el directorio si no existe
        directory = os.path.join(self._config.export_directory, self._config.scenario_name)
        os.makedirs(directory, exist_ok=True)

        filepath = os.path.join(directory, 'solution.csv')
        self.write_to_csv(df, filepath)

    def read_data_model(self, logger: Logger) -> Dict[str, pd.DataFrame]:
        """
        Load all data model tables from the specified directory.

        Args:
            logger (Logger): Logger instance for logging messages.

        Returns:
            Dict[str, pd.DataFrame]: Dictionary of loaded DataFrames.
        """
        directory = os.path.join(self._config.transform_export_directory, self._config.scenario_name, 'data_model')
        logger.info(f"Reading data model from {directory}.")

        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        data_model = {
            os.path.splitext(file)[0]: self.read_csv(os.path.join(directory, file))
            for file in csv_files
        }

        return data_model

    def write_data_model(self, data_model: Dict[str, pd.DataFrame], logger: Logger) -> None:
        """
        Save all data model tables to the specified directory.

        Args:
            data_model (Dict[str, pd.DataFrame]): Dictionary of DataFrames to save.
            logger (Logger): Logger instance for logging messages.
        """
        directory = os.path.join(self._config.transform_export_directory, self._config.scenario_name, 'data_model')
        os.makedirs(directory, exist_ok=True)

        logger.info(f"Writing data model to {directory}.")

        for name, df in data_model.items():
            filepath = os.path.join(directory, f'{name}.csv')

            if df.empty:
                logger.warning(f"The DataFrame '{name}' is empty. Skipping writing to {filepath}.")
                continue

            self.write_to_csv(df, filepath)

    def write_solution_model(self, solution: Dict[str, pd.DataFrame], logger: Logger) -> None:
        """
        Save all data model tables to the specified directory.

        Args:
            solution  (Dict[str, pd.DataFrame]): Dictionary of DataFrames to save.
            logger (Logger): Logger instance for logging messages.
        """
        directory = os.path.join(self._config.export_directory, self._config.scenario_name, 'solution')
        os.makedirs(directory, exist_ok=True)

        logger.info(f"Writing data model to {directory}.")

        for name, df in solution.items():
            filepath = os.path.join(directory, f'{name}.csv')

            if df.empty:
                logger.warning(f"The DataFrame '{name}' is empty. Skipping writing to {filepath}.")
                continue

            self.write_to_csv(df, filepath)

    @staticmethod
    def read_parquet(filepath: str) -> pd.DataFrame:
        """
        Load a Parquet file into a DataFrame.

        Args:
            filepath (str): Path to the Parquet file.

        Returns:
            pd.DataFrame: Loaded DataFrame.
        """
        return pd.read_parquet(filepath)

    @staticmethod
    def read_pickle(filepath: str) -> pd.DataFrame:
        """
        Load a Pickle file into a DataFrame.

        Args:
            filepath (str): Path to the Pickle file.

        Returns:
            pd.DataFrame: Loaded DataFrame.
        """
        return pd.read_pickle(filepath)
