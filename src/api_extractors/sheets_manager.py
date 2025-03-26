from typing import Dict, Optional, List
import pandas as pd
from logging import Logger
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from openpyxl.utils import get_column_letter

from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor
from src.utils.constants import MAPPING_RENAME_COL_REGISTRO


class GoogleSheetsManager(BaseExtractor):
    """
    Manages read/write operations with Google Sheets.

    Features:
    - Read data from sheets with automatic cleaning
    - Write/update entire DataFrames to sheets
    - Handle cell range conversions (A1 notation)
    - Error handling with detailed logging

    Requirements in config.json:
    - google_sheets_scopes: Should include 'https://www.googleapis.com/auth/spreadsheets'
    - sheet_id: ID of the target Google Sheet
    """

    def __init__(self, config: ConfigurationManager, logger: Logger) -> None:
        """
        Initialize the Google Sheets manager.

        Args:
            config: Application configuration
            logger: Logger instance for tracking operations
        """
        self._credentials_path = config.google_credentials_json
        self._sheets_scopes = config.google_sheets_scopes

        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path, scopes=self._sheets_scopes
        )

        self._service = build('sheets', 'v4', credentials=credentials)
        self._sheet_id = config.sheet_id
        self._sheet_name = config.sheet_name
        self._default_sheet_name = "Hoja 1"

        logger.name = "GoogleSheetsManager"
        super().__init__(config, logger)

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Read data from Google Sheets and store in raw_inputs.

        Returns:
            Dictionary with 'sheets_data' key containing the DataFrame

        Raises:
            Exception: If any error occurs during the read operation
        """
        try:
            df = self.read(sheet_name=self._sheet_name)
            df.rename(columns=MAPPING_RENAME_COL_REGISTRO, inplace=True)
            return {"master_register": df}
        except Exception as e:
            self._logger.error(f"Error in get_input_data: {str(e)}")
            raise

    def clean_input_data(self) -> None:
        """
        Clean and prepare the raw sheet data.

        Operations performed:
        - Remove completely empty rows
        - Strip whitespace from string values
        - Convert column names to lowercase
        """
        df = self._raw_inputs.get("master_register")
        if df is not None and not df.empty:
            # Basic cleaning
            df = df.dropna(how='all')
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

            # Column name normalization
            df.columns = [col.strip().lower() for col in df.columns]

            self._clean_inputs["master_register"] = df
            self._logger.info("Sheet data cleaned successfully")

    def read(self, sheet_name: Optional[str] = "registro") -> pd.DataFrame:
        """
        Read data from specified sheet.

        Args:
            sheet_name: Target sheet name (default: 'Sheet1')

        Returns:
            Pandas DataFrame with sheet data

        Raises:
            HttpError: For Google API-related errors
            Exception: For other unexpected errors
        """
        sheet_name = sheet_name or self._default_sheet_name
        range_name = f"{sheet_name}!A:Z"

        try:
            result = self._service.spreadsheets().values().get(
                spreadsheetId=self._sheet_id,
                range=range_name
            ).execute()

            values = result.get("values", [])

            if not values:
                return pd.DataFrame()

            headers = [col.strip() for col in values[0]]
            data = values[1:] if len(values) > 1 else []

            return pd.DataFrame(data, columns=headers)

        except HttpError as e:
            self._logger.error(f"HTTP error reading data: {e}")
            raise
        except Exception as e:
            self._logger.error(f"General error reading data: {e}")
            raise

    def write_dataframe(self, df: pd.DataFrame, sheet_name: Optional[str] = None,
                        range_start: str = "A1", clear: bool = True) -> bool:
        """
        Write/update a DataFrame to the specified sheet.

        Args:
            df: DataFrame to write
            sheet_name: Target sheet name (default: 'Sheet1')
            range_start: Starting cell (A1 notation)
            clear: Whether to clear existing data in range

        Returns:
            True if operation succeeded, False otherwise
        """
        sheet_name = sheet_name or self._default_sheet_name
        try:
            range_name = self._convert_to_a1_notation(
                sheet_name=sheet_name,
                start_cell=range_start,
                num_rows=len(df) + 1,  # +1 for headers
                num_cols=len(df.columns)
            )

            # Prepare data structure
            values = [df.columns.tolist()] + df.values.tolist()
            body = {'values': values, 'majorDimension': 'ROWS'}

            if clear:
                self._clear_range(range_name)

            request = self._service.spreadsheets().values().update(
                spreadsheetId=self._sheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            )
            request.execute()

            self._logger.info(f"Data updated successfully in {range_name}")
            return True

        except HttpError as e:
            self._logger.error(f"HTTP error writing data: {e}")
            return False
        except Exception as e:
            self._logger.error(f"General error writing data: {e}")
            return False

    def _clear_range(self, range_name: str) -> None:
        """Clear contents of specified range."""
        self._service.spreadsheets().values().clear(
            spreadsheetId=self._sheet_id,
            range=range_name,
        ).execute()

    def _convert_to_a1_notation(self, sheet_name: str, start_cell: str,
                                num_rows: int, num_cols: int) -> str:
        """
        Convert coordinates to A1 range notation.

        Example:
            _convert_to_a1_notation('Sheet1', 'B2', 3, 2) -> 'Sheet1!B2:C4'
        """
        start_col = ''.join(filter(str.isalpha, start_cell))
        start_row = ''.join(filter(str.isdigit, start_cell))

        end_col = get_column_letter(
            self._column_to_index(start_col) + num_cols - 1
        )
        end_row = int(start_row) + num_rows - 1

        return f"{sheet_name}!{start_cell}:{end_col}{end_row}"

    @staticmethod
    def _column_to_index(col: str) -> int:
        """
        Convert column letter(s) to numerical index.

        Args:
            col: Column letter(s) (e.g., 'A', 'BC')

        Returns:
            Numerical index (A=1, Z=26, AA=27, etc.)
        """
        num = 0
        for c in col:
            if c.isalpha():
                num = num * 26 + (ord(c.upper()) - ord('A') + 1)
        return num