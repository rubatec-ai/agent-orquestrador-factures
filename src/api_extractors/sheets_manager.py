import logging
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
      - Reading data from Sheets with cleaning and type conversion.
      - Writing/updating entire DataFrames to Sheets.
      - Appending new rows to Sheets.
      - Handling A1 notation for cell ranges.
    """

    def __init__(self, config: ConfigurationManager) -> None:
        """
        Initializes the Google Sheets Manager.

        Args:
            config: Application configuration containing:
                - google_credentials_json
                - google_sheets_scopes
                - sheet_id
                - sheet_name
        """
        self._credentials_path = config.google_credentials_json
        self._sheets_scopes = config.google_sheets_scopes

        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path, scopes=self._sheets_scopes
        )

        self._service = build('sheets', 'v4', credentials=credentials)
        self._sheet_id = config.sheet_id
        self._sheet_name = config.sheet_name_registro
        self._default_sheet_name = "Hoja 1"

        self._logger = logging.getLogger("GoogleSheetsManager")
        self._logger.info('Starting Google Sheets Manager...')
        super().__init__(config)

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Reads data from the configured Google Sheet and returns a dictionary of DataFrames.
        Performs column renaming and type conversion according to MAPPING_RENAME_COL_REGISTRO.
        """
        try:
            df = self.read(sheet_name=self._sheet_name)
            rename_dict = {old: new for old, (new, _) in MAPPING_RENAME_COL_REGISTRO.items()}
            type_dict = {new: dtype for _, (new, dtype) in MAPPING_RENAME_COL_REGISTRO.items()}
            df.rename(columns=rename_dict, inplace=True)

            line_items = self.read(sheet_name='line_items')

            for col, dtype in type_dict.items():
                if col in df.columns:
                    if dtype == 'date':
                        df[col] = pd.to_datetime(
                            df[col],
                            errors='coerce',
                            dayfirst=True,
                            format='%d/%m/%Y'
                        ).dt.date
                    elif dtype == 'datetime':
                        df[col] = pd.to_datetime(
                            df[col],
                            errors='coerce',
                            dayfirst=True,
                            format='%d/%m/%Y %H:%M:%S'
                        ).dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        df[col] = df[col].astype(dtype, errors='ignore')

            return {"register": df, "line_items": line_items}

        except Exception as e:
            self._logger.error(f"Error in get_input_data: {str(e)}")
            raise

    def clean_input_data(self) -> None:
        """
        Cleans and prepares the raw sheet data:
          - Removes completely empty rows.
          - Strips whitespace from string values.
          - Normalizes column names.
        """
        for key, df in self._raw_inputs.items():
            if df is not None and not df.empty:
                df = df.dropna(how='all')
                df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
                df.columns = [col.strip() for col in df.columns]
                self._clean_inputs[key] = df
                self._logger.info("Sheet data cleaned successfully")

    def read(self, sheet_name: Optional[str] = "registro") -> pd.DataFrame:
        """
        Reads data from the specified sheet.

        Args:
            sheet_name: Target sheet name (default: 'Sheet1')

        Returns:
            A Pandas DataFrame with the sheet data.
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

            # Rellenar filas más cortas con valores vacíos
            num_cols = len(headers)
            data = [row + [None] * (num_cols - len(row)) for row in data]

            df = pd.DataFrame(data, columns=headers)

            return df

        except HttpError as e:
            self._logger.error(f"HTTP error reading data: {e}")
            raise
        except Exception as e:
            self._logger.error(f"General error reading data: {e}")
            raise

    def append_row(self, rows_data: List[List], sheet_name: Optional[str] = None,
                   sheet_range: Optional[str] = 'A:Q') -> bool:
        """
        Añade múltiples filas a la hoja especificada.

        Args:
            rows_data (List[List]): Lista de listas, donde cada sublista representa una fila a insertar.
            sheet_name (str): Nombre de la pestaña (default: _default_sheet_name).
            sheet_range (str): Rango dentro de Google sheets.
        Returns:
            True si se inserta correctamente, False en caso contrario.
        """
        sheet_name = sheet_name or self._default_sheet_name
        range_name = f"{sheet_name}!{sheet_range}"
        # Limpiar cada fila individualmente
        cleaned_rows = [
            [str(item) if item is not None else "" for item in row]
            for row in rows_data
        ]
        body = {"values": cleaned_rows}  # Sin envolver en otra lista

        try:
            request = self._service.spreadsheets().values().append(
                spreadsheetId=self._sheet_id,
                range=range_name,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body
            )
            request.execute()
            self._logger.info(f"{len(rows_data)} filas añadidas a '{sheet_name}'.")
            return True
        except Exception as e:
            self._logger.error(f"Error añadiendo filas a '{sheet_name}': {e}")
            return False

    def _clear_range(self, range_name: str) -> None:
        """Clears the contents of the specified range."""
        self._service.spreadsheets().values().clear(
            spreadsheetId=self._sheet_id,
            range=range_name,
        ).execute()

    def _convert_to_a1_notation(self, sheet_name: str, start_cell: str,
                                num_rows: int, num_cols: int) -> str:
        """
        Converts coordinates to A1 range notation.

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
        Converts column letter(s) to numerical index.

        Args:
            col: Column letters (e.g., 'A', 'BC').

        Returns:
            Numerical index (A=1, Z=26, AA=27, etc.).
        """
        num = 0
        for c in col:
            if c.isalpha():
                num = num * 26 + (ord(c.upper()) - ord('A') + 1)
        return num
