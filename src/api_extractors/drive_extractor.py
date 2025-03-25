from typing import Dict
import pandas as pd
from logging import Logger
from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor


class DriveExtractor(BaseExtractor):
    """
    Extracts and processes data from the Google Drive API using service account credentials.

    This class loads credentials from a JSON file, creates a Drive API client,
    fetches raw file metadata, and cleans it into a DataFrame.
    """

    def __init__(self, config: ConfigurationManager, logger: Logger) -> None:
        # Load credentials from JSON with the proper Drive scope.
        self._credentials_path = config.google_credentials_json
        self._scopes = ['https://www.googleapis.com/auth/drive.metadata.readonly']
        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path, scopes=self._scopes
        )
        self._service = build('drive', 'v3', credentials=credentials)
        logger.name = "DriveExtractor"
        super().__init__(config, logger)

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetches raw Google Drive data using the Drive API.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary containing raw Drive data under the key "files".
        """
        try:
            # Retrieve a list of files with metadata.
            response = self._service.files().list(
                pageSize=100,
                fields="files(id, name, mimeType, createdTime)"
            ).execute()
            files = response.get('files', [])
            df = pd.DataFrame(files)
            return {"files": df}
        except Exception as e:
            raise Exception(f"Failed to fetch Drive data: {str(e)}")

    def clean_input_data(self):
        """
        Processes and cleans the raw Google Drive data.

        For example, filters out non-PDF files and renames columns.
        """
        df = self._raw_inputs.get("files")
        if df is not None and not df.empty:
            # Example: Filter only PDF files.
            df = df[df['mimeType'] == 'application/pdf']
            # Rename columns for consistency.
            df = df.rename(columns={"id": "file_id", "name": "file_name"})
            self._clean_inputs["files"] = df