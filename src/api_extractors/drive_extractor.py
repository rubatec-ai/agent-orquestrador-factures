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
    fetches metadata of PDF files from a specified Drive folder (provided in the configuration),
    and converts the data into a DataFrame with one row per PDF file.

    The resulting DataFrame contains:
      - file_id: ID of the file.
      - file_name: Name of the file.
      - created_time: When the file was created.
      - modified_time: When the file was last modified.
      - file_size: Size of the file in bytes.
      - web_view_link: Link to view the file in Drive.
    """

    def __init__(self, config: ConfigurationManager, logger: Logger) -> None:
        self._credentials_path = config.google_credentials_json
        self._scopes = config.google_drive_scopes
        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path, scopes=self._scopes
        )
        self._service = build('drive', 'v3', credentials=credentials)
        # Folder ID donde se almacenan los PDFs; debe definirse en el config.
        self._folder_id = config.google_drive_folder_id
        logger.name = "DriveExtractor"
        super().__init__(config, logger)

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetches PDF file metadata from the specified Drive folder.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary with a DataFrame under the key "files" where each row
            corresponds to a PDF file in the folder, including metadata such as file_id, file_name,
            created_time, modified_time, file_size, and web_view_link.
        """
        try:
            # Build query to fetch only PDF files in the folder
            query = f"'{self._folder_id}' in parents and mimeType='application/pdf'"
            response = self._service.files().list(
                q=query,
                pageSize=100,
                fields="files(id, name, mimeType, createdTime, modifiedTime, size, webViewLink)"
            ).execute()
            files = response.get('files', [])
            df = pd.DataFrame(files)
            return {"files": df}
        except Exception as e:
            raise Exception(f"Failed to fetch Drive data: {str(e)}")

    def clean_input_data(self):
        """
        Processes and cleans the raw Drive data.

        In this case, it renames columns for consistency.
        """
        df = self._raw_inputs.get("files")
        if df is not None and not df.empty:
            df = df.rename(columns={
                "id": "file_id",
                "name": "file_name",
                "createdTime": "created_time",
                "modifiedTime": "modified_time",
                "size": "file_size",
                "webViewLink": "web_view_link"
            })
            self._clean_inputs["files"] = df
