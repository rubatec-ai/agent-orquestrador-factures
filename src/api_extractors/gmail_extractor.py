from typing import Dict
import pandas as pd
from logging import Logger
from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor


class GmailExtractor(BaseExtractor):
    """
    Extracts and processes data from the Gmail API using service account credentials.

    This class loads credentials from a JSON file, creates a Gmail API client,
    fetches raw email message metadata, and cleans it into a DataFrame.
    """

    def __init__(self, config: ConfigurationManager, logger: Logger) -> None:
        self._credentials_path = config.google_credentials_json
        self._scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path, scopes=self._scopes
        )
        # If necessary, delegate to a user (for domain-wide delegation):
        # credentials = credentials.with_subject(config.gmail_user_email)
        self._service = build('gmail', 'v1', credentials=credentials)
        logger.name = "GmailExtractor"
        super().__init__(config, logger)

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetches raw Gmail data using the Gmail API.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary containing raw Gmail data under the key "emails".
        """
        try:
            response = self._service.users().messages().list(userId='me', maxResults=100).execute()
            messages = response.get('messages', [])
            df = pd.DataFrame(messages)
            return {"emails": df}
        except Exception as e:
            raise Exception(f"Failed to fetch Gmail data: {str(e)}")

    def clean_input_data(self):
        """
        Processes and cleans the raw Gmail data.

        For example, renames columns or ensures proper data types.
        """
        df = self._raw_inputs.get("emails")
        if df is not None and not df.empty:
            # Example: Rename the 'id' column to 'email_id'
            df = df.rename(columns={"id": "email_id"})
            self._clean_inputs["emails"] = df