from typing import Dict, Optional, List
import pandas as pd
import base64
import os
import shutil
from datetime import datetime
from logging import Logger
from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor
from src.utils.constants import MAX_RESULTS_GMAIL


class GmailExtractor(BaseExtractor):
    """
    Extracts and processes Gmail data using service account credentials.

    This class:
      - Loads credentials from a JSON file.
      - Creates a Gmail API client.
      - Fetches emails filtered by start_date and label (using parameters from config).
      - For each email, retrieves full message details and iterates over PDF attachments.
      - Builds a DataFrame with one row per PDF attachment containing:
          - email_id: ID of the email.
          - subject: Subject of the email.
          - date_received: Date when the email was received.
          - thread_id: Thread ID.
          - body: Email body (decoded text/plain).
          - attachment_id: ID of the PDF attachment.
          - filename: Name of the PDF file.
          - pdf_local_path: Local path where the PDF was saved.

    Note: The temporary folder where PDFs are saved is not automatically cleared.
          You should implement a cleaning routine if needed.
    """

    def __init__(self, config: ConfigurationManager, logger: Logger) -> None:
        # Load Gmail service using credentials and scopes from config
        self._credentials_path = config.google_credentials_json
        self._scopes = config.google_gmail_scopes
        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path, scopes=self._scopes
        )
        # For domain-wide delegation, uncomment the following line and ensure gmail_user_email is set in config:
        credentials = credentials.with_subject(config.gmail_user_email)
        self._service = build('gmail', 'v1', credentials=credentials)

        # Folder to save PDF attachments (temporary folder)
        self._save_pdf_folder = config.gmail_save_pdf_attachments_folder

        # Retrieve filtering parameters from config
        self._start_date = config.gmail_start_date  # e.g., "2023/06/01"
        self._label = config.gmail_label  # e.g., "INBOX"

        logger.name = "GmailExtractor"
        super().__init__(config, logger)

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetches Gmail messages filtered by start_date and label, retrieves full message details,
        downloads PDF attachments (if any) and builds a DataFrame with one row per attachment.
        """
        try:
            query = self._build_query()
            list_response = self._service.users().messages().list(
                userId='me', q=query, maxResults=MAX_RESULTS_GMAIL
            ).execute()
            messages = list_response.get('messages', [])
            attachments_data = []

            for message in messages:
                msg_id = message["id"]
                msg = self._service.users().messages().get(
                    userId='me', id=msg_id, format="full"
                ).execute()

                # Extract header info: subject and date
                subject, date_received = "", ""
                headers = msg.get("payload", {}).get("headers", [])
                for h in headers:
                    header_name = h.get("name", "").lower()
                    if header_name == "subject":
                        subject = h.get("value", "")
                    elif header_name == "date":
                        date_received = h.get("value", "")
                thread_id = msg.get("threadId", "")

                # Extract email body (text/plain)
                body = self._extract_body(msg)

                # Process attachments: create one row per PDF attachment
                parts = msg.get("payload", {}).get("parts", [])
                for part in parts:
                    filename = part.get("filename", "")
                    if filename and filename.lower().endswith(".pdf"):
                        attachment_id = part.get("body", {}).get("attachmentId", "")
                        # Download the PDF attachment and get local path
                        local_path = self._download_pdf_attachment(msg_id, attachment_id, filename)

                        attachment_row = {
                            "email_id": msg_id,
                            "subject": subject,
                            "date_received": date_received,
                            "thread_id": thread_id,
                            "body": body,
                            "attachment_id": attachment_id,
                            "filename": filename,
                            "pdf_local_path": local_path
                        }
                        attachments_data.append(attachment_row)

            df = pd.DataFrame(attachments_data)
            return {"pdf_attachments": df}
        except Exception as e:
            raise Exception(f"Failed to fetch Gmail data: {str(e)}")

    def clean_input_data(self):
        """
        Assigns the DataFrame of PDF attachments to the _clean_inputs dictionary.
        """
        df = self._raw_inputs.get("pdf_attachments")
        if df is not None and not df.empty:
            self._clean_inputs["pdf_attachments"] = df

    def _build_query(self) -> str:
        """Builds the query for Gmail API using start_date and label."""
        query_parts = []
        if self._start_date:
            query_parts.append(f"after:{self._start_date}")
        if self._label:
            query_parts.append(f"label:{self._label}")
        return " ".join(query_parts).strip()

    def _extract_body(self, msg: dict) -> str:
        """Extracts the plain text body from the email message."""
        payload = msg.get("payload", {})
        if "parts" in payload:
            for part in payload["parts"]:
                if part.get("mimeType") == "text/plain":
                    data = part.get("body", {}).get("data", "")
                    return self._decode_base64(data)
        else:
            data = payload.get("body", {}).get("data", "")
            return self._decode_base64(data)
        return ""

    def _download_pdf_attachment(self, msg_id: str, attachment_id: str, filename: str) -> Optional[str]:
        """
        Downloads the PDF attachment using the Gmail API and saves it in the temporary folder.
        Returns the local file path.
        """
        if not self._save_pdf_folder:
            return None

        try:
            attachment = self._service.users().messages().attachments().get(
                userId='me', messageId=msg_id, id=attachment_id
            ).execute()
            data = attachment.get('data', '')
            pdf_content = base64.urlsafe_b64decode(data.encode("UTF-8"))

            # Create the folder if it doesn't exist
            os.makedirs(self._save_pdf_folder, exist_ok=True)
            # Optionally, include a timestamp or msg_id to avoid conflicts
            local_filename = f"{msg_id}_{filename}"
            local_path = os.path.join(self._save_pdf_folder, local_filename)

            with open(local_path, "wb") as f:
                f.write(pdf_content)

            return local_path
        except Exception as e:
            self._logger.warning(f"Error downloading attachment {attachment_id} for message {msg_id}: {e}")
            return None

    def _decode_base64(self, data: str) -> str:
        """Decodes a base64 url-safe string to UTF-8."""
        if not data:
            return ""
        try:
            return base64.urlsafe_b64decode(data.encode("UTF-8")).decode("UTF-8")
        except Exception:
            return ""
