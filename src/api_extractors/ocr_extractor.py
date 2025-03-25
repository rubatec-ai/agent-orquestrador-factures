from typing import Dict
import pandas as pd
from logging import Logger
from google.oauth2 import service_account
from google.cloud import documentai_v1 as documentai

from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor


class GoogleOCRExtractor(BaseExtractor):
    """
    Extracts and processes OCR data using Google Document AI.

    This extractor loads service account credentials from a JSON file,
    creates a Document AI client, processes a document, and converts the
    result into a DataFrame.
    """

    def __init__(self, config: ConfigurationManager, logger: Logger) -> None:
        # Instead of an API key, load the JSON credentials.
        self._credentials_path = config.google_credentials_json  # Path to your credentials JSON.
        self._project_id = config.google_project_id
        self._location = config.documentai_location  # e.g., "us" or "eu"
        self._processor_id = config.documentai_processor_id
        self._client = self._get_documentai_client()
        logger.name = "GoogleOCRExtractor"
        super().__init__(config, logger)

    def _get_documentai_client(self):
        """
        Loads service account credentials from JSON and creates a Document AI client.
        """
        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path
        )
        client = documentai.DocumentProcessorServiceClient(credentials=credentials)
        return client

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Processes a document using Google Document AI and returns the OCR results in a DataFrame.

        For demonstration, this example processes a local PDF file.
        """
        input_file_path = self._config.documentai_input_file  # e.g., "path/to/document.pdf"
        with open(input_file_path, "rb") as document_file:
            document_content = document_file.read()

        # Build the full resource name of the processor.
        name = f"projects/{self._project_id}/locations/{self._location}/processors/{self._processor_id}"

        request = documentai.ProcessRequest(
            name=name,
            raw_document=documentai.RawDocument(
                content=document_content,
                mime_type="application/pdf"
            )
        )

        result = self._client.process_document(request=request)
        document_text = result.document.text

        # Create a DataFrame with the OCR result.
        df = pd.DataFrame([{"document_text": document_text}])
        return {"ocr_output": df}

    def clean_input_data(self):
        """
        Cleans and processes the OCR output data.

        For example, trimming extra whitespace and ensuring non-empty results.
        """
        df = self._raw_inputs.get("ocr_output")
        if df is not None and not df.empty:
            df["document_text"] = df["document_text"].str.strip()
            self._clean_inputs["ocr_output"] = df
