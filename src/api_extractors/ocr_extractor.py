from typing import Dict
import pandas as pd
import os
from logging import Logger
from google.oauth2 import service_account
from google.cloud import documentai_v1 as documentai

from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor


class GoogleOCRExtractor(BaseExtractor):
    """
    Extracts key invoice data from PDF documents using Google Document AI.

    This class loads service account credentials from a JSON file,
    creates a Document AI client, and processes a list of PDF files.
    It expects a DataFrame (provided via the configuration) that has a column
    "pdf_local_path" with the path of each PDF file.

    For each PDF, it sends the file to Document AI and extracts the OCR text.
    The resulting DataFrame (under the key "ocr_output") contains one row per PDF with:
      - pdf_local_path: Local path of the PDF file.
      - document_text: Full OCR text extracted from the document.

    (You can later extend this class to parse additional invoice parameters.)
    """

    def __init__(self, config: ConfigurationManager, logger: Logger,input_df: pd.DataFrame = None) -> None:
        self._input_df = input_df
        self._credentials_path = config.google_credentials_json
        self._project_id = config.documentai_project_id
        self._location = config.documentai_location
        self._processor_id = config.documentai_processor_id
        self._client = self._get_documentai_client()
        logger.name = "GoogleOCRExtractor"
        super().__init__(config, logger)

    def _get_documentai_client(self):
        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path
        )
        client = documentai.DocumentProcessorServiceClient(credentials=credentials)
        return client

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Processes a list of PDF files provided via an external DataFrame (self._input_df).
        For each row, it extracts the OCR text using Document AI and builds a new DataFrame
        with one row per document.
        """
        if self._input_df is None or self._input_df.empty:
            raise Exception("No se proporcionó un DataFrame de entrada para Document AI.")

        processed_docs = []
        for idx, row in self._input_df.iterrows():
            pdf_path = row.get("pdf_local_path")
            if pdf_path and os.path.exists(pdf_path):
                try:
                    params = self._process_document(pdf_path)
                    processed_doc = {
                        "pdf_local_path": pdf_path,
                        "param1": params,
                        "param2": params
                    }
                    processed_docs.append(processed_doc)
                except Exception as e:
                    self._logger.warning(f"Error procesando {pdf_path}: {e}")
            else:
                self._logger.warning(f"No se encontró el archivo: {pdf_path}")

        df = pd.DataFrame(processed_docs)
        return {"ocr_output": df}

    def _process_document(self, pdf_path: str) -> str:
        """
        Processes a single PDF document using Document AI and returns the extracted OCR text.
        """
        return pdf_path

    def clean_input_data(self):
        """
        Cleans the OCR output data by trimming whitespace from the extracted text.
        """
        df = self._raw_inputs.get("ocr_output")
        if df is not None and not df.empty:
            df["document_text"] = df["document_text"].str.strip()
            self._clean_inputs["ocr_output"] = df
