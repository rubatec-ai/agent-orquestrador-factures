import os
import logging
from datetime import datetime

from google.oauth2 import service_account
from google.cloud import documentai_v1 as documentai

import PyPDF2

from src.api_extractors.openai_extractor import AIExtractor
from src.config import ConfigurationManager
from src.utils.constants import EXTRACTED_DATA_INVOICE_PARSER  # Diccionario base con los campos esperados


class GoogleOCRExtractor:
    """
    Extracts key invoice data from PDF documents using Google Document AI.

    This class:
      - Connects to Document AI using the provided configuration.
      - Processes a given PDF with process_invoice(), which:
          * Reads the PDF.
          * Calls Document AI to extract entities.
          * Iterates through the entities extracting fields (using safe methods for normalized values).
          * Combines these fields with additional processing from an OpenAI extractor.
          * Returns a dictionary with the extracted data.
    """

    def __init__(self, config: ConfigurationManager) -> None:
        self._logger = logging.getLogger("GoogleOCRExtractor")
        self._logger.info("Initializing GoogleOCRExtractor...")
        self._config = config
        self._credentials_path = config.google_credentials_json_provisional
        self._project_id = config.documentai_project_id
        self._location = config.documentai_location
        self._processor_id = config.documentai_processor_id
        self._client = self._get_documentai_client()

    def _get_documentai_client(self):
        credentials = service_account.Credentials.from_service_account_file(self._credentials_path)
        api_endpoint = f"{self._location}-documentai.googleapis.com"
        client_options = {"api_endpoint": api_endpoint}
        return documentai.DocumentProcessorServiceClient(
            credentials=credentials, client_options=client_options
        )

    def _process_document_invoice_parser(self, pdf_path: str) -> dict:
        """
        Uses Document AI to process the PDF and extract invoice entities.
        Returns a dictionary with the extracted data.
        """
        self._logger.info(f"Running InvoiceParser extraction on {pdf_path}")

        # Leer contenido del PDF
        try:
            with open(pdf_path, "rb") as pdf_file:
                content = pdf_file.read()
        except Exception as e:
            self._logger.error(f"Error reading PDF: {e}")
            return {}

        # Construir el nombre del processor
        processor_name = f"projects/{self._project_id}/locations/{self._location}/processors/{self._processor_id}"
        document = {"content": content, "mime_type": "application/pdf"}
        request = {"name": processor_name, "raw_document": document}

        try:
            result = self._client.process_document(request=request)
            document_object = result.document
        except Exception as e:
            self._logger.error(f"Document AI processing failed: {e}")
            return {}

        # Empezamos con una copia base de los campos esperados
        extracted_data = EXTRACTED_DATA_INVOICE_PARSER.copy()
        # Para campos que pueden repetirse, usamos listas
        line_items_list = []
        vat_list = []

        # Iterar sobre las entidades del documento
        for entity in document_object.entities:
            entity_type = entity.type_.lower().strip() if entity.type_ else "unknown"
            mention_text = entity.mention_text.strip() if entity.mention_text else ""

            normalized_val = None
            if entity.normalized_value:
                try:
                    normalized_val = getattr(entity.normalized_value, "number_value", None)
                    if normalized_val is None:
                        normalized_val = getattr(entity.normalized_value, "text", None)
                except Exception as e:
                    self._logger.error(f"Error accessing normalized value for '{entity_type}': {e}")
                    normalized_val = None

            # Procesar según el tipo de entidad
            if entity_type == "line_item":
                item_data = {}
                for prop in entity.properties:
                    prop_type = prop.type_.lower().strip() if prop.type_ else "unknown_prop"
                    prop_text = prop.mention_text.strip() if prop.mention_text else ""
                    prop_norm = None
                    if prop.normalized_value:
                        try:
                            prop_norm = getattr(prop.normalized_value, "number_value", None)
                            if prop_norm is None:
                                prop_norm = getattr(prop.normalized_value, "text", None)
                        except Exception as e:
                            self._logger.error(f"Error in line item property '{prop_type}': {e}")
                    item_data[prop_type] = prop_norm if prop_norm is not None else prop_text
                line_items_list.append(item_data)
            elif entity_type == "vat":
                vat_data = {}
                for prop in entity.properties:
                    sub_type = prop.type_.lower().strip() if prop.type_ else "unknown_vat_prop"
                    sub_text = prop.mention_text.strip() if prop.mention_text else ""
                    sub_norm = None
                    if prop.normalized_value:
                        try:
                            sub_norm = getattr(prop.normalized_value, "number_value", None)
                            if sub_norm is None:
                                sub_norm = getattr(prop.normalized_value, "text", None)
                        except Exception as e:
                            self._logger.error(f"Error in VAT property '{sub_type}': {e}")
                    vat_data[sub_type] = sub_norm if sub_norm is not None else sub_text
                vat_list.append(vat_data)
            else:
                # Para campos simples, si hay valor normalizado se utiliza, sino se usa el texto reconocido.
                extracted_data[entity_type] = normalized_val if normalized_val is not None else mention_text

        # Guardar las listas en el diccionario final si hay datos
        if line_items_list:
            extracted_data["line_item"] = line_items_list
        if vat_list:
            extracted_data["vat"] = vat_list


        extracted_data["marca_temporal_ocr"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        return extracted_data

    def process_invoice(self, pdf_path: str, openai_extractor: AIExtractor) -> dict:
        """
        Procesa el PDF de una factura para extraer datos mediante Document AI y
        realiza procesamiento adicional con OpenAI si es necesario.

        Combina la extracción de Document AI y OpenAI en un solo diccionario.
        """
        self._logger.info(f"Processing invoice: {pdf_path}")

        # Extraer el texto completo del PDF (para utilizar en prompts u otros fines)
        pdf_text = self.extract_text_from_pdf(pdf_path)

        # Extraer datos con Document AI
        data_invoice_parser = self._process_document_invoice_parser(pdf_path)

        fields_openai = ['supplier_email', 'supplier_website', ]
        ocr_hints = {key: data_invoice_parser[key] for key in fields_openai if key in data_invoice_parser}

        # Extraer datos adicionales (casos edge) con OpenAI
        openai_invoice_fields = openai_extractor.extract_edge_case(pdf_text=pdf_text, ocr_extracted_data=ocr_hints)

        # Combinar ambos diccionarios, dando prioridad a los datos de Document AI
        ocr_data = {**data_invoice_parser, **openai_invoice_fields}

        # Agregar una marca temporal de la extracción
        ocr_data["marca_temporal_ocr"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return ocr_data

    @staticmethod
    def extract_text_from_pdf(pdf_path: str) -> str:
        """
        Extrae el texto completo de un PDF utilizando PyPDF2.
        """
        text = ""
        try:
            with open(pdf_path, "rb") as file:
                reader = PyPDF2.PdfReader(file)
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            logging.getLogger("GoogleOCRExtractor").error(f"Error extracting text from PDF: {e}")
        return text
