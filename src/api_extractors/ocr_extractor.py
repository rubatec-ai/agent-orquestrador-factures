import base64
import os
import logging
from datetime import datetime

from google.oauth2 import service_account
from google.cloud import documentai_v1 as documentai
import io

from pdf2image import convert_from_path

from src.api_extractors.openai_extractor import AIExtractor
from src.config import ConfigurationManager
from src.utils.constants import EXTRACTED_DATA_INVOICE_PARSER, \
    DEFAULT_PARAMETERS_TO_SEARCH, MAX_WORDS_FIRST_PAGE  # Diccionario base con los campos esperados


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
        self._credentials_path = config.google_credentials_json
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

    def generate_image_data_url(self, pdf_path: str) -> str:
        """
        Generates a PNG image from the first page of the PDF and returns a base64-encoded data URL.

        Args:
            pdf_path: Path to the PDF file.

        Returns:
            str: Data URL in the format "data:image/png;base64,<base64_string>" or empty string if failed.
        """
        try:
            images = convert_from_path(pdf_path, first_page=1, last_page=1)
            image = images[0]
            buffer = io.BytesIO()
            image.save(buffer, format='PNG')
            image_bytes = buffer.getvalue()
            base64_image = base64.b64encode(image_bytes).decode('utf-8')
            data_url = f"data:image/png;base64,{base64_image}"
            self._logger.info(f"Generated image data URL for {pdf_path}")
            return data_url
        except Exception as e:
            self._logger.error(f"Error generating image for {pdf_path}: {e}")
            return ""

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
        full_text = document_object.text if document_object.text else ""

        # Lógica para recortar texto según páginas
        words = full_text.split()[:MAX_WORDS_FIRST_PAGE]

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
                # Para campos simples, si hay valor normalizado se utiliza, si no se usa el texto reconocido.
                extracted_data[entity_type] = normalized_val if normalized_val is not None else mention_text

        # Guardar las listas en el diccionario final si hay datos
        if line_items_list:
            extracted_data["line_item"] = line_items_list
        if vat_list:
            extracted_data["vat"] = vat_list

        extracted_data["marca_temporal_ocr"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        extracted_data["text"] = " ".join(words)

        return extracted_data

    def process_invoice(self, pdf_path: str, openai_extractor: AIExtractor) -> dict:
        """
        Processes a PDF invoice to extract data using Document AI and OpenAI with an image.

        Args:
            pdf_path: Path to the PDF file.
            openai_extractor: Instance of AIExtractor for additional processing.

        Returns:
            dict: Combined extracted data from Document AI and OpenAI.
        """
        self._logger.info(f"Processing invoice: {pdf_path}")

        # Paso 1: Extraer datos con Document AI
        try:
            data_invoice_parser = self._process_document_invoice_parser(pdf_path)
        except Exception as e:
            self._logger.error(f"Error al procesar Document AI: {e}")
            data_invoice_parser = {}

        # Obtener el texto extraído por Document AI
        pdf_text = data_invoice_parser.get("text", "")

        # Paso 2: Generar URL de datos de la imagen
        image_data_url = self.generate_image_data_url(pdf_path)

        # Verificar si hay datos suficientes para continuar
        if not pdf_text and not data_invoice_parser and not image_data_url:
            self._logger.warning("No text, structured data, or image extracted. Finalizing processing.")
            return {"marca_temporal_ocr": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

        # Paso 3: Preparar datos para OpenAI
        fields_openai = ['supplier_email', 'supplier_website']
        ocr_hints = {key: data_invoice_parser.get(key) for key in fields_openai if key in data_invoice_parser}

        # Paso 4: Extraer datos adicionales con OpenAI usando la imagen
        if image_data_url:
            try:
                openai_invoice_fields = openai_extractor.extract_edge_case(
                    image_data_url=image_data_url,
                    ocr_extracted_data=ocr_hints
                )
            except Exception as e:
                self._logger.error(f"Error al procesar OpenAI: {e}")
                openai_invoice_fields = DEFAULT_PARAMETERS_TO_SEARCH
        else:
            self._logger.warning("No hay datos suficientes para OpenAI. Saltando este paso.")
            openai_invoice_fields = DEFAULT_PARAMETERS_TO_SEARCH

        # Paso 5: Combinar ambos diccionarios
        ocr_data = {**data_invoice_parser, **openai_invoice_fields,
                    "marca_temporal_ocr": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

        return ocr_data