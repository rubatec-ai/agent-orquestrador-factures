import logging
import re

import pandas as pd

from src.api_extractors.drive_manager import DriveManager
from src.api_extractors.ocr_extractor import GoogleOCRExtractor
from src.api_extractors.openai_extractor import AIExtractor
from src.api_extractors.sheets_manager import GoogleSheetsManager
from src.invoice_orchestrator.classes.problem import InvoiceProblem

class InvoiceOrchestrator:
    """
    Orchestrates the invoice processing workflow.
    """
    def __init__(self,
                 problem: InvoiceProblem,
                 openai_extractor: AIExtractor,
                 ocr_extractor: GoogleOCRExtractor,
                 drive_manager: DriveManager,
                 sheets_manager: GoogleSheetsManager,
                 logger: logging.Logger = None):

        self.problem = problem
        self.openai_extractor = openai_extractor
        self.ocr_extractor = ocr_extractor
        self.drive_manager = drive_manager
        self.sheets_manager = sheets_manager
        self.logger = logger or logging.getLogger("InvoiceOrchestrator")
        self.ocr_df = pd.DataFrame()

    def run(self) -> pd.DataFrame:
        """
        Process each invoice:
          1. Ejecuta OCR en el PDF.
          2. Actualiza el objeto Invoice con los datos extraídos.
          3. Sube el PDF a Drive.
          4. Añade una nueva fila en Sheets.
          5. Recoge los datos OCR en un DataFrame.
        """
        raw_ocr_results = []

        for invoice in self.problem.invoices:
            try:
                self.logger.info(f"Processing invoice with hash: {invoice.invoice_hash}")
                # Extraer datos OCR + OpenAI
                ocr_data = invoice.process_ocr(
                    ocr_extractor=self.ocr_extractor,
                    openai_extractor=self.openai_extractor
                )

                # Almacenar datos OCR para consulta posterior
                raw_ocr_results.append({
                    'invoice_hash': invoice.invoice_hash,
                    **ocr_data
                })

                # Actualizar el invoice con campos extraídos
                invoice.fr_proveedor = ocr_data.get("fr_proveedor", "invoice_id")
                invoice.proveedor = ocr_data.get("proveedor", "supplier_name")
                invoice.canal_sie = str(ocr_data.get("canal_sie"))
                invoice.base = ocr_data.get("net_amount")
                invoice.iva_eur = ocr_data.get("total_tax_amount")
                invoice.total = ocr_data.get("total_amount")
                invoice.iva_pct = (invoice.total/invoice.base)-1
                invoice.forma_pago = ocr_data.get("forma_pago", "payment_terms")
                invoice.due_date = ocr_data.get("due_date")
                invoice.marca_temporal_ocr = ocr_data.get("marca_temporal_ocr")

                # Subir PDF a Drive y obtener el enlace
                canal = str(invoice.canal_sie) if invoice.canal_sie is not None else ""
                if re.fullmatch(r"\d{4}", canal):
                    target_relative_path = canal
                else:
                    target_relative_path = "desconocido"

                file_metadata = self.drive_manager.upload_pdf(
                    invoice.pdf_local_path,
                    target_relative_path=target_relative_path
                )
                invoice.web_view_link = file_metadata.get("webViewLink", "")

                # Preparar la fila para Google Sheets
                invoice_row = {
                    "web_view_link": invoice.web_view_link,
                    "fr_proveedor": invoice.fr_proveedor,
                    "proveedor": invoice.proveedor,
                    "date_received": invoice.date_received,
                    "sender": invoice.sender,
                    "invoice_filename": invoice.invoice_filename,
                    "canal_sie": invoice.canal_sie,
                    "base": invoice.base,
                    "iva_pct": invoice.iva_pct,
                    "iva_eur": invoice.iva_eur,
                    "total": invoice.total,
                    "due_date": invoice.due_date,
                    "forma_pago": invoice.forma_pago,
                    "marca_temporal_ocr": invoice.marca_temporal_ocr,
                    "invoice_hash": invoice.invoice_hash,
                }

                success = self.sheets_manager.append_invoice_row(invoice_row)
                if not success:
                    self.logger.error(f"Failed to append invoice {invoice.invoice_hash} to Google Sheets.")

            except Exception as e:
                self.logger.error(f"Error processing invoice {invoice.invoice_hash}: {e}")

        self.ocr_df = pd.DataFrame(raw_ocr_results)
        self.logger.info("Raw OCR data has been stored in self.ocr_df.")
        return self.ocr_df
