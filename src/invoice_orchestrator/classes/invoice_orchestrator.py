import os
import logging
import re
from typing import Dict, List

import pandas as pd

from src.api_extractors.drive_manager import DriveManager
from src.api_extractors.gmail_manager import GmailManager
from src.api_extractors.ocr_extractor import GoogleOCRExtractor
from src.api_extractors.openai_extractor import AIExtractor
from src.api_extractors.sheets_manager import GoogleSheetsManager
from src.config import ConfigurationManager
from src.invoice_orchestrator.classes.invoice import Invoice
from src.invoice_orchestrator.classes.problem import InvoiceProblem
from src.invoice_orchestrator.utils.utils import get_field
from src.utils.constants import CRITICAL_FIELDS_LINE_ITEMS, EXTRACTED_DATA_INVOICE_PARSER, EXTRACTED_DATA_OPENAI, \
    INVALID_CANAL_SIE_VALUES, EMAIL_INVALID_CANAL_SUBJECT, EMAIL_INVALID_CANAL_BODY
from src.utils.utils import parse_currency


class InvoiceOrchestrator:
    """
    Orchestrates the invoice processing workflow.
    """

    def __init__(self,
                 problem: InvoiceProblem,
                 config: ConfigurationManager,
                 openai_extractor: AIExtractor,
                 ocr_extractor: GoogleOCRExtractor,
                 drive_manager: DriveManager,
                 sheets_manager: GoogleSheetsManager,
                 gmail_manager: GmailManager,
                 logger: logging.Logger = None):

        self.problem = problem
        self.config = config
        self.openai_extractor = openai_extractor
        self.ocr_extractor = ocr_extractor
        self.drive_manager = drive_manager
        self.sheets_manager = sheets_manager
        self.gmail_manager = gmail_manager
        self.logger = logger or logging.getLogger("InvoiceOrchestrator")

        self.raw_ocr_results = []
        self.line_results = []

    def format_row(self, row: List) -> List[str]:
        """
        Formats all numeric values in a row to strings with a comma as the decimal separator.
        Non-numeric values are left unchanged. Converts None to empty strings.
        """

        def fmt_float(val):
            if isinstance(val, (float, int)):
                return f"{val:.2f}".replace('.', ',')
            return val if val is not None else ""  # Convert None to empty string

        return [str(fmt_float(item)) if item is not None else "" for item in row]

    def is_valid_line_item(self, line_items: Dict) -> bool:
        """
        Validates if a line item has at least 2 critical fields and a description.
        Critical fields: 'line_item/amount', 'line_item/quantity', 'line_item/unit_price'.
        """
        valid_fields = sum(1 for field in CRITICAL_FIELDS_LINE_ITEMS
                           if line_items.get(field) not in [None, "", 0])
        has_description = line_items.get('line_item/description') not in [None, ""]
        return valid_fields >= 2 and has_description

    def process_invoice_orchestra(self, invoice: Invoice):
        """
        Processes a single invoice:
        - Extracts OCR and OpenAI data.
        - Updates invoice fields.
        - Sends an email notification if invoice.canal_sie is "desconocido".
        - Uploads the PDF to Google Drive.
        - Collects raw OCR data and line items for later writing to Google Sheets.

        Args:
            invoice: The invoice object to process.
        """
        try:
            # Extract OCR and OpenAI data
            ocr_data = invoice.process_ocr(
                ocr_extractor=self.ocr_extractor,
                openai_extractor=self.openai_extractor
            )
            self.logger.debug(f"OCR data extracted for {invoice.invoice_filename}: {ocr_data}")

            # Update invoice fields
            invoice.fr_proveedor = get_field(ocr_data, ocr_key='invoice_id', openai_key='fr_proveedor')
            invoice.name_proveedor = get_field(ocr_data, ocr_key= 'supplier_name', openai_key= 'proveedor')
            invoice.id_proveedor = get_field(ocr_data, ocr_key= 'supplier_tax_id', openai_key= 'supplier_id')
            invoice.canal_sie = ("desconocido" if ocr_data.get("canal_sie") in INVALID_CANAL_SIE_VALUES
                                 else str(ocr_data.get("canal_sie", "")))
            invoice.base = parse_currency(ocr_data.get("net_amount", ""))
            invoice.iva_eur = parse_currency(ocr_data.get("total_tax_amount", ""))
            invoice.total = parse_currency(ocr_data.get("total_amount", ""))

            # Calculate IVA percentage only if base and total are valid
            if invoice.base and invoice.total:
                invoice.iva_pct = round((invoice.total / invoice.base - 1) * 100, 2)  # As percentage
            else:
                invoice.iva_pct = None

            invoice.forma_pago = get_field(ocr_data, ocr_key="payment_terms", openai_key="forma_pago")  # Fixed syntax error
            invoice.due_date = ocr_data.get("due_date")
            invoice.marca_temporal_ocr = ocr_data.get("marca_temporal_ocr")

            # Extract line items
            invoice.line_items = ocr_data.get("line_item", [{}])[0] if ocr_data.get("line_item") else {}
            self.logger.debug(f"Line items for {invoice.invoice_filename}: {invoice.line_items}")

            # Store raw OCR data for later review
            raw_ocr_entry = {'invoice_hash': invoice.invoice_hash, **ocr_data}
            self.raw_ocr_results.append(raw_ocr_entry)
            self.logger.info(f"Added to raw_ocr_results: {len(self.raw_ocr_results)} items")

            # Build and validate line item
            if self.is_valid_line_item(invoice.line_items):
                line_item = {
                    'filename': invoice.invoice_filename,
                    'supplier': invoice.name_proveedor,
                    'line_item/product_code': invoice.line_items.get('line_item/product_code', ""),
                    'line_item/unit': str(invoice.line_items.get('line_item/unit', "UN")),
                    'line_item/description': invoice.line_items.get('line_item/description', ""),
                    'line_item/unit_price': parse_currency(str(invoice.line_items.get('line_item/unit_price', "0"))),
                    'line_item/quantity': parse_currency(str(invoice.line_items.get('line_item/quantity', "0"))),
                    'line_item/amount': parse_currency(str(invoice.line_items.get('line_item/amount', "0"))),
                    'md5Checksum': invoice.invoice_hash,
                    'marca_temporal': invoice.marca_temporal_ocr
                }
                self.line_results.append(line_item)
                self.logger.info(f"Added to line_results: {len(self.line_results)} items")
            else:
                self.logger.debug(
                    f"Line item skipped for {invoice.invoice_filename}: insufficient data or no description")

            # Envío automático de correo si canal_sie es "desconocido".
            if (invoice.canal_sie == "desconocido") & self.config.auto_claim_canal:
                self.logger.info(
                    f"Invoice {invoice.invoice_filename} has 'desconocido' in canal_sie. Sending notification email to {invoice.sender}.")

                subject = f"{EMAIL_INVALID_CANAL_SUBJECT}: Revisar **{invoice.invoice_filename}**"
                sent_message = self.gmail_manager.send_email(
                    recipient=invoice.sender,
                    subject=subject,
                    body_text=EMAIL_INVALID_CANAL_BODY,
                    attachment_path=invoice.pdf_local_path,
                )
                self.logger.info(f"Notification email sent, message ID: {sent_message.get('id', 'N/A')}")

            # Upload PDF to Google Drive

            #canal = invoice.canal_sie if invoice.canal_sie else "desconocido"
            #target_relative_path = canal if re.fullmatch(r"\d{4}", canal) else "desconocido"

            file_metadata = self.drive_manager.upload_pdf(
                invoice.pdf_local_path,
                target_relative_path=None
            )
            invoice.web_view_link = file_metadata.get("webViewLink", "")

            # Generar preview y subirla a Drive
            preview_path = self.drive_manager.generate_preview_image(invoice.pdf_local_path)
            if preview_path:
                # 1) Subimos el PNG generado
                uploaded = self.drive_manager.upload_image(preview_path)
                # 2) Dejamos en invoice.imagen sólo el nombre de fichero (para Sheets/AppSheet)
                invoice.imagen = os.path.basename(preview_path)
            else:
                self.logger.warning(f"No se generó preview para {invoice.pdf_local_path}")
                invoice.imagen = ""

        except Exception as e:
            self.logger.error(f"Error processing invoice {invoice.invoice_hash}: {e}", exc_info=True)
            raise  # Raise to debug full stack trace

    def write_to_sheets(self):
        """
        Writes collected data to Google Sheets:
        - Writes invoice data to the "registro" sheet.
        - Writes line items to the "line_items" sheet.
        - Writes raw OCR data to the "ocr_data" sheet.
        """
        # Write to "registro"
        if self.problem.invoices:
            registro_rows = []
            for invoice in self.problem.invoices:
                registro_rows.append(self.format_row([
                    invoice.web_view_link,
                    invoice.fr_proveedor,
                    invoice.name_proveedor,
                    invoice.id_proveedor,
                    invoice.date_received.strftime("%Y-%m-%d %H:%M:%S") if invoice.date_received else "",
                    invoice.sender,
                    invoice.invoice_filename,
                    invoice.canal_sie,
                    invoice.base,
                    invoice.iva_pct if invoice.iva_pct is not None else "",
                    invoice.iva_eur,
                    invoice.total,
                    invoice.forma_pago,
                    invoice.due_date,
                    invoice.marca_temporal_ocr,
                    invoice.invoice_hash,
                    invoice.imagen,
                ]))
            self.sheets_manager.append_row(rows_data=registro_rows, sheet_name="registro", sheet_range="A1")
            self.logger.info(f"Wrote {len(registro_rows)} rows to 'registro' sheet")

        # Write to "line_items"
        if self.line_results:
            line_item_rows = []
            for line_result in self.line_results:
                line_item_rows.append(self.format_row([
                    line_result.get('md5Checksum', ""),
                    line_result.get('marca_temporal', ""),
                    line_result.get('line_item/product_code', ""),
                    line_result.get('line_item/amount', ""),
                    line_result.get('line_item/quantity', ""),
                    line_result.get('line_item/unit', ""),
                    line_result.get('line_item/unit_price', ""),
                    line_result.get('line_item/description', ""),
                ]))
            self.sheets_manager.append_row(rows_data=line_item_rows, sheet_name="line_items", sheet_range="A1")
            self.logger.info(f"Wrote {len(line_item_rows)} rows to 'line_items' sheet")

        # Write to "ocr_data"
        if self.raw_ocr_results:
            base_keys = list(EXTRACTED_DATA_INVOICE_PARSER.keys())
            extra_keys = EXTRACTED_DATA_OPENAI
            final_keys = ['invoice_hash'] + base_keys + extra_keys
            formatted_ocr_rows = []
            for item in self.raw_ocr_results:
                row = [item.get(key, "") for key in final_keys]
                formatted_ocr_rows.append(self.format_row(row))
            self.sheets_manager.append_row(rows_data=formatted_ocr_rows, sheet_name="ocr_data", sheet_range="A1")
            self.logger.info(f"Wrote {len(formatted_ocr_rows)} rows to 'ocr_data' sheet")

    def run(self) -> Dict[str, pd.DataFrame]:
        """
        Processes all invoices in the problem:
        - Runs OCR and OpenAI extraction.
        - Updates invoice objects.
        - Uploads PDFs to Google Drive.
        - Writes data to Google Sheets.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary containing DataFrames for raw OCR data and line items.
        """
        # Process each invoice
        for invoice in self.problem.invoices:
            self.logger.info(f"Processing invoice: {invoice.invoice_filename}")
            self.process_invoice_orchestra(invoice=invoice)

        # Write data to Google Sheets
        self.write_to_sheets()

        # Log final state
        self.logger.info(f"Final raw_ocr_results size: {len(self.raw_ocr_results)}")
        self.logger.info(f"Final line_results size: {len(self.line_results)}")

        # Return DataFrames
        result = {
            'ocr_raw': pd.DataFrame(self.raw_ocr_results),
            'line_raw': pd.DataFrame(self.line_results),
        }
        self.logger.info(
            f"Returning solution with ocr_raw rows: {len(result['ocr_raw'])}, line_raw rows: {len(result['line_raw'])}")
        return result
