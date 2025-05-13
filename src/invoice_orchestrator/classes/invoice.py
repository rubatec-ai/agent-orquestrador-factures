from src.api_extractors.ocr_extractor import GoogleOCRExtractor
from src.api_extractors.openai_extractor import AIExtractor

class Invoice:
    """
    Represents a unified invoice, combining data from Gmail, Drive and Sheets.
    """
    def __init__(self, row: dict):
        """
        Initializes the Invoice object from a merged row dictionary.
        """
        if not row.get('new_invoice', False):
            raise ValueError("Invoice is not marked as new (new_invoice != True).")

        # Datos de Gmail
        self.invoice_hash = row.get('hash')
        self.date_received = row.get('date_received')
        self.sender = row.get('sender')
        self.invoice_filename = row.get('filename')
        self.pdf_local_path = row.get('pdf_local_path')
        self.is_latest = row.get('is_latest')

        # Campos que se rellenarán con OCR y OpenAI
        self.fr_proveedor = None
        self.name_proveedor = None
        self.id_proveedor = None
        self.canal_sie = None
        self.base = None
        self.iva_pct = None
        self.iva_eur = None
        self.total = None
        self.forma_pago = None
        self.due_date = None
        self.marca_temporal_ocr = None

        self.line_items = None
        self.vat = None

        # Enlaces a facturas en Drive
        self.web_view_link = None
        # Enlaces foto en Drive
        self.imagen = None

    def __repr__(self):
        return (f"Invoice(hash={self.invoice_hash}, "
                f"date_received={self.date_received}, "
                f"filename={self.invoice_filename})")

    def process_ocr(self, ocr_extractor: GoogleOCRExtractor, openai_extractor: AIExtractor):
        """
        Processes the invoice PDF using the provided OCR and OpenAI extractors.
        """
        if not self.pdf_local_path:
            raise ValueError("No PDF local path available for OCR processing.")
        self.ocr_data = ocr_extractor.process_invoice(
            pdf_path=self.pdf_local_path,
            openai_extractor=openai_extractor
        )
        return self.ocr_data
