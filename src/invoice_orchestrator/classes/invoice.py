import pandas as pd


class Invoice:
    """
    Represents a single invoice extracted from Gmail.

    This class is instantiated from a row of the transformed invoices DataFrame.
    It is intended only for rows where new_invoice == True.

    Attributes:
        hash (str): The invoice hash.
        message_id (str): Gmail message identifier.
        thread_id (str): Gmail thread identifier.
        subject (str): Email subject.
        date_received (datetime): When the invoice was received.
        sender (str): Email sender.
        attachment_id (str): Attachment identifier.
        filename (str): Invoice PDF filename.
        pdf_local_path (str): Local path where the PDF is stored.
        invoice_count (int): Count of occurrences (raw).
        duplicated (bool): True if raw invoice_count > 1.
        new_invoice (bool): True if invoice is new (not found in Drive).
        common_invoice (bool): True if invoice exists in both sources.
        diff_filename_in_gmail (bool): True if multiple filenames exist in Gmail for this hash.
        diff_filename_between_sources (bool): True if Gmail and Drive filenames differ.
        is_latest (bool): True if the row is the latest record for this hash.
    """

    def __init__(self, row: dict):
        """
        Initializes an Invoice object from a dictionary representing a row.

        Args:
            row (dict): A dictionary (from itertuples()._asdict()) from the transformed invoices DataFrame.

        Raises:
            ValueError: If new_invoice is not True.
        """
        if not row.get('new_invoice', False):
            raise ValueError("Invoice row is not marked as new (new_invoice == True).")

        self.hash = row.get('hash')
        self.message_id = row.get('message_id')
        self.thread_id = row.get('thread_id')
        self.subject = row.get('subject')
        self.date_received = row.get('date_received')
        self.sender = row.get('sender')
        self.attachment_id = row.get('attachment_id')
        self.filename = row.get('filename')
        self.pdf_local_path = row.get('pdf_local_path')
        self.invoice_count = row.get('invoice_count')
        self.duplicated = row.get('duplicated')
        self.new_invoice = row.get('new_invoice')
        self.common_invoice = row.get('common_invoice')
        self.diff_filename_in_gmail = row.get('diff_filename_in_gmail')
        self.diff_filename_between_sources = row.get('diff_filename_between_sources')
        self.is_latest = row.get('is_latest')

    def __repr__(self):
        return (f"Invoice(hash={self.hash}, subject={self.subject}, "
                f"date_received={self.date_received}, filename={self.filename})")

    def process_ocr(self, ocr_extractor):
        """
        Processes the invoice PDF using the provided OCR extractor.

        Args:
            ocr_extractor: An object/function with method process_invoice(pdf_path: str)

        Returns:
            dict: OCR-extracted data.
        """
        if not self.pdf_local_path:
            raise ValueError("No PDF local path available for OCR processing.")
        return ocr_extractor.process_invoice(self.pdf_local_path)
