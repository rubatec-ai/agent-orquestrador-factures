from typing import List
import pandas as pd
from src.invoice_orchestrator.classes.invoice import Invoice


class InvoiceProblem:
    """
    Represents the problem domain for processing new invoices.

    This class initializes a list of Invoice objects from the transformed invoices DataFrame,
    filtering only rows where new_invoice == True.
    """

    def __init__(self, invoices_df: pd.DataFrame):
        """
        Initializes InvoiceProblem by creating Invoice objects from the DataFrame rows.
        Uses itertuples() for better performance.

        Args:
            invoices_df (pd.DataFrame): The transformed invoices DataFrame.
        """
        # Filter for new invoices only.
        new_invoices_df = invoices_df[invoices_df['new_invoice'] == True]
        # Use itertuples to iterate faster; _asdict() converts namedtuple to dict.
        self.invoices: List[Invoice] = [Invoice(row._asdict()) for row in new_invoices_df.itertuples(index=False)]

    def run_ocr_on_all(self, ocr_extractor):
        """
        Processes OCR on all new invoices using the given OCR extractor.

        Args:
            ocr_extractor: An object/function with method process_invoice(pdf_path: str).

        Returns:
            List[dict]: OCR results for each invoice.
        """
        results = []
        for invoice in self.invoices:
            try:
                ocr_data = invoice.process_ocr(ocr_extractor)
                results.append({
                    'hash': invoice.hash,
                    'ocr_data': ocr_data
                })
            except Exception as e:
                results.append({
                    'hash': invoice.hash,
                    'ocr_data': None,
                    'error': str(e)
                })
        return results

    def __repr__(self):
        return f"InvoiceProblem({len(self.invoices)} new invoices)"
