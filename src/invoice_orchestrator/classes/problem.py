from typing import List, Dict
import pandas as pd
from src.invoice_orchestrator.classes.invoice import Invoice


class InvoiceProblem:
    """
    Represents the domain of new invoices to be processed.

    This class initializes a list of Invoice objects from the master merged DataFrame,
    including only rows where new_invoice == True.

    Attributes:
        invoices (List[Invoice]): List of new Invoice objects.
    """

    def __init__(self, data_model: Dict[str, pd.DataFrame]):
        """
        Initializes InvoiceProblem by creating Invoice objects for each new invoice.

        Args:
            data_model :
        """

        invoices = data_model['invoices'].copy()
        new_invoices_df = invoices[(invoices['new_invoice'] == True)&(invoices['is_earliest'] == True)]

        # Use itertuples for performance; convert each row to a dictionary.
        self.invoices: List[Invoice] = [Invoice(row._asdict()) for row in new_invoices_df.itertuples(index=False)]

    def __repr__(self):
        return f"InvoiceProblem({len(self.invoices)} new invoices)"
