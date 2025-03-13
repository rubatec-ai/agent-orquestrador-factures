import logging
import os

import pandas as pd


def generate_scope_from_directory(directory: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Generates the 'scope' DataFrame from a directory of PDF files.

    Args:
        directory (str): Path to the directory containing PDF files.

    Returns:
        pd.DataFrame: A DataFrame with columns 'path', 'Name', and 'type'.
    """
    logger.info(f"Generating 'scope' table from directory: {directory}")

    # List all PDF files in the directory
    pdf_files = [f for f in os.listdir(directory) if f.endswith('.pdf')]

    # Create the DataFrame
    data = {
        'path': [os.path.join(directory, f) for f in pdf_files],
        'name': [os.path.splitext(f)[0] for f in pdf_files],  # Remove file extension
        'type': ['pdf'] * len(pdf_files)  # All files are PDFs
    }

    return pd.DataFrame(data)