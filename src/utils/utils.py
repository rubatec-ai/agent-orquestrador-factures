import pandas as pd
import logging

def replace_nan_values(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Replaces string 'nan' values with None in all columns of the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to process.
        logger (logging.Logger): Logger for logging.

    Returns:
        pd.DataFrame: DataFrame with 'nan' strings replaced by None.
    """
    logger.debug("Replacing 'nan' strings with None in all columns.")
    for col in df.columns:
        df[col] = df[col].replace('nan', None)
    return df

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