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