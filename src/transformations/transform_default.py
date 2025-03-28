import pandas as pd
import numpy as np
from typing import Dict
from logging import Logger

def transform_default(key: str, all_inputs: Dict[str, pd.DataFrame], logger: Logger) -> pd.DataFrame:
    logger.info(f"Starting default transformation of table: {key}")
    df = all_inputs[key].copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip().str.lower()
            df[col] = df[col].replace({'nan': None, 'none': None, '': None})
        df[col] = df[col].replace({np.nan: None})
    return df