import pandas as pd
from typing import Dict
from logging import Logger

def transform_register(df: pd.DataFrame, all_inputs: Dict[str, pd.DataFrame], logger: Logger) -> pd.DataFrame:
    df = df.copy()
    return df