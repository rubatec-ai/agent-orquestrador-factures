import pandas as pd
from typing import Dict

def transform_register(df: pd.DataFrame, all_inputs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    df = df.copy()
    return df