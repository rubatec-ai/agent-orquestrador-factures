import pandas as pd
from typing import Dict

def transform_gmail(df: pd.DataFrame, all_inputs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Puedes acceder a df, pero también a all_inputs['sage'], etc.
    return df