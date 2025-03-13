import logging

import pandas as pd
import spacy

from src.config import ConfigurationManager
from src.utils.embeddings import preprocess_text, calculate_embeddings, ensure_array
from src.utils.utils import replace_nan_values


def transform_param(df: pd.DataFrame, nlp_ca: spacy.Language, nlp_es: spacy.Language, config: ConfigurationManager,
                    logger: logging.Logger) -> pd.DataFrame:
    
    df = df.copy()
    df = replace_nan_values(df, logger)

    if config.semantic_search_cosine_similarity_filter:
        # Generate processed text
        df.loc[:, 'input_ca'] = df['description_ca'].apply(
            lambda x: preprocess_text(
                text=x,
                nlp_catalan=nlp_ca,
                nlp_spanish=nlp_es,
                n=config.etl_param_embedding_tokens
            )
        )
        # Generate processed text
        df.loc[:, 'input_es'] = df['description_es'].apply(
            lambda x: preprocess_text(
                text=x,
                nlp_catalan=nlp_ca,
                nlp_spanish=nlp_es,
                n=config.etl_param_embedding_tokens
            )
        )
        # Generate embeddings ca
        df = calculate_embeddings(
            df=df,
            column='input_ca',
            logger=logger,
            config=config
        )
        df['embedding_ca'] = df['embedding']
        # Generate embeddings es
        df = calculate_embeddings(
            df=df,
            column='input_es',
            logger=logger,
            config=config
        )
        df['embedding_es'] = df['embedding']
        df.drop(columns=['embedding'], inplace=True)
        df.loc[:, 'embedding_ca'] = df['embedding_ca'].apply(ensure_array)
        df.loc[:, 'embedding_es'] = df['embedding_es'].apply(ensure_array)
    else:
        df.loc[:, 'input_ca'] = None
        df.loc[:, 'input_es'] = None
        df.loc[:, 'embedding_ca'] = None
        df.loc[:, 'embedding_es'] = None

    return df
