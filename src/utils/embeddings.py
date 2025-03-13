import logging
from typing import List

import numpy as np
import pandas as pd
import spacy
from openai import OpenAI
from langdetect import detect, LangDetectException

from src.config import ConfigurationManager


def calculate_embeddings(df: pd.DataFrame,column:str, logger: logging.Logger, config: ConfigurationManager) -> pd.DataFrame:
    client = OpenAI(api_key=config.agent_api_key)
    df["embedding"] = df[column].apply(lambda x: get_embedding_vec(
        client=client,
        config=config,
        logger=logger,
        input=x
    ))

    return df


def get_embedding_vec(client, config: ConfigurationManager, logger: logging.Logger, input: str) -> List[float] | None:
    try:
        response = client.embeddings.create(input=input, model=config.etl_param_embedding_model)
        return response.data[0].embedding
    except Exception as e:
        logger.debug(f"Error generating embedding for input '{input}': {e}")
        return None

def detect_language(text: str) -> str:
    """
    Detects the language of the given text.

    Args:
        text (str): Text to detect language for.

    Returns:
        str: Detected language code ('ca' for Catalan, 'es' for Spanish).
             Defaults to 'es' if detection fails or language is neither Catalan nor Spanish.
    """
    try:
        lang = detect(text)
        if lang in ['ca', 'es']:
            return lang
        else:
            return 'ca'  # Default to Spanish if other language detected
    except LangDetectException:
        return 'ca'  # Default to Spanish if detection fails


def preprocess_text(text: str, nlp_catalan: spacy.Language, nlp_spanish: spacy.Language, n: int = 100) -> str:
    """
    Preprocesses text by removing stopwords, punctuation, extra spaces,
    and converting to lowercase while keeping numbers and units.

    Args:
        text (str): The text to preprocess.
        nlp_catalan (Language): Loaded spaCy Catalan NLP model.
        nlp_spanish (Language): Loaded spaCy Spanish NLP model.
        n (int): Maximum number of tokens to include in the processed text.

    Returns:
        str: The processed text.
    """
    if not isinstance(text, str):
        return ''

    # Detect language
    language = detect_language(text)
    nlp = nlp_catalan if language == 'ca' else nlp_spanish

    # Tokenize text using spaCy
    doc = nlp(text)

    # Process tokens: keep only words and numbers, remove stopwords & punctuation
    tokens = [
        token.text.lower()
        for token in doc
        if not token.is_stop and not token.is_punct and not token.is_space
    ]

    # Return the first N tokens as the processed text
    return " ".join(tokens[:n])


def ensure_array(embedding):
    if isinstance(embedding, list):
        return np.array(embedding, dtype=np.float32)
    elif isinstance(embedding, np.ndarray):
        return embedding
    else:
        raise TypeError(f"Invalid embedding type: {type(embedding)}")