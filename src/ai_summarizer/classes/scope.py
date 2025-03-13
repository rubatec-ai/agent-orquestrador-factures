from typing import Optional, Union, List

import numpy as np


class Scope:
    """
    Represents a scope item to be compared against the master items.

    Attributes:
        parameter (str): Unique identifier for the scope item.
        description_en (str): English description of the scope item.
        description_ca (str): Catalan description of the scope item.
        description_es (str): Spanish description of the scope item.
        embedding_ca (np.ndarray): Catalan numerical embedding for the scope item.
        embedding_es (np.ndarray): Spanish numerical embedding for the scope item.

    Methods:
        from_dataframe_row: Creates a Scope object from a DataFrame row.
        to_dict: Converts the Scope object to a dictionary.
    """

    def __init__(self, parameter: str, description_en: str, description_ca: str, description_es: str,
                 embedding_ca: np.array, embedding_es: np.array):
        self.parameter = parameter
        self.description_en = description_en
        self.description_ca = description_ca
        self.description_es = description_es
        self.embedding_ca = embedding_ca
        self.embedding_es = embedding_es

    @classmethod
    def from_dataframe_row(cls, row):
        """
        Factory method to create a Scope object from a DataFrame row.

        Args:
            row: A descriptions tuple from the `scope` DataFrame.

        Returns:
            Scope: An instance of the Scope class.
        """
        return cls(
            parameter=row.parameter,
            description_en=row.description_en,
            description_ca=row.description_ca,
            description_es=row.description_es,
            embedding_ca=row.embedding_ca,
            embedding_es=row.embedding_es
        )

    def to_dict(self):
        """
        Converts the Scope object to a dictionary.

        Returns:
            dict: A dictionary representation of the Scope object.
        """
        return {
            "parameter": self.parameter,
            "description_en": self.description_en,
            "description_ca": self.description_ca,
            "description_es": self.description_es,
            "embedding_ca": self.embedding_ca,
            "embedding_es": self.embedding_es
        }
