import numpy as np
import pandas as pd


class Master:
    """
    Represents a single row from the 'master' DataFrame, encapsulating all its information.

    Attributes:
        id (str): Unique identifier for the master item.
        name (str):
        page (int): Page of the item.
        paragraph (str):
        embedding (np.array) : Embedded paragraph using an array of 3072 values (OpenAI: text-embedding-3-large)

    Methods:
        from_dataframe_row: Creates a Master object from a DataFrame row.
        to_dict: Converts the Master object to a dictionary.
    """

    def __init__(self, id: str, name: str, page: int, paragraph: str, lang: str, embedding: np.array):
        self.id = id
        self.name = name
        self.page = page
        self.paragraph = paragraph
        self.lang = lang
        self.embedding = embedding

    @classmethod
    def from_dataframe_row(cls, index, row):
        """
        Factory method to create a Master object from a DataFrame row.

        Args:
            index (int): The index of the DataFrame row.
            row (pd.Series): A row from the 'master' DataFrame.

        Returns:
            Master: An instance of the Master class.
        """
        unique_id = f"{row.name}_{index}_{row.page}"
        return cls(
            id=unique_id,
            name=str(row.name),
            page=int(row.page),
            paragraph=row.paragraph,
            lang=row.detected_lang,
            embedding=row.embedding
        )

    def to_dict(self):
        """
        Converts the Master object to a dictionary.

        Returns:
            dict: A dictionary representation of the Master object.
        """
        return {
            "id": self.id,
            "name": self.name,
            "page": self.page,
            "paragraph": self.paragraph,
            "lang": self.lang,
            "embedding": self.embedding
        }
