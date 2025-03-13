from typing import List
import pandas as pd
from src.ai_summarizer.classes.scope import Scope
from src.ai_summarizer.classes.master import Master


class Problem:
    """
    Represents the semantic search problem, managing scope and master data
    and initializing objects for further processing.

    Attributes:
        scope_objects (List[Scope]): List of Scope objects derived from the scope DataFrame.
        master_objects (List[Master]): List of Master objects derived from the master DataFrame.

    Methods:
        initialize_scope: Converts the scope DataFrame into a list of Scope objects.
        initialize_master: Converts the master DataFrame into a list of Master objects.
        get_scope: Returns the list of Scope objects.
        get_master: Returns the list of Master objects.
    """

    def __init__(self, scope_df: pd.DataFrame, master_df: pd.DataFrame):
        """
        Initializes the Problem with scope and master data.

        Args:
            scope_df (pd.DataFrame): DataFrame containing scope data.
            master_df (pd.DataFrame): DataFrame containing master data.
        """
        self.scope_objects: List[Scope] = self.initialize_scope(scope_df)
        self.master_objects: List[Master] = self.initialize_master(master_df)

    @staticmethod
    def initialize_scope(scope_df: pd.DataFrame) -> List[Scope]:
        """
        Converts the scope DataFrame into a list of Scope objects.

        Args:
            scope_df (pd.DataFrame): DataFrame containing scope data.

        Returns:
            List[Scope]: List of Scope objects.
        """
        return [
            Scope.from_dataframe_row(row)
            for index, row in enumerate(scope_df.itertuples(index=True))
        ]

    @staticmethod
    def initialize_master(master_df: pd.DataFrame) -> List[Master]:
        """
        Converts the master DataFrame into a list of Master objects.

        Args:
            master_df (pd.DataFrame): DataFrame containing master data.

        Returns:
            List[Master]: List of Master objects.        """

        return [
            Master.from_dataframe_row(index, row)
            for index, row in enumerate(master_df.itertuples(index=True))
        ]

    def get_scope(self) -> List[Scope]:
        """
        Returns the list of Scope objects.

        Returns:
            List[Scope]: List of Scope objects.
        """
        return self.scope_objects

    def get_master(self) -> List[Master]:
        """
        Returns the list of Master objects.

        Returns:
            List[Master]: List of Master objects.
        """
        return self.master_objects
