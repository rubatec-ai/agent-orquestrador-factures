import logging

from tqdm import tqdm
import pandas as pd
from typing import List, Dict, Any
from src.ai_summarizer.classes.agent import Agent
from src.ai_summarizer.classes.scope import Scope
from src.ai_summarizer.classes.master import Master
from src.ai_summarizer.classes.problem import Problem
from src.ai_summarizer.utils.cosine_similarity import compute_cosine_similarity
from src.config import ConfigurationManager
from src.utils.constants import COMBINED_PARAMETER


class SemanticSearch:
    """
    Orchestrates the semantic search process by comparing Scope and Master objects
    using an Agent to generate final answers from filtered paragraphs.

    Attributes:
        problem (Problem): The problem instance containing Scope and Master objects.
        config (ConfigurationManager): Config with thresholds, etc.
        agent (Agent): The Agent instance used for chat completions.
        logger (logging.Logger): For logging progress, warnings, etc.
        results (List[Dict[str, Any]]): To store the final results.

    Methods:
        run_search: Performs the semantic search.
        generate_output_dataframe: Produces the final output DataFrame.
    """

    def __init__(self, config: ConfigurationManager, problem: Problem, agent: Agent, logger: logging.Logger):
        self.problem = problem
        self.config = config
        self.agent = agent
        self.results: List[Dict[str, Any]] = []
        self.logger = logger

    def run_search(self) -> None:
        """
        1. If cosine_similarity_filter=True:
            - Compute similarity between each scope and each paragraph
            - Filter paragraphs below threshold
            - Fallback to top-N if no matches
            - Call the agent with the final filtered paragraphs

        2. If cosine_similarity_filter=False:
            - Skip similarity checks
            - Pass ALL paragraphs to the agent for each scope

        3. If all_against_all=True:
            - Combine all scope objects and all paragraphs into a single batch
            - Send the batch to the agent for processing
        """
        scope_objects: List[Scope] = self.problem.get_scope()
        master_objects: List[Master] = self.problem.get_master()

        if self.config.semantic_search_all_against_all:
            self.logger.info("Using all-against-all strategy. Combining all scopes and paragraphs into a single batch.")

            # Gather all paragraphs
            all_paragraphs = [m.paragraph for m in master_objects]

            # Combine all scope parameters and descriptions into a single prompt
            combined_scope_parameters = "\n".join(
                [f"Parameter: {scope.parameter}\nDescription: {scope.description_en}" for scope in scope_objects])

            # Generate a single answer for all scopes and paragraphs
            answer = self.agent.generate_answer(
                scope_parameter="Combined Parameters",
                scope_description=combined_scope_parameters,
                paragraphs=all_paragraphs
            )

            # Store the result for the combined parameter

            self.results.append({
                "parameter": COMBINED_PARAMETER,
                "answer": answer
            })

        elif self.config.semantic_search_cosine_similarity_filter:
            self.logger.info("Starting semantic search with cosine similarity.")

            # -----------------------------
            # Step 1: Compute similarities
            # -----------------------------
            similarity_results = compute_cosine_similarity(
                scope_objects, master_objects, self.logger
            )

            # Step 2: Filter results above threshold
            similarity_threshold = self.config.semantic_search_similarity_threshold
            filtered_results = [
                r for r in similarity_results
                if r["similarity"] >= similarity_threshold
            ]
            self.logger.info(
                f"Number of scope-master pairs after threshold: {len(filtered_results)}"
            )

            # Step 2b: Fallback if none pass threshold for a given scope
            for scope in scope_objects:
                scope_filtered = [
                    r for r in filtered_results
                    if r["parameter"] == scope.parameter
                ]
                if not scope_filtered:
                    # No matches above threshold → pick top-N
                    all_scope_results = [
                        r for r in similarity_results
                        if r["parameter"] == scope.parameter
                    ]
                    top_similar = sorted(
                        all_scope_results,
                        key=lambda x: x["similarity"],
                        reverse=True
                    )
                    top_similar = top_similar[: self.config.semantic_search_topn_similar]

                    self.logger.warning(
                        f"No matches found for '{scope.parameter}' above threshold. "
                        f"Including the top {len(top_similar)} paragraphs as fallback."
                    )
                    filtered_results.extend(top_similar)

            # Step 3: Group paragraphs by scope
            scope_paragraphs_map = {}
            for result in filtered_results:
                param = result["parameter"]
                paragraph_text = result["paragraph"]
                scope_paragraphs_map.setdefault(param, []).append(paragraph_text)

            # Step 4: For each scope, call the agent with the filtered paragraphs
            for scope in tqdm(scope_objects, desc="Processing final answers", unit="scope"):
                param = scope.parameter
                relevant_paragraphs = scope_paragraphs_map.get(param, [])

                answer = self.agent.generate_answer(
                    scope_parameter=param,
                    scope_description=scope.description_en,
                    paragraphs=relevant_paragraphs
                )

                self.results.append({
                    "parameter": param,
                    "answer": answer
                })

        else:
            # -------------------------------------------------------
            # If the filter is disabled, just use ALL paragraphs
            # -------------------------------------------------------
            self.logger.info("cosine_similarity_filter is OFF. Using ALL paragraphs for each scope.")

            # Gather every paragraph from master_objects
            all_paragraphs = [m.paragraph for m in master_objects]

            # Generate an answer for each scope using ALL paragraphs
            for scope in tqdm(scope_objects, desc="Processing final answers", unit="scope"):
                answer = self.agent.generate_answer(
                    scope_parameter=scope.parameter,
                    scope_description=scope.description_en,
                    paragraphs=all_paragraphs
                )
                self.results.append({
                    "parameter": scope.parameter,
                    "answer": answer
                })

        # Log total cost
        self.logger.info(f"Total cost for API calls: ${self.agent.total_cost:.6f}")

    def generate_output_dataframe(self) -> pd.DataFrame:
        """
        Generates the final output DataFrame from the search results.

        The DataFrame contains:
            - parameter:
            - answer:

        Returns:
            pd.DataFrame: The output DataFrame.
        """
        if not self.results:
            self.logger.warning("No results found. Ensure the semantic search ran correctly.")
            raise ValueError("No results found.")

        df = pd.DataFrame(self.results)

        return df

