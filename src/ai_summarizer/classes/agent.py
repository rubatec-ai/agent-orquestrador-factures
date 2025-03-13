import logging

import openai
from typing import Dict, Any, Optional, List
from src.utils.constants import PROMPT_TASK, MODELS_COST, SYSTEM_PROMPT_ANALYSIS


class Agent:
    """
    Responsible for interacting with the OpenAI API to perform semantic comparisons
    between rows from the `scope` and `master` DataFrames.

    Attributes:
        model (str): The OpenAI model to use (e.g., "gpt-4").
        api_key (str): API key to authenticate with OpenAI.
        temperature (float): Creativity parameter for the model (range: 0-1).
        max_tokens (int): Maximum number of tokens the model can return.
        logger (logging.Logger): Logger to record events and debug information.

    Methods:
        generate_reason: Generates a reason explaining the similarity between two rows.
        compare_rows: Compares two rows and returns a similarity score and explanation.
        call_openai_api: Internal method to send requests to the OpenAI API.
    """

    def __init__(self, model: str, api_key: str, logger: logging.Logger, temperature: float = 0.7, max_tokens: int = 100):
        self.model = model
        self.api_key = api_key
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.total_cost = 0.0
        self.logger = logger
        self.client = openai.OpenAI(api_key=self.api_key)

    def generate_answer(self, scope_parameter: str, scope_description: str, paragraphs: List[str]) -> Optional[str]:
        """
        Creates a single prompt that includes:
          - The scope definition (what you want to find)
          - The relevant paragraphs (separated by \n)
        Sends this prompt to OpenAI, returning the model's answer.
        """
        # Build a dynamic prompt combining scope definition and all paragraphs
        dynamic_prompt = (
            "#### Task ####\n"
            f"{PROMPT_TASK}\n"
            "----------------------------------------------\n"
            f"Parameter to find: {scope_parameter}\n"
            "----------------------------------------------\n"
            f"Parameter instructions:\n{scope_description}\n"
            "----------------------------------------------\n"
            "Below are the paragraphs:\n\n"
        )

        # Add each paragraph separated by new line
        for i, paragraph in enumerate(paragraphs, start=1):
            dynamic_prompt += f"Paragraph {i}: {paragraph}\n\n"

        # Send the prompt to OpenAI Chat Completion
        response = self.call_openai_api(dynamic_prompt)
        if not response:
            return None

        # Extract the text from the response
        answer = response.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        return answer

    def call_openai_api(self, prompt: str) -> Dict[str, Any]:
        """
        Sends a prompt to the OpenAI API and returns the response.

        Args:
            prompt (str): The input prompt.

        Returns:
            Dict[str, Any]: The API response.
        """
        try:
            self.logger.info("Sending request to OpenAI API.")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_ANALYSIS},
                    {"role": "user", "content": prompt},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            self.logger.info("Request completed successfully.")

            # Convert the response object to a dictionary
            response_dict: Dict[str, Any] = response.to_dict()

            # Extract usage details as a dictionary
            usage: Dict[str, int] = response_dict.get("usage", {})
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)

            # Calculate and log cost
            self.total_cost += self.calculate_cost(self.model, input_tokens, output_tokens)

            return response_dict

        except openai.APIConnectionError as e:
            self.logger.error(f"Connection error: {e}")
        except openai.AuthenticationError as e:
            self.logger.error(f"Authentication failed: {e}")
        except openai.RateLimitError as e:
            self.logger.error(f"Rate limit exceeded: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        return {}

    def calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """
        Calculates the cost of an API call based on the number of tokens and the model used.

        Args:
            model (str): The OpenAI model being used (e.g., "gpt-4o-mini").
            input_tokens (int): Number of input tokens.
            output_tokens (int): Number of output tokens.

        Returns:
            float: Total cost in USD.
        """

        # Get the cost details for the given model
        if model not in MODELS_COST:
            self.logger.error(f"Model '{model}' not recognized for cost calculation.")
            return 0.0

        input_cost_per_token = MODELS_COST[model]["input"]
        output_cost_per_token = MODELS_COST[model]["output"]

        # Calculate total cost
        input_cost = input_tokens * input_cost_per_token
        output_cost = output_tokens * output_cost_per_token
        total_cost = input_cost + output_cost

        self.logger.info(
            f"Model: {model}, Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, "
            f"Cost: ${total_cost:.6f}"
        )
        return total_cost
