import logging
import json
import openai
from typing import Dict, Any, Optional
from src.utils.constants import SYSTEM_PROMPT_ANALYSIS, MODELS_COST, PARAMETERS_TO_SEARCH, PROMPT_TASK

class AIExtractor:
    """
    Responsable de interactuar con la API de OpenAI.
    """
    def __init__(self, model: str, api_key: str, logger: logging.Logger, temperature: float = 0.2, max_tokens: int = 100):
        self.model = model
        self.api_key = api_key
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.total_cost = 0.0
        self.logger = logger
        self.client = openai.OpenAI(api_key=self.api_key)

    def extract_edge_case(self, pdf_text: str, ocr_extracted_data: Dict[str, Any]):
        """
        Extrae los campos definidos a partir del texto del PDF y retorna un diccionario.
        """
        dynamic_prompt = (
            "#### Task ####\n"
            f"{PROMPT_TASK}\n"
        )
        for parameter, description in PARAMETERS_TO_SEARCH.items():
            dynamic_prompt += (
                "----------------------------------------------\n"
                f"Parameter to find: {parameter}\n"
                "----------------------------------------------\n"
                f"Parameter instructions:\n{description}\n"
                "----------------------------------------------\n"
            )

        dynamic_prompt += (
            "----------------------------------------------\n"
            f"Preliminary results from the OCR: {str(ocr_extracted_data)}\n"
            "----------------------------------------------\n"
            f"Pdf content:\n{pdf_text}\n"
            "----------------------------------------------\n"
        )

        response = self.call_openai_api(dynamic_prompt)
        output_text = response.get("output", "")[0].get("content", "")[0].get("text", "")

        try:
            result = json.loads(output_text)
            return result
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decodificando JSON de la respuesta de OpenAI: {e}")
            return {"edge_case_raw": output_text}

    def call_openai_api(self, prompt: str) -> Dict[str, Any]:
        """
        Envía un prompt a la API de OpenAI y retorna la respuesta utilizando Structured Outputs.
        """
        try:
            self.logger.info("Enviando petición a la API de OpenAI.")

            # Generamos un schema dinámico a partir de los parámetros a buscar.
            schema = {
                "type": "object",
                "properties": {key: {"type": "string"} for key in PARAMETERS_TO_SEARCH.keys()},
                "required": list(PARAMETERS_TO_SEARCH.keys()),
                "additionalProperties": False
            }

            response = self.client.responses.create(
                model=self.model,
                input=[
                    {"role": "system", "content": SYSTEM_PROMPT_ANALYSIS},
                    {"role": "user", "content": prompt},
                ],
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "edge_case",
                        "schema": schema,
                        "strict": True
                    }
                }
            )

            response_dict: Dict[str, Any] = response.to_dict()

            # Extraer detalles de uso para calcular el coste
            usage: Dict[str, int] = response_dict.get("usage", {})
            input_tokens = usage.get("input_tokens", 0)
            cached_tokens = usage.get("input_tokens_details", {}).get("cached_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)
            self.total_cost += self.calculate_cost(
                model=self.model,
                input_tokens=input_tokens,
                cached_input_tokens=cached_tokens,
                output_tokens=output_tokens
            )

            return response_dict

        except openai.APIConnectionError as e:
            self.logger.error(f"Error de conexión: {e}")
        except openai.AuthenticationError as e:
            self.logger.error(f"Error de autenticación: {e}")
        except openai.RateLimitError as e:
            self.logger.error(f"Se ha excedido el límite de peticiones: {e}")
        except Exception as e:
            self.logger.error(f"Error inesperado: {e}")
        return {}

    def calculate_cost(self, model: str, input_tokens: int, cached_input_tokens: int, output_tokens: int) -> float:
        """
        Calcula el coste de una llamada a la API basado en el número de tokens,
        incluyendo tokens de entrada, entrada cacheada y tokens de salida.
        """
        if model not in MODELS_COST:
            self.logger.error(f"Modelo '{model}' no reconocido para el cálculo de coste.")
            return 0.0

        cost_input = MODELS_COST[model]["input"]
        cost_output = MODELS_COST[model]["output"]
        # Se asume 0.0 si no se ha definido "cached_input" en el diccionario
        cost_cached_input = MODELS_COST[model].get("cached_input") or 0.0

        total_cost = (input_tokens * cost_input) + (cached_input_tokens * cost_cached_input) + (
                    output_tokens * cost_output)

        self.logger.info(
            f"Modelo: {model}, Tokens de entrada: {input_tokens}, Tokens de entrada cacheada: {cached_input_tokens}, "
            f"Tokens de salida: {output_tokens}, Coste: ${total_cost:.6f}"
        )
        return total_cost

