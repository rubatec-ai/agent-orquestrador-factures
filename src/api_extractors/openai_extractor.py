import logging
import json
import openai
from typing import Dict, Any, Optional
from src.utils.constants import SYSTEM_PROMPT_ANALYSIS, MODELS_COST, PARAMETERS_TO_SEARCH, PROMPT_TASK, VALID_CANAL_SIE, \
    PROMPT_SIE


class AIExtractor:
    """
    Responsable de interactuar con la API de OpenAI.
    """

    def __init__(self, model: str, api_key: str, logger: logging.Logger, temperature: float = 0.2,
                 max_tokens: int = 100):
        self.model = model
        self.api_key = api_key
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.total_cost = 0.0
        self.logger = logger
        self.client = openai.OpenAI(api_key=self.api_key)

    def extract_edge_case(self, image_data_url: str, ocr_extracted_data: Dict[str, Any]):
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
        )
        # First call: extract all parameters
        response = self.call_openai_api(prompt=dynamic_prompt, image_data_url=image_data_url, flag_fallback=False)
        output_text = response.choices[0].message.content
        try:
            result = json.loads(output_text)
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decodificando JSON de la respuesta de OpenAI: {e}")
            return {"edge_case_raw": output_text}
        # Check and refine canal_sie if necessary
        sie_value = result.get("canal_sie", "").strip()
        if sie_value not in VALID_CANAL_SIE:
            self.logger.info(f"Refining canal_sie: '{sie_value}' ...")

            # Provide mapping as additional context in the prompt
            prompt_sie = (
                "#### Task ####\n"
                f"{PROMPT_SIE}\n"
            )

            try:
                response_fall_back = self.call_openai_api(prompt=prompt_sie, image_data_url=image_data_url,
                                                          flag_fallback=True)
                output_fall_back = response_fall_back.choices[0].message.content
                sie_json = json.loads(output_fall_back)
                refined = sie_json.get("canal_sie", sie_value)

                self.logger.info(f"The refined canal_sie is '{refined}' ...")
                if refined in VALID_CANAL_SIE:
                    result["canal_sie"] = refined
                else:
                    self.logger.warning(f"Refined canal_sie '{refined}' not in valid list")
                    result["canal_sie"] = "desconocido"
            except Exception as e:
                self.logger.warning(f"Failed to refine canal_sie: {e}")
        return result

    def call_openai_api(self, prompt: str, image_data_url: str, flag_fallback: bool) -> Any:
        """
        Envía un prompt a la API de OpenAI y retorna la respuesta utilizando Structured Outputs.
        """
        try:
            self.logger.info("Enviando petición a la API de OpenAI.")

            # Generamos un schema dinámico a partir de los parámetros a buscar.

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_ANALYSIS},
                    {"role": "user", "content": [{"type": "text", "text": prompt},
                                                 {"type": "image_url", "image_url": {"url": image_data_url}}]}
                ],
                response_format={"type": "json_object"}
            )

            # Extraer detalles de uso para calcular el coste
            usage = response.usage
            input_tokens = usage.prompt_tokens
            cached_tokens = usage.prompt_tokens_details.cached_tokens
            output_tokens = usage.completion_tokens
            self.total_cost += self.calculate_cost(
                model=self.model,
                input_tokens=input_tokens,
                cached_input_tokens=cached_tokens,
                output_tokens=output_tokens
            )

            return response

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
