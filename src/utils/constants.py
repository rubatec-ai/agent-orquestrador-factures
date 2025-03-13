SYSTEM_PROMPT_ANALYSIS = (
    "You are an assistant specializing in searching parameters within distinct paragraphs. "
    "All paragraphs may be written in Spanish or Catalan. Provide responses that respect this language context."
)

PROMPT_TASK = """
You are an assistant specialized in extracting specific tender or procurement data from text. 
Your goal is to find information about one or more requested parameters from structured data sources (text paragraphs or JSON tables). 
The tables will be provided in JSON format, with rows containing column names and values.

1. Read each paragraph/table carefully.
2. Identify all content relevant to the requested parameter(s).
3. If the parameter typically has a numeric code, date, or numeric duration, return **only** that numeric or date value.
4. If the parameter is mentioned but no numeric/date value is given:
   - **If a specific section or paragraph discusses the parameter**, copy and paste the entire section or paragraph.
   - **If no explicit section is found but the text contains relevant information**, generate a summary of the information based on the context.
5. When multiple parameters are provided, process them together and return results for each parameter **separately**.
"""

COMBINED_PARAMETER = "Combined Parameters"

# Model pricing constants (as of 2025-01-13)

# Define model costs per token
MODELS_COST = {
    "gpt-4o": {
        "input": 0.00000250,
        "output": 0.00001000
    },
    "gpt-4o-mini": {
        "input": 0.00000015,
        "output": 0.00000060
    },
    "o1": {
        "input": 0.00001500,
        "output": 0.00006000
    },
    "o1-mini": {
        "input": 0.00000300,
        "output": 0.00001200
    },
}

