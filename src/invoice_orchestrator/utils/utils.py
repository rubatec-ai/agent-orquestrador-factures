def get_field(data, ocr_key, openai_key):
    # Si el valor de OpenAI es "desconocido", usamos el valor del OCR
    # En caso contrario, usamos el valor de OpenAI
    return data.get(ocr_key) if data.get(openai_key) == "desconocido" else data.get(openai_key, "")