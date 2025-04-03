
def get_field(ocr_data, primary_key, fallback_key):
    return ocr_data.get(primary_key) if ocr_data.get(fallback_key) == "desconocido" else ocr_data.get(fallback_key, "")