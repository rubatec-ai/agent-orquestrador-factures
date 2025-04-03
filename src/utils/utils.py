import hashlib
import re

def compute_hash(data: bytes, hash_algo: str = 'md5') -> str:
    hasher = hashlib.new(hash_algo)
    hasher.update(data)
    return hasher.hexdigest()


def parse_currency(value_str: str):
    """
    Extracts a monetary value from a string (e.g., "123.45" or "123,45") and returns it as a float.
    If no valid value is found, returns None.
    """
    if not value_str:
        return None

    # Reemplazar comas por puntos para manejar formatos europeos
    value_str = value_str.replace(',', '.')

    # Este regex busca uno o más dígitos, un punto, y hasta dos dígitos decimales
    pattern = r"(\d+(?:\.\d{1,2})?)"
    match = re.search(pattern, value_str)
    if match:
        return float(match.group(1))
    return None