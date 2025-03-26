import hashlib

def compute_hash(data: bytes, hash_algo: str = 'md5') -> str:
    hasher = hashlib.new(hash_algo)
    hasher.update(data)
    return hasher.hexdigest()
