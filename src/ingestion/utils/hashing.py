import hashlib
from typing import BinaryIO

def compute_file_checksum(file_path: str, algorithm: str = 'sha256', chunk_size: int = 8192) -> str:
    hasher = hashlib.new(algorithm)
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            hasher.update(chunk)
    return hasher.hexdigest()

def compute_stream_checksum(stream: BinaryIO, algorithm: str = 'sha256', chunk_size: int = 8192) -> str:
    hasher = hashlib.new(algorithm)
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        hasher.update(chunk)
    return hasher.hexdigest()

def compute_bytes_checksum(data: bytes, algorithm: str = 'sha256') -> str:
    hasher = hashlib.new(algorithm)
    hasher.update(data)
    return hasher.hexdigest()
