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


def compute_directory_checksum(dir_path: str, pattern: str = "*", algorithm: str = 'sha256') -> str:
    """Compute a deterministic SHA-256 checksum of all matching files in a directory."""
    import glob
    import os

    hasher = hashlib.new(algorithm)
    files = sorted(glob.glob(os.path.join(dir_path, pattern)))
    for f in files:
        if os.path.isfile(f):
            rel = os.path.relpath(f, dir_path)
            hasher.update(rel.encode("utf-8"))
            with open(f, "rb") as fh:
                for chunk in iter(lambda: fh.read(65536), b""):
                    hasher.update(chunk)
    return hasher.hexdigest()

