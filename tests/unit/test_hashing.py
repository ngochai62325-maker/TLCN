import pytest
import os
from ingestion.utils.hashing import compute_file_checksum, compute_bytes_checksum

def test_compute_file_checksum_sha256(tmp_path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("hello world")
    checksum = compute_file_checksum(str(file_path), algorithm="sha256")
    # echo -n "hello world" | sha256sum
    assert checksum == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

def test_compute_file_checksum_md5(tmp_path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("hello world")
    checksum = compute_file_checksum(str(file_path), algorithm="md5")
    # echo -n "hello world" | md5sum
    assert checksum == "5eb63bbbe01eeed093cb22bb8f5acdc3"

def test_compute_bytes_checksum():
    data = b"hello world"
    checksum = compute_bytes_checksum(data, algorithm="sha256")
    assert checksum == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

def test_large_file_streaming(tmp_path):
    file_path = tmp_path / "large.bin"
    # Write 2MB file
    with open(file_path, "wb") as f:
        f.write(os.urandom(2 * 1024 * 1024))
    
    # Just verify it doesn't crash and returns a string
    checksum = compute_file_checksum(str(file_path))
    assert isinstance(checksum, str)
    assert len(checksum) == 64  # sha256 length
