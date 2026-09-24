"""Streaming HTTP client with retry, checksum, and resume support.

Handles downloading large files (e.g., FAOSTAT Trade Matrix ~600 MB ZIP)
with streaming writes to disk and incremental SHA-256 computation.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from typing import Optional

import requests

from ingestion.core.config import RetryConfig
from ingestion.core.enums import ErrorType
from ingestion.utils.error_classifier import (
    TransientError,
    PermanentError,
    classify_http_status,
)
from ingestion.utils.retry import retry_with_backoff


import socket
import urllib3.util.connection as urllib3_cn

# Force IPv4 resolution to prevent Windows IPv6 connection hangs on CDNs
try:
    urllib3_cn.allowed_gai_family = lambda: socket.AF_INET
except Exception:
    pass

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 (RiceLakehouse-Ingestion/1.0)"


@dataclass
class DownloadResult:
    """Metadata returned after a successful download."""

    path: str
    size_bytes: int
    content_type: str = ""
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    checksum_sha256: str = ""
    supports_range: bool = False


class HttpClient:
    """HTTP client with streaming download and integrated retry."""

    def __init__(
        self,
        retry_config: Optional[RetryConfig] = None,
        timeout: int = 60,
    ) -> None:
        self.retry_config = retry_config or RetryConfig(max_attempts=3)
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})

    # ------------------------------------------------------------------
    # Response error handling
    # ------------------------------------------------------------------
    @staticmethod
    def _raise_for_status(response: requests.Response) -> None:
        """Raise TransientError or PermanentError based on status code."""
        if response.ok:
            return
        error_type = classify_http_status(response.status_code)
        msg = f"HTTP {response.status_code}: {response.reason}"
        if error_type == ErrorType.TRANSIENT:
            exc = TransientError(msg)
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                exc.retry_after = retry_after  # type: ignore[attr-defined]
            raise exc
        raise PermanentError(msg)

    # ------------------------------------------------------------------
    # HEAD request (for readiness checks)
    # ------------------------------------------------------------------
    def head(self, url: str, **kwargs) -> requests.Response:
        """Send a HEAD request with retry."""

        def _do_head() -> requests.Response:
            resp = self._session.head(url, timeout=self.timeout, allow_redirects=True, **kwargs)
            self._raise_for_status(resp)
            return resp

        wrapped = retry_with_backoff(_do_head, self.retry_config)
        return wrapped()

    # ------------------------------------------------------------------
    # Range support check
    # ------------------------------------------------------------------
    def check_range_support(self, url: str) -> bool:
        """Return True if the server advertises ``Accept-Ranges: bytes``."""
        try:
            resp = self.head(url)
            return resp.headers.get("Accept-Ranges", "").lower() == "bytes"
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Streaming download
    # ------------------------------------------------------------------
    def download_streaming(
        self,
        url: str,
        dest_path: str,
        chunk_size_bytes: int = 8192,
    ) -> DownloadResult:
        """Stream-download *url* to *dest_path*, computing SHA-256 on the fly."""

        def _do_download() -> DownloadResult:
            resp = self._session.get(url, timeout=self.timeout, stream=True)
            self._raise_for_status(resp)

            sha256 = hashlib.sha256()
            size = 0

            with open(dest_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=chunk_size_bytes):
                    if chunk:
                        fh.write(chunk)
                        sha256.update(chunk)
                        size += len(chunk)

            return DownloadResult(
                path=dest_path,
                size_bytes=size,
                content_type=resp.headers.get("Content-Type", ""),
                etag=resp.headers.get("ETag"),
                last_modified=resp.headers.get("Last-Modified"),
                checksum_sha256=sha256.hexdigest(),
                supports_range=resp.headers.get("Accept-Ranges", "").lower() == "bytes",
            )

        wrapped = retry_with_backoff(_do_download, self.retry_config)
        return wrapped()

    # ------------------------------------------------------------------
    # Resumable download (best-effort)
    # ------------------------------------------------------------------
    def download_with_resume(
        self,
        url: str,
        dest_path: str,
        chunk_size_bytes: int = 8192,
    ) -> DownloadResult:
        """Download with resume if the server supports Range requests.

        Falls back to a full streaming download otherwise.
        """
        if not self.check_range_support(url):
            return self.download_streaming(url, dest_path, chunk_size_bytes)

        existing_size = 0
        if os.path.exists(dest_path):
            existing_size = os.path.getsize(dest_path)

        if existing_size == 0:
            return self.download_streaming(url, dest_path, chunk_size_bytes)

        # Attempt resume from existing_size
        def _do_resume() -> DownloadResult:
            headers = {"Range": f"bytes={existing_size}-"}
            resp = self._session.get(url, headers=headers, timeout=self.timeout, stream=True)
            if resp.status_code not in (200, 206):
                self._raise_for_status(resp)

            sha256 = hashlib.sha256()
            # Re-hash existing portion
            with open(dest_path, "rb") as fh:
                while True:
                    data = fh.read(chunk_size_bytes)
                    if not data:
                        break
                    sha256.update(data)

            size = existing_size
            mode = "ab" if resp.status_code == 206 else "wb"
            if resp.status_code == 200:
                size = 0
                sha256 = hashlib.sha256()

            with open(dest_path, mode) as fh:
                for chunk in resp.iter_content(chunk_size=chunk_size_bytes):
                    if chunk:
                        fh.write(chunk)
                        sha256.update(chunk)
                        size += len(chunk)

            return DownloadResult(
                path=dest_path,
                size_bytes=size,
                content_type=resp.headers.get("Content-Type", ""),
                etag=resp.headers.get("ETag"),
                last_modified=resp.headers.get("Last-Modified"),
                checksum_sha256=sha256.hexdigest(),
                supports_range=True,
            )

        wrapped = retry_with_backoff(_do_resume, self.retry_config)
        return wrapped()
