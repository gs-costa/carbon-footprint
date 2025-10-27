from typing import Any

import requests

from src.common.logger import Logger


class BaseHTTPClient:
    """Base HTTP client class."""

    logger = Logger(__name__)

    def __init__(self, base_url: str, default_headers: dict[str, str] | None = None):
        self.base_url = base_url.rstrip("/")
        self.default_headers = default_headers or {}
        self.session = requests.Session()

        if self.default_headers:
            self.session.headers.update(self.default_headers)

    def _build_url(self, endpoint: str) -> str:
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    def get(
        self, endpoint: str, auth: tuple[str, str] | None = None, params: dict[str, Any] | None = None
    ) -> list[dict]:
        """Get data from the API."""
        url = self._build_url(endpoint)
        try:
            response = self.session.get(url, auth=auth, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Error getting resource from {url}: {e}")
            raise e
