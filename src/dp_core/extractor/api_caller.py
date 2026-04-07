import time
import json
from typing import Any, Dict, Generator, Optional
from urllib.parse import urlparse, parse_qsl

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from dp_core.utils.http_utils import validate_url
from dp_core.utils.logger import get_logger

logger = get_logger(__name__)


class APIError(Exception):
    pass


class RateLimitError(Exception):
    """Raised when HTTP 429 is encountered."""

    pass


class TransientAPIError(Exception):
    """Retryable API error for transient HTTP failures (5xx, 408, etc.)."""

    pass


class APICaller:
    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        session: Optional[requests.Session] = None,
        timeout_secs: int = 10,
        min_request_interval_secs: float = 0.0,
    ) -> None:
        if not url:
            raise ValueError("API URL must be provided")
        validate_url(url)
        self.url = url
        self.session = session or requests.Session()
        self.timeout_secs = timeout_secs
        self.min_request_interval_secs = min_request_interval_secs
        self._last_request_monotonic: float = 0.0
        if headers:
            self.session.headers.update(headers)

    @staticmethod
    def _sanitize_params(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not params:
            return params
        redacted_keys = {
            "token",
            "key",
            "api_key",
            "apikey",
            "authorization",
            "auth",
            "access_token",
            "secret",
        }
        return {
            k: ("***" if k.lower() in redacted_keys else v) for k, v in params.items()
        }

    @staticmethod
    def _extract_query_params(url_or_query: str) -> Dict[str, Any]:
        try:
            parsed = urlparse(url_or_query)
            query = parsed.query if parsed.scheme else (url_or_query or "")
            return dict(parse_qsl(query, keep_blank_values=True))
        except Exception as e:
            logger.warning(f"Failed to parse nextPage query params: {e}")
            return {}

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=3),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(
            (
                RateLimitError,
                TransientAPIError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            )
        ),
        reraise=True,
    )
    def get(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if self.min_request_interval_secs > 0:
            now = time.monotonic()
            remaining = self.min_request_interval_secs - (
                now - self._last_request_monotonic
            )
            if remaining > 0:
                time.sleep(min(remaining, 2.0))

        safe_params = self._sanitize_params(params)
        logger.info(f"GET {self.url} params={safe_params}")
        resp = self.session.get(self.url, params=params, timeout=self.timeout_secs)
        self._last_request_monotonic = time.monotonic()

        if resp.status_code == 429:
            retry_after_raw = resp.headers.get("Retry-After", "0")
            try:
                retry_after = max(0, int(float(retry_after_raw)))
            except ValueError:
                retry_after = 0
            if retry_after > 0:
                logger.warning(f"HTTP 429. Sleeping for {retry_after}s")
                time.sleep(min(retry_after, 120))
            raise RateLimitError()

        status_code = resp.status_code
        if status_code >= 400:
            try:
                payload = resp.json()
                desc = json.dumps(payload)[:500]
            except ValueError:
                desc = (resp.text or "")[:500]
            logger.error(
                f"HTTP {status_code} for {self.url}: {desc} | params={safe_params}"
            )

            if status_code == 408 or 500 <= status_code < 600:
                raise TransientAPIError(f"Transient HTTP {status_code}: {desc}")

            resp.raise_for_status()

        try:
            return resp.json()
        except ValueError as e:
            raise APIError(f"Invalid JSON response: {e}")

    def paginate_by_next_page(
        self, initial_params: Dict[str, Any], next_page_key: str = "nextPage"
    ) -> Generator[Dict[str, Any], None, None]:
        params = dict(initial_params)
        page_index = 0
        while True:
            page_index += 1
            data = self.get(params=params)
            yield data
            raw_next = data.get(next_page_key)
            if not raw_next:
                break
            next_params = self._extract_query_params(str(raw_next))
            params = {**initial_params, **next_params}
