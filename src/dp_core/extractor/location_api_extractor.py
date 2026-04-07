import argparse
import json
from urllib.parse import urlparse

import requests

from typing import Dict, Any, cast
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

from dp_core.utils.api_auth import BearerAuth, AuthBase
from dp_core.utils.databricks_utils import write_json
from dp_core.utils.logger import get_logger
from dp_core.utils.config_loader import load_configs
from dp_core.utils.time_utils import build_dates_for_path

logger = get_logger(__name__)


def validate_url(url: str) -> None:
    if not url:
        raise ValueError("URL is empty")

    try:
        parsed = urlparse(url)
    except Exception as e:
        raise ValueError(f"Invalid URL format: {e}")

    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            f"Invalid URL scheme '{parsed.scheme}'; only http and https are allowed"
        )

    blocked_hosts = {
        "localhost",
        "127.0.0.1",
        "::1",
        "[::1]",
        "169.254.169.254",  # AWS metadata
        "metadata.google.internal",  # GCP metadata
    }

    hostname = (parsed.hostname or "").lower()
    if hostname in blocked_hosts:
        raise ValueError(f"URL targets blocked host '{hostname}'; SSRF protection")

    if not hostname:
        raise ValueError("URL has no valid hostname")

    logger.debug(f"✓ URL validation passed: {hostname}")


class LocationAPIExtractor:
    def __init__(self, config: Dict[str, Any], bearer_auth: AuthBase) -> None:
        self.config: Dict[str, Any] = config

        self.auth: AuthBase = bearer_auth

        self.url: str = cast(str, self.config.get("location_url", ""))

        self.request_params: Dict[str, Any] = cast(
            Dict[str, Any], self.config.get("request_params", {})
        )

        self.limit: int = int(self.config.get("limit", 100))
        self.offset: int = 0

        self.target_path: str = cast(str, self.config.get("target_path", ""))

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def _fetch(self, offset: int) -> dict:
        params: Dict[str, Any] = {
            **(self.request_params or {}),
            "limit": self.limit,
            "offset": offset,
        }
        headers = self.auth.get_auth_header()
        logger.info(f"🔹 Calling API offset={offset}, limit={self.limit}")

        if not isinstance(self.url, str) or not self.url:
            raise RuntimeError("Missing or invalid 'location_url' in configuration")

        validate_url(self.url)
        resp = requests.get(self.url, headers=headers, params=params, timeout=30)

        resp.raise_for_status()
        return resp.json()

    def _extract_locations(self, resp_json: dict):
        return resp_json.get("locations", [])

    def _save_response_json(self, resp_json: dict, offset: int):
        ingestion_date, ingestion_hour, time_str = build_dates_for_path()

        file_path = f"{self.target_path}/{ingestion_date}/{ingestion_hour}/locations_{time_str}_{offset}.json"

        json_text = json.dumps(resp_json)

        write_json(file_path, json_text)

        logger.info(f"✔ Saved raw JSON response offset={offset} → {file_path}")

    def run(self):
        first_resp = self._fetch(offset=0)

        total = first_resp.get("totalCount")
        logger.info(f"Total locations reported: {total}")

        self._save_response_json(first_resp, offset=0)
        offset = self.limit
        while offset < total:
            resp = self._fetch(offset)
            items = self._extract_locations(resp)

            if not items:
                logger.info("No more locations returned. Stopping.")
                break

            self._save_response_json(resp, offset)

            offset += self.limit

        logger.info("Location extraction completed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--config_path", required=True)
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--task_name", required=True)
    args = parser.parse_args()

    configs = load_configs(
        config_file_name="config",
        config_path_str=args.config_path,
        profile=args.profile,
    )
    job_configs = configs[args.job_name][args.task_name]
    logger.info(
        f"Running {args.job_name}-{args.task_name} with configurations: {job_configs}"
    )

    bearer_auth = BearerAuth(job_configs.get("authentication", {}))
    job = LocationAPIExtractor(job_configs, bearer_auth)
    job.run()
