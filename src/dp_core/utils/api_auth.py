from abc import ABC, abstractmethod

import requests
from typing import Dict, Optional, Any
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

from dp_core.utils.databricks_utils import get_dbutils_credentials
from dp_core.utils.logger import get_logger

logger = get_logger(__name__)


class AuthError(Exception):
    pass


class AuthBase(ABC):
    @abstractmethod
    def get_auth_header(self) -> Dict[str, str]: ...


class BearerAuth(AuthBase):
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config: Dict[str, Any] = config
        self.token_url: Optional[str] = self.config.get("auth_url")
        self.secret_scope: Optional[str] = self.config.get("secret_scope")
        self.client_id_key: Optional[str] = self.config.get("client_id_key")
        self.client_secret_key: Optional[str] = self.config.get("client_secret_key")
        self.grant_type: str = self.config.get("grant_type", "password")
        self.timeout: int = int(self.config.get("timeout", 30))

        self._access_token: Optional[str] = None
        self._token_expires_in: Optional[int] = None

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(AuthError),
        reraise=True,
    )
    def _request_token(self) -> Dict[str, Any]:
        client_id_value = get_dbutils_credentials(self.secret_scope, self.client_id_key)
        client_secret_value = get_dbutils_credentials(
            self.secret_scope, self.client_secret_key
        )

        if not isinstance(self.token_url, str) or not self.token_url:
            raise AuthError(
                "Missing or invalid 'auth_url' in authentication configuration"
            )

        if not isinstance(client_id_value, str) or not isinstance(
            client_secret_value, str
        ):
            raise AuthError("Missing client id/secret in Databricks secrets")

        payload = {
            "client_id": client_id_value,
            "client_secret": client_secret_value,
            "grant_type": self.grant_type,
        }

        logger.info(f"Requesting Bearer token from {self.token_url}")
        try:
            response = requests.post(self.token_url, data=payload, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            raise AuthError(f"Error connecting to token endpoint: {e}")

        if response.status_code != 200:
            logger.error(
                f"Token request failed ({response.status_code}): {response.text}"
            )
            raise AuthError(f"Token request failed ({response.status_code})")

        json_data = response.json()
        if "access_token" not in json_data:
            raise AuthError(f"Missing 'access_token' in token response: {json_data}")

        self._access_token = json_data["access_token"]
        self._token_expires_in = json_data.get("expires_in")

        logger.info("✔ Retrieved Bearer token successfully.")
        return json_data

    def get_auth_header(self) -> Dict[str, str]:
        if not self._access_token:
            self._request_token()
        return {"Authorization": f"Bearer {self._access_token}"}

    def __repr__(self):
        return f"<BearerAuth token_url={self.token_url} expires_in={self._token_expires_in}>"
