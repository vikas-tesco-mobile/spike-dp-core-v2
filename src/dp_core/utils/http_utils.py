from urllib.parse import urlparse

from dp_core.utils.logger import get_logger

logger = get_logger(__name__)


def validate_url(url: str) -> None:
    """Validate a URL for scheme, hostname, and block known metadata/localhost hosts.

    Raises ValueError on any validation failure.
    """
    if not url:
        raise ValueError("URL is empty")

    try:
        parsed = urlparse(url)
    except Exception as e:  # pragma: no cover
        raise ValueError(f"Invalid URL format: {e}")

    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Invalid URL scheme '{parsed.scheme}'")

    hostname = (parsed.hostname or "").lower()
    if not hostname:
        raise ValueError("URL has no valid hostname")

    blocked_hosts = {
        "localhost",
        "127.0.0.1",
        "::1",
        "[::1]",
        "169.254.169.254",  # AWS metadata
        "metadata.google.internal",  # GCP metadata
    }

    if hostname in blocked_hosts:
        raise ValueError(f"URL targets blocked host '{hostname}' (SSRF protection)")

    logger.debug(f"✓ URL validation passed: {hostname}")
