import pytest
from unittest.mock import patch, MagicMock

import requests

from dp_core.utils.api_auth import BearerAuth, AuthError


@pytest.fixture
def config():
    return {
        "auth_url": "https://auth.example.com/token",
        "secret_scope": "my-scope",
        "client_id_key": "client-id",
        "client_secret_key": "client-secret",
        "grant_type": "password",
        "timeout": 5,
    }


@patch("dp_core.utils.api_auth.get_dbutils_credentials")
@patch("dp_core.utils.api_auth.requests.post")
def test_request_token_success(mock_post, mock_get_creds, config):
    mock_get_creds.side_effect = ["cid-value", "secret-value"]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"access_token": "abc123", "expires_in": 3600}
    mock_post.return_value = mock_resp

    auth = BearerAuth(config)
    token_json = auth._request_token()

    assert auth._access_token == "abc123"
    assert token_json["access_token"] == "abc123"

    mock_post.assert_called_once_with(
        config["auth_url"],
        data={
            "client_id": "cid-value",
            "client_secret": "secret-value",
            "grant_type": "password",
        },
        timeout=5,
    )


@patch("dp_core.utils.api_auth.get_dbutils_credentials")
@patch("dp_core.utils.api_auth.requests.post")
def test_get_auth_header_lazy(mock_post, mock_get_creds, config):
    mock_get_creds.side_effect = ["id", "secret"]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"access_token": "xyz"}
    mock_post.return_value = mock_resp

    auth = BearerAuth(config)
    header = auth.get_auth_header()

    assert header == {"Authorization": "Bearer xyz"}
    assert auth._access_token == "xyz"


@patch("dp_core.utils.api_auth.get_dbutils_credentials")
def test_request_missing_auth_url(mock_get_creds):
    mock_get_creds.return_value = "dummy"

    config = {
        "auth_url": None,  # invalid → should raise
        "secret_scope": "scope",
        "client_id_key": "id",
        "client_secret_key": "secret",
    }

    auth = BearerAuth(config)

    with pytest.raises(AuthError) as exc:
        auth._request_token.__wrapped__(auth)

    assert "Missing or invalid 'auth_url'" in str(exc.value)


@patch("dp_core.utils.api_auth.get_dbutils_credentials")
def test_request_missing_secrets(mock_get_creds, config):
    mock_get_creds.side_effect = [None, "secret"]

    auth = BearerAuth(config)

    with pytest.raises(AuthError) as exc:
        auth._request_token.__wrapped__(auth)

    assert "Missing client id/secret" in str(exc.value)


@patch("dp_core.utils.api_auth.get_dbutils_credentials")
@patch("dp_core.utils.api_auth.requests.post")
def test_request_token_http_error(mock_post, mock_get_creds, config):
    mock_get_creds.side_effect = ["id", "secret"]

    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_resp.text = "unauthorized"
    mock_resp.json.return_value = {}
    mock_post.return_value = mock_resp

    auth = BearerAuth(config)

    with pytest.raises(AuthError) as exc:
        auth._request_token.__wrapped__(auth)

    assert "Token request failed (401)" in str(exc.value)


@patch("dp_core.utils.api_auth.get_dbutils_credentials")
@patch("dp_core.utils.api_auth.requests.post")
def test_request_missing_access_token(mock_post, mock_get_creds, config):
    mock_get_creds.side_effect = ["id", "secret"]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {}
    mock_post.return_value = mock_resp

    auth = BearerAuth(config)

    with pytest.raises(AuthError) as exc:
        auth._request_token.__wrapped__(auth)

    assert "Missing 'access_token'" in str(exc.value)


@patch("dp_core.utils.api_auth.get_dbutils_credentials")
@patch("dp_core.utils.api_auth.requests.post")
def test_request_retry_on_autherror(mock_post, mock_get_creds, config):
    mock_get_creds.side_effect = ["id", "secret", "id", "secret"]

    resp_500 = MagicMock()
    resp_500.status_code = 500
    resp_500.text = "error"
    resp_500.json.return_value = {}
    resp_500.raise_for_status.side_effect = requests.exceptions.HTTPError("500")

    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.text = "ok"
    resp_200.json.return_value = {"access_token": "retry_token"}
    resp_200.raise_for_status.return_value = None

    mock_post.side_effect = [resp_500, resp_200]

    auth = BearerAuth(config)

    header = auth.get_auth_header()

    assert header == {"Authorization": "Bearer retry_token"}
    assert mock_post.call_count == 2
    assert mock_get_creds.call_count == 4
