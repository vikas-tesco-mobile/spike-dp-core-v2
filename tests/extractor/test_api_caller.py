import json
from unittest.mock import Mock, patch

import pytest

from dp_core.extractor.api_caller import APICaller


class DummyResponse:
    def __init__(self, status_code=200, json_data=None, headers=None):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.headers = headers or {}
        self.text = json.dumps(self._json_data)

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


@pytest.fixture
def mock_session():
    return Mock()


def test_get_return_result_with_success(monkeypatch, mock_session):
    expected_data = {"result": "success", "data": [1, 2, 3]}
    mock_session.get.return_value = DummyResponse(
        status_code=200, json_data=expected_data
    )

    api_caller = APICaller(
        url="https://api.example.com/data", session=mock_session, timeout_secs=5
    )

    result = api_caller.get(params={"q": "test"})

    assert result == expected_data
    mock_session.get.assert_called_once_with(
        "https://api.example.com/data", params={"q": "test"}, timeout=5
    )


@patch("dp_core.extractor.api_caller.time.sleep", lambda *_: None)
def test_get_retries_on_429_and_succeeds(mock_session):
    responses = [
        DummyResponse(
            status_code=429,
            json_data={"error": "rate limit"},
            headers={"Retry-After": "0"},
        ),
        DummyResponse(status_code=200, json_data={"ok": True}),
    ]
    mock_session.get.side_effect = responses

    api_call = APICaller(
        url="https://api.example.com/data", session=mock_session, timeout_secs=5
    )

    result = api_call.get(params={"q": "value"})

    assert result == {"ok": True}
    assert mock_session.get.call_count == 2
    mock_session.get.assert_any_call(
        "https://api.example.com/data", params={"q": "value"}, timeout=5
    )


@patch("dp_core.extractor.api_caller.time.sleep", lambda *_: None)
def test_get_retries_on_transient_error_and_succeeds(mock_session):
    responses = [
        DummyResponse(
            status_code=500,
            json_data={"error": "rate limit"},
            headers={"Retry-After": "0"},
        ),
        DummyResponse(status_code=200, json_data={"ok": True}),
    ]
    mock_session.get.side_effect = responses

    api_call = APICaller(
        url="https://api.example.com/data", session=mock_session, timeout_secs=5
    )

    result = api_call.get(params={"q": "value"})

    assert result == {"ok": True}
    assert mock_session.get.call_count == 2
    mock_session.get.assert_any_call(
        "https://api.example.com/data", params={"q": "value"}, timeout=5
    )


@patch("dp_core.extractor.api_caller.time.sleep", lambda *_: None)
def test_get_non_retryable_http_error_does_not_retry(mock_session):
    response = DummyResponse(status_code=403, json_data={"error": "forbidden"})
    mock_session.get.return_value = response

    api_call = APICaller(
        url="https://api.example.com/data", session=mock_session, timeout_secs=5
    )

    with pytest.raises(Exception):
        api_call.get(params={"q": "value"})
    assert mock_session.get.call_count == 1


@patch("dp_core.extractor.api_caller.APICaller.get")
def test_paginate_by_next_page_multiple_pages(mock_get) -> None:
    api_caller = APICaller(url="https://api.example.com/data")

    page1 = {"results": [1, 2], "nextPage": "cursor=abc"}
    page2 = {"results": [3, 4], "nextPage": None}
    mock_get.side_effect = [page1, page2]

    initial_params = {"limit": 1}
    pages = list(api_caller.paginate_by_next_page(initial_params))

    assert pages == [page1, page2]
    assert mock_get.call_count == 2
    called_params_first = mock_get.call_args_list[0].kwargs["params"]
    called_params_second = mock_get.call_args_list[1].kwargs["params"]
    assert called_params_first == {"limit": 1}
    assert called_params_second == {"limit": 1, "cursor": "abc"}


@patch("dp_core.extractor.api_caller.APICaller.get")
def test_paginate_by_next_page_stops_after_first_page(mock_get) -> None:
    api_caller = APICaller(url="https://api.example.com/data")

    page1 = {"results": [1, 2]}
    mock_get.side_effect = [page1]

    initial_params = {"limit": 1}
    pages = list(api_caller.paginate_by_next_page(initial_params))

    assert pages == [page1]
    assert mock_get.call_count == 1
    called_params_first = mock_get.call_args_list[0].kwargs["params"]
    assert called_params_first == {"limit": 1}
