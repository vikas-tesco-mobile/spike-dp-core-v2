import pytest
from unittest.mock import patch, MagicMock
import requests
import json
from dp_core.extractor.location_api_extractor import LocationAPIExtractor


@pytest.fixture
def config():
    return {
        "location_url": "https://api.example.com/locations",
        "target_path": "/tmp/output",
        "request_params": {"fields": "format"},
        "limit": 100,
    }


@pytest.fixture
def mock_auth():
    mock = MagicMock()
    mock.get_auth_header.return_value = {"Authorization": "Bearer testtoken"}
    return mock


def test_extract_locations(config, mock_auth):
    extractor = LocationAPIExtractor(config, mock_auth)
    assert extractor._extract_locations({"locations": [1]}) == [1]
    assert extractor._extract_locations({}) == []


@patch("dp_core.extractor.location_api_extractor.requests.get")
def test_fetch_success(mock_get, config, mock_auth):
    extractor = LocationAPIExtractor(config, mock_auth)

    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {"locations": [1]}
    mock_get.return_value = mock_resp

    result = extractor._fetch(0)

    assert result == {"locations": [1]}
    mock_get.assert_called_once()


# @patch("dp_core.extractor.location_api_extractor.requests.get")
# def test_fetch_invalid_url(mock_get, config, mock_auth):
#     config["location_url"] = "not a url"
#     extractor = LocationAPIExtractor(config, mock_auth)
#
#     with pytest.raises(ValueError):
#         extractor._fetch(0)


@patch("dp_core.extractor.location_api_extractor.requests.get")
def test_fetch_http_error(mock_get, config, mock_auth):
    extractor = LocationAPIExtractor(config, mock_auth)

    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("boom")
    mock_get.return_value = mock_resp

    with pytest.raises(requests.exceptions.HTTPError):
        extractor._fetch(0)


@patch("dp_core.extractor.location_api_extractor.write_json")
@patch("dp_core.extractor.location_api_extractor.build_dates_for_path")
def test_save_response_json(mock_dates, mock_write, config, mock_auth):
    extractor = LocationAPIExtractor(config, mock_auth)

    mock_dates.return_value = ("20250101", "10", "20250101100000")
    resp_data = {"locations": [1]}

    extractor._save_response_json(resp_data, offset=100)

    expected_path = "/tmp/output/20250101/10/locations_20250101100000_100.json"

    mock_write.assert_called_once_with(expected_path, json.dumps(resp_data))


@patch("dp_core.extractor.location_api_extractor.write_json")
@patch("dp_core.extractor.location_api_extractor.LocationAPIExtractor._fetch")
def test_run_pagination(mock_fetch, mock_write, config, mock_auth):
    extractor = LocationAPIExtractor(config, mock_auth)

    mock_fetch.side_effect = [
        {"totalCount": 250, "locations": [1]},
        {"locations": [2]},
        {"locations": [3]},
    ]

    extractor.run()

    assert mock_fetch.call_count == 3
    assert mock_write.call_count == 3


@patch("dp_core.extractor.location_api_extractor.write_json")
@patch("dp_core.extractor.location_api_extractor.LocationAPIExtractor._fetch")
def test_run_stops_on_empty(mock_fetch, mock_write, config, mock_auth):
    extractor = LocationAPIExtractor(config, mock_auth)

    mock_fetch.side_effect = [
        {"totalCount": 200, "locations": [1]},
        {"locations": []},  # stops here
    ]

    extractor.run()

    assert mock_fetch.call_count == 2
    assert mock_write.call_count == 1
