import json
from typing import Dict
from unittest.mock import patch, call

from dp_core.extractor.products_extractor import ProductsExtractor
from dp_core.utils.api_auth import BearerAuth


class DummyBearerAuth(BearerAuth):
    def __init__(self) -> None:
        super().__init__(
            {
                "auth_url": "https://auth.example.com/token",
                "secret_scope": "dummy-scope",
                "client_id_key": "dummy-client-id",
                "client_secret_key": "dummy-client-secret",
            }
        )

    def get_auth_header(self) -> Dict[str, str]:  # type: ignore[override]
        return {"Authorization": "Bearer dummy-token"}


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


@patch("dp_core.extractor.products_extractor.build_dates_for_path")
@patch("dp_core.extractor.products_extractor.write_json")
@patch("dp_core.extractor.products_extractor.APICaller.get")
@patch("dp_core.extractor.products_extractor.APICaller.paginate_by_next_page")
def test_run_processes_batches(
    mock_paginate, mock_get, mock_write_json, mock_build_dates
):
    # Simulate API pagination returning productResults
    mock_paginate.side_effect = [
        # For first sectionCode
        [
            {"productResults": [{"uniqueKey": "p1"}, {"uniqueKey": "p2"}]},
            {"productResults": [{"uniqueKey": "p3"}]},
        ],
        # For second sectionCode
        [
            {"productResults": [{"uniqueKey": "p4"}]},
        ],
    ]
    mock_get.side_effect = [
        {"products": [{"uniqueKey": "p1"}, {"uniqueKey": "p2"}]},
        {"products": [{"uniqueKey": "p3"}, {"uniqueKey": "p4"}]},
    ]
    mock_build_dates.return_value = ["2024-01-01", "02", "2024010102"]

    # Config now has multiple section codes
    config = {
        "product_list": {
            "url": "https://api.example.com/products/list",
            "request_params": {"sectionCode": ["V11", "V13"]},
        },
        "products_by_id": {"url": "https://api.example.com/products/by_id"},
        "target_path": "/tmp/products",
        "batch_size": 2,
    }
    auth = DummyBearerAuth()
    extractor = ProductsExtractor(config, auth)

    extractor.run()

    # paginate_by_next_page should be called once per section code with correct params
    expected_paginate_calls = [
        call({"sectionCode": "V11"}),
        call({"sectionCode": "V13"}),
    ]
    assert mock_paginate.call_args_list == expected_paginate_calls

    # Validate total GET calls for products_by_id
    assert mock_get.call_count == 2
    assert mock_write_json.call_count == 2

    expected_paths = [
        "/tmp/products/2024-01-01/02/products_2024010102_1.json",
        "/tmp/products/2024-01-01/02/products_2024010102_2.json",
    ]
    actual_paths = [args[0] for args, _ in mock_write_json.call_args_list]
    assert actual_paths == expected_paths
