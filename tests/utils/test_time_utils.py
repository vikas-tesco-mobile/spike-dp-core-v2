from unittest.mock import patch
from dp_core.utils.time_utils import build_dates_for_path


def test_build_dates_for_path_mock_datetime():
    from datetime import datetime as real_dt, timezone

    real_tz = timezone.utc

    class MockDateTime:
        @staticmethod
        def now(tz=None):
            return real_dt(2025, 1, 1, 10, 25, 47, tzinfo=real_tz)

        @staticmethod
        def fromtimestamp(ts, tz=None):
            return real_dt.fromtimestamp(ts, tz)

        def strftime(self, fmt):
            return real_dt.strftime(self, fmt)

    with patch("dp_core.utils.time_utils.datetime", MockDateTime):
        date_str, hour_str, rounded = build_dates_for_path()

    assert date_str == "20250101"
    assert hour_str == "10"
    assert rounded == "20250101102547"
