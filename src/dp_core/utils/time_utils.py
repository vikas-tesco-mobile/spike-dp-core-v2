from datetime import datetime, timezone

from dp_core.utils.constants import TimeStringConstants


def build_dates_for_path():
    now = datetime.now(timezone.utc)

    return (
        now.strftime(TimeStringConstants.DATE_FORMAT),
        now.strftime(TimeStringConstants.HOUR_FORMAT),
        now.strftime(TimeStringConstants.DATETIME_FORMAT),
    )
