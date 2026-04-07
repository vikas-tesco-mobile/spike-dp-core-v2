from pathlib import Path
from unittest.mock import patch

import pytest
from pyspark import Row

from dp_core.exporter.data_exporter import DataExporter
from dp_core.utils.constants import ExporterConstants


@patch("dp_core.exporter.data_exporter.build_dates_for_path")
@patch("dp_core.exporter.data_exporter.rename_spark_part_files")
@patch("dp_core.exporter.data_exporter.DFReaderUtil.read_table")
@pytest.mark.parametrize(
    ("file_format", "expected_extension", "expected_spark_extension"),
    [
        (
            file_format,
            config["target_file_extension"],
            config["spark_write_file_extension"],
        )
        for file_format, config in ExporterConstants.SUPPORTED_FORMATS.items()
    ],
)
def test_export_creates_renamed_file(
    mock_read_table,
    mock_rename,
    mock_build_dates,
    spark,
    tmp_path,
    file_format,
    expected_extension,
    expected_spark_extension,
):
    configs = {
        "table_name": "dummy_table",
        "file_format": file_format,
        "filename_prefix": "testfile",
        "output_location": str(tmp_path),
        "additional_write_options": (
            {"header": "true"} if file_format == "csv" else {}
        ),
    }

    mock_build_dates.return_value = ("20240620", None, "2024062001")

    df = spark.createDataFrame(
        [
            Row(
                transaction_number="1",
                location_id="L1",
                event_timestamp="2023-01-01T00:00:00Z",
            ),
            Row(
                transaction_number="2",
                location_id="L2",
                event_timestamp="2024-01-01T00:00:00Z",
            ),
        ]
    )

    mock_read_table.return_value = df

    exporter = DataExporter(spark, configs)
    exporter.export()

    expected_path = f"{tmp_path}/20240620"
    expected_prefix = "testfile_2024062001"

    if file_format == "jsonl":
        mock_rename.assert_called_once_with(
            expected_path,
            expected_prefix,
            expected_extension,
            source_extension=expected_spark_extension,
        )
    else:
        mock_rename.assert_called_once_with(
            expected_path, expected_prefix, expected_extension
        )
    exported_files = [
        f for f in Path(expected_path).iterdir() if f.suffix == expected_spark_extension
    ]
    assert len(exported_files) == 1


def test_unsupported_file_format_raises_value_error(spark):
    configs = {
        "table_name": "dummy_table",
        "file_format": "xml",
        "filename_prefix": "testfile",
        "output_location": "/tmp/export",
    }

    with pytest.raises(ValueError, match="Unsupported file_format 'xml'"):
        DataExporter(spark, configs)
