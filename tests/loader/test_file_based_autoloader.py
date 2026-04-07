import json

from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.testing import assertDataFrameEqual

from conftest import generate_random_string
from dp_core.loader.file_based_autoloader import FileBasedAutoloader


def test_transform_adds_required_columns(spark, monkeypatch):
    sample_df = spark.createDataFrame([Row(id=1, name="Sample", _metadata="meta_info")])

    autoloader = FileBasedAutoloader(spark, configs={})
    autoloader.raw_df = sample_df

    expected_raw_json = json.dumps({"id": 1, "name": "Sample"}, separators=(",", ":"))

    monkeypatch.setattr(
        "dp_core.loader.file_based_autoloader.current_timestamp",
        lambda: lit("2025-01-01 00:00:00"),
    )
    monkeypatch.setattr(
        "dp_core.loader.file_based_autoloader.current_date",
        lambda: lit("2025-01-01"),
    )

    autoloader.transform()

    actual = autoloader.raw_df
    expected = spark.createDataFrame(
        [
            Row(
                id=1,
                name="Sample",
                _metadata="meta_info",
                _raw_data=expected_raw_json,
                _ingested_at="2025-01-01 00:00:00",
                _ingestion_date="2025-01-01",
            )
        ]
    )
    assertDataFrameEqual(actual, expected)


def test_write_creates_delta_table_in_append_mode(spark, tmp_path):
    data1 = {"id": 1, "name": "Sample1"}
    data2 = {"id": 2, "name": "Sample2", "price": 12.0}
    table_name = f"spark_catalog.default.sample_table_{generate_random_string()}"
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True),
        ]
    )

    data_path = tmp_path / "data"
    data_path.mkdir()

    __create_sample_json_file(data_path, "sample1.json", data1)
    __create_sample_json_file(data_path, "sample2.json", data2)

    configs = {
        "trigger_option_available_now": True,
        "target_table": table_name,
        "checkpoint_location": f"{tmp_path}",
    }

    autoloader = FileBasedAutoloader(spark, configs=configs)
    autoloader.raw_df = spark.readStream.schema(schema).json(str(data_path))

    autoloader.write()

    expected = spark.createDataFrame(
        [
            Row(id="1", name="Sample1", price=None),
            Row(id="2", name="Sample2", price=12.0),
        ]
    )
    actual = spark.read.format("delta").table(table_name).sort("id")
    assertDataFrameEqual(actual, expected)


def __create_sample_json_file(data_path, file_name, data):
    json_file_path = data_path / file_name
    json_file_path.write_text(json.dumps(data, separators=(",", ":")))
