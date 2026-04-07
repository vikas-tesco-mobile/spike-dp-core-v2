from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import lit, col

from conftest import generate_random_string, project_root
from dp_core.transformer.models.silver_layer_config import (
    SilverTableConfig,
    ColumnFlattenerConfig,
    SilverColumnConfig,
    SilverSchemaConfig,
)
from dp_core.transformer.silver_data_processor import SilverDataProcessor
from dp_core.utils.constants import LoadTypeConstants


def _default_silver_table_config() -> SilverTableConfig:
    return SilverTableConfig(
        name="",
        description=None,
        tags={},
        primary_keys=[],
        deduplication_keys=[],
        flatten_config=ColumnFlattenerConfig(cols_to_explode=[], cols_to_exclude=[]),
        clustering_keys=None,
        columns=[],
    )


@patch("dp_core.transformer.silver_data_processor.DFReaderUtil.read_table")
def test_read_with_date_range(mock_read_table, spark):
    data = [
        ("2024-01-01", 1),
        ("2024-01-02", 2),
        ("2024-01-03", 3),
    ]
    df = spark.createDataFrame(data, ["_ingested_at", "id"])

    job_configs = {
        "bronze_table": "dummy_table",
        "start_date": "2024-01-02",
        "end_date": "2024-01-03",
    }
    silver_table_config = _default_silver_table_config()

    processor = SilverDataProcessor(spark, job_configs, silver_table_config)

    mock_read_table.return_value = df
    processor.read()
    actual = processor.cleaned_df.collect()
    expected = [
        Row(_ingested_at="2024-01-02", id=2),
        Row(_ingested_at="2024-01-03", id=3),
    ]

    assert len(actual) == 2
    assert actual == expected


def test_flatten_with_array_cols(spark):
    data = [
        Row(
            total_count=2,
            locations=[
                Row(id=1, name="test1", nested=Row(level_two="value1")),
                Row(id=2, name="test2", nested=Row(level_two="value2")),
            ],
            _metadata=Row(file_name="test.yml"),
        )
    ]

    df = spark.createDataFrame(data)

    flattener_config = ColumnFlattenerConfig(
        cols_to_explode=["locations"], cols_to_exclude=["_metadata"]
    )
    silver_table_config = _default_silver_table_config()
    silver_table_config.flatten_config = flattener_config
    job_configs = {}

    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df

    processor.flatten()

    actual = processor.cleaned_df.collect()
    expected = [
        Row(
            total_count=2,
            locations__id=1,
            locations__name="test1",
            locations__nested__level_two="value1",
            _metadata=Row(file_name="test.yml"),
        ),
        Row(
            total_count=2,
            locations__id=2,
            locations__name="test2",
            locations__nested__level_two="value2",
            _metadata=Row(file_name="test.yml"),
        ),
    ]

    assert actual == expected


def test_flatten_with_array_cols_adds_index_when_configured(spark):
    data = [
        Row(
            total_count=2,
            locations=[
                Row(id=1, name="test1", nested=Row(level_two="value1")),
                Row(id=2, name="test2", nested=Row(level_two="value2")),
            ],
        )
    ]

    df = spark.createDataFrame(data)

    flattener_config = ColumnFlattenerConfig(
        cols_to_explode=["locations"],
        cols_to_exclude=[],
        explode_with_index_col=True,
    )
    silver_table_config = _default_silver_table_config()
    silver_table_config.flatten_config = flattener_config
    job_configs = {}

    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df

    processor.flatten()

    actual = processor.cleaned_df.collect()
    expected = [
        Row(
            total_count=2,
            locations__index=0,
            locations__id=1,
            locations__name="test1",
            locations__nested__level_two="value1",
        ),
        Row(
            total_count=2,
            locations__index=1,
            locations__id=2,
            locations__name="test2",
            locations__nested__level_two="value2",
        ),
    ]

    assert actual == expected


@patch("dp_core.transformer.silver_data_processor.current_timestamp")
def test_add_metadata_cols_with_existing_metadata(mock_current_timestamp, spark):
    mock_current_timestamp.return_value = lit(datetime(2025, 1, 1))
    data = [
        Row(
            id=1, _ingested_at=datetime(2025, 1, 1), _metadata=Row(file_name="test.yml")
        )
    ]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    job_configs = {}

    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df

    processor.add_metadata_cols()

    actual = processor.cleaned_df.collect()
    expected = [
        Row(
            id=1,
            _ingested_at=datetime(2025, 1, 1),
            _metadata=Row(
                file_name="test.yml",
                bronze_ingested_at=datetime(2025, 1, 1),
                silver_processed_at=datetime(2025, 1, 1),
            ),
        )
    ]
    assert actual == expected


@patch("dp_core.transformer.silver_data_processor.current_timestamp")
def test_add_metadata_cols_without_existing_metadata(mock_current_timestamp, spark):
    mock_current_timestamp.return_value = lit(datetime(2025, 1, 1))
    data = [Row(id=1, _ingested_at=datetime(2025, 1, 1))]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    job_configs = {}

    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df

    processor.add_metadata_cols()

    actual = processor.cleaned_df.collect()
    expected = [
        Row(
            id=1,
            _ingested_at=datetime(2025, 1, 1),
            _metadata=Row(
                bronze_ingested_at=datetime(2025, 1, 1),
                silver_processed_at=datetime(2025, 1, 1),
            ),
        )
    ]
    assert actual == expected


def test_enforce_schema_to_create_new_cols_with_new_name_and_type(spark):
    data = [Row(id="1", val="100", _metadata=Row(file_name="test.yml"))]

    silver_table_config = _default_silver_table_config()
    silver_column_configs = [
        SilverColumnConfig(name="identifier", source_col_name="id", type="string"),
        SilverColumnConfig(name="value", source_col_name="val", type="int"),
    ]
    silver_table_config.columns = silver_column_configs
    job_configs = {}

    df = spark.createDataFrame(data)
    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df

    processor.apply_schema_mappings()
    actual = processor.cleaned_df.collect()
    expected = [
        Row(
            id="1",
            val="100",
            _metadata=Row(file_name="test.yml"),
            identifier="1",
            value=100,
        )
    ]

    assert actual == expected


def test_enforce_schema_do_not_create_new_col_if_source_col_is_unavailable(spark):
    data = [Row(id="1", value="100", other_col="some-value")]

    silver_table_config = _default_silver_table_config()
    silver_column_configs = [
        SilverColumnConfig(name="id", source_col_name=None, type="string"),
        SilverColumnConfig(name="value", source_col_name=None, type="int"),
    ]
    silver_table_config.columns = silver_column_configs
    job_configs = {}

    df = spark.createDataFrame(data)
    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df

    processor.apply_schema_mappings()
    actual = processor.cleaned_df.collect()
    expected = [Row(id="1", value=100, other_col="some-value")]

    assert actual == expected


def test_enforce_schema_raises_when_target_col_cannot_be_mapped(spark):
    data = [Row(id="1")]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    silver_table_config.columns = [
        SilverColumnConfig(
            name="customer_id",
            source_col_name="source_customer_id",
            type="string",
        ),
    ]

    processor = SilverDataProcessor(spark, {}, silver_table_config)
    processor.cleaned_df = df

    with pytest.raises(ValueError) as exc_info:
        processor.apply_schema_mappings()

    assert (
        "Column `customer_id` is not available after schema mapping. "
        "Provide a valid `source_col_name`."
    ) in str(exc_info.value)


def test_enforce_schema_supports_column_transform_expressions(spark):
    data = [Row(id=1, first_name="john", last_name="doe")]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    silver_table_config.columns = [
        SilverColumnConfig(name="id", type="int"),
        SilverColumnConfig(
            name="full_name",
            type="string",
            transform="concat(first_name, ' ', last_name)",
        ),
        SilverColumnConfig(
            name="last_name_upper",
            type="string",
            transform="upper(last_name)",
        ),
        SilverColumnConfig(
            name="id_plus_10",
            type="int",
            transform="id + 10",
        ),
    ]

    processor = SilverDataProcessor(spark, {}, silver_table_config)
    processor.cleaned_df = df
    processor.apply_schema_mappings()

    actual = processor.cleaned_df.collect()
    expected = [
        Row(
            id=1,
            first_name="john",
            last_name="doe",
            full_name="john doe",
            last_name_upper="DOE",
            id_plus_10=11,
        )
    ]
    assert actual == expected


def test_enforce_schema_converts_numeric_date_with_transform_expression(spark):
    data = [Row(id=1, source_date_num=20260210)]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    silver_table_config.columns = [
        SilverColumnConfig(name="id", type="int"),
        SilverColumnConfig(
            name="event_date",
            type="date",
            transform="to_date(source_date_num, 'yyyyMMdd')",
        ),
    ]

    processor = SilverDataProcessor(spark, {}, silver_table_config)
    processor.cleaned_df = df
    processor.apply_schema_mappings()

    actual = processor.cleaned_df.collect()
    expected = [
        Row(id=1, source_date_num=20260210, event_date=datetime(2026, 2, 10).date())
    ]

    assert actual == expected


def test_deduplicate_should_keep_latest_based_on_pk_and_dedup_key(spark):
    data = [
        Row(id=1, modified_at="2024-01-01 00:00:00", val="value1"),
        Row(id=1, modified_at="2024-01-01 00:10:00", val="value11"),
        Row(id=2, modified_at="2024-01-01 10:10:00", val="value2"),
    ]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    silver_table_config.primary_keys = ["id"]
    silver_table_config.deduplication_keys = ["modified_at"]
    job_configs = {}
    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df

    processor.deduplicate()
    actual = processor.cleaned_df.collect()
    expected = [
        Row(id=1, modified_at="2024-01-01 00:10:00", val="value11"),
        Row(id=2, modified_at="2024-01-01 10:10:00", val="value2"),
    ]

    assert actual == expected


def test_deduplicate_should_filter_rows_with_null_or_empty_pk_and_dedup_keys(
    spark,
):
    data = [
        Row(id="1", modified_at="2024-01-01 00:00:00", val="value1"),
        Row(id="1", modified_at="2024-01-01 00:10:00", val="value11"),
        Row(id=None, modified_at="2024-01-02 00:00:00", val="null-id"),
        Row(id="2", modified_at=None, val="null-dedup"),
        Row(id="   ", modified_at="2024-01-03 00:00:00", val="blank-id"),
        Row(id="3", modified_at="   ", val="blank-dedup"),
        Row(id=None, modified_at=None, val="all-null"),
        Row(id="   ", modified_at="   ", val="all-blank"),
    ]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    silver_table_config.primary_keys = ["id"]
    silver_table_config.deduplication_keys = ["modified_at"]
    processor = SilverDataProcessor(spark, {}, silver_table_config)
    processor.cleaned_df = df

    processor.deduplicate()

    actual = processor.cleaned_df.collect()
    expected = [Row(id="1", modified_at="2024-01-01 00:10:00", val="value11")]

    assert actual == expected


def test_deduplicate_should_filter_composite_rows_with_missing_key_parts(spark):
    data = [
        Row(pk1="A", pk2="B", dk1="2024-01-01", dk2="1", val="keep"),
        Row(pk1="A", pk2=None, dk1="2024-01-01", dk2="1", val="keep-null-pk-part"),
        Row(pk1="A", pk2=" ", dk1="2024-01-01", dk2="1", val="keep-blank-pk-part"),
        Row(pk1="C", pk2="D", dk1=None, dk2="1", val="keep-null-dk-part"),
        Row(pk1="X", pk2="Y", dk1="2024-01-01", dk2=" ", val="keep-blank-dk-part"),
        Row(pk1=None, pk2=None, dk1="2024-01-01", dk2="1", val="drop-empty-pk-group"),
        Row(pk1=" ", pk2=" ", dk1="2024-01-01", dk2="1", val="drop-blank-pk-group"),
        Row(pk1="A", pk2="B", dk1=None, dk2=None, val="drop-empty-dk-group"),
        Row(pk1="A", pk2="B", dk1=" ", dk2=" ", val="drop-blank-dk-group"),
    ]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    silver_table_config.primary_keys = ["pk1", "pk2"]
    silver_table_config.deduplication_keys = ["dk1", "dk2"]
    processor = SilverDataProcessor(spark, {}, silver_table_config)
    processor.cleaned_df = df

    processor.deduplicate()

    actual = processor.cleaned_df.collect()
    expected = [
        Row(pk1="A", pk2="B", dk1="2024-01-01", dk2="1", val="keep"),
        Row(pk1="A", pk2=" ", dk1="2024-01-01", dk2="1", val="keep-blank-pk-part"),
        Row(pk1="A", pk2=None, dk1="2024-01-01", dk2="1", val="keep-null-pk-part"),
        Row(pk1="C", pk2="D", dk1=None, dk2="1", val="keep-null-dk-part"),
        Row(pk1="X", pk2="Y", dk1="2024-01-01", dk2=" ", val="keep-blank-dk-part"),
    ]

    assert sorted(actual, key=lambda x: x.val) == sorted(expected, key=lambda x: x.val)


def test_upsert_should_create_table_if_not_exists(spark):
    data = [
        Row(id=1, modified_at="2024-01-01 00:00:00", val="value1"),
        Row(id=2, modified_at="2024-01-01 10:10:00", val="value2"),
    ]
    df = spark.createDataFrame(data)
    sample_silver_table = (
        f"spark_catalog.default.sample_silver_table_{generate_random_string()}"
    )

    silver_table_config = _default_silver_table_config()
    silver_table_config.primary_keys = ["id"]
    silver_table_config.deduplication_keys = ["modified_at"]
    job_configs = {"silver_table": sample_silver_table}
    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df

    assert not spark.catalog.tableExists(sample_silver_table)

    processor.upsert()

    assert spark.catalog.tableExists(sample_silver_table)
    actual = spark.read.table(sample_silver_table).sort("id").collect()
    expected = [
        Row(id=1, modified_at="2024-01-01 00:00:00", val="value1"),
        Row(id=2, modified_at="2024-01-01 10:10:00", val="value2"),
    ]

    assert actual == expected


def test_upsert_should_upsert_records_if_table_exists(spark):
    existing_data = [
        Row(id=1, modified_at="2024-01-01 00:00:00", val="value1"),
        Row(id=2, modified_at="2024-01-01 10:10:00", val="value2"),
    ]
    existing_df = spark.createDataFrame(existing_data)
    sample_silver_table = (
        f"spark_catalog.default.sample_silver_table_{generate_random_string()}"
    )
    existing_df.write.format("delta").mode("overwrite").saveAsTable(sample_silver_table)

    data = [
        Row(id=1, modified_at="2024-01-01 00:10:00", val="value11"),
        Row(id=3, modified_at="2024-01-01 20:20:00", val="value33"),
    ]
    df = spark.createDataFrame(data)
    silver_table_config = _default_silver_table_config()
    silver_table_config.primary_keys = ["id"]
    silver_table_config.deduplication_keys = ["modified_at"]
    job_configs = {"silver_table": sample_silver_table}
    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df

    processor.upsert()

    assert spark.catalog.tableExists(sample_silver_table)
    actual = spark.read.table(sample_silver_table).sort("id").collect()
    expected = [
        Row(id=1, modified_at="2024-01-01 00:10:00", val="value11"),
        Row(id=2, modified_at="2024-01-01 10:10:00", val="value2"),
        Row(id=3, modified_at="2024-01-01 20:20:00", val="value33"),
    ]

    assert actual == expected


@patch("dp_core.transformer.silver_data_processor.current_timestamp")
def test_process(mock_current_timestamp, spark):
    mock_current_timestamp.return_value = lit(datetime(2025, 11, 27))
    sample_bronze_table = (
        f"spark_catalog.default.sample_bronze_table_{generate_random_string()}"
    )
    bronze_table_data_path = (
        f"{project_root()}/tests/data/api/locations/sample_bronze_table.json"
    )
    df = (
        spark.read.option("multiline", True)
        .option("inferSchema", True)
        .json(bronze_table_data_path)
    )
    df.write.format("delta").saveAsTable(sample_bronze_table)

    sample_silver_table = (
        f"spark_catalog.default.sample_silver_table_{generate_random_string()}"
    )

    schema_configs = {
        "version": 1.0,
        "table": {
            "name": sample_silver_table,
            "primary_keys": ["location_id"],
            "deduplication_keys": ["modified_at"],
            "flatten_config": {
                "cols_to_explode": ["locations"],
                "cols_to_exclude": ["_metadata"],
            },
            "columns": [
                {
                    "name": "location_id",
                    "source_col_name": "locations__location__id",
                    "type": "string",
                },
                {
                    "name": "location_name",
                    "source_col_name": "locations__location__name",
                    "type": "string",
                },
                {
                    "name": "country_code",
                    "source_col_name": "locations__location__country__countryCode",
                    "type": "string",
                },
                {
                    "name": "store_format",
                    "source_col_name": "locations__location__format__storeFormat",
                    "type": "string",
                },
                {
                    "name": "detailed_store_format",
                    "source_col_name": "locations__location__format__detailedStoreFormat",
                    "type": "string",
                },
                {
                    "name": "created_at",
                    "source_col_name": "locations__audit__createdAt",
                    "type": "string",
                },
                {
                    "name": "modified_at",
                    "source_col_name": "locations__audit__modifiedAt",
                    "type": "string",
                },
            ],
        },
    }

    schema_config = SilverSchemaConfig(**schema_configs)
    silver_table_config = schema_config.table
    job_configs = {
        "silver_table": sample_silver_table,
        "bronze_table": sample_bronze_table,
    }
    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.process()

    actual = spark.read.table(sample_silver_table).sort("location_id").collect()
    expected = [
        Row(
            location_id="9daa5428-2244-409b-96ac-695c5d71a41e",
            location_name="Henleaze Esso Express - Petrol Filling Station",
            country_code="GB",
            store_format="Extra",
            detailed_store_format="Extra",
            created_at="2021-03-10T13:38:58.707922Z",
            modified_at="2022-12-13T13:56:32.202502Z",
            _metadata=Row(
                file_block_length="43274",
                file_block_start="0",
                file_modification_time="2025-11-26T03:36:28.000Z",
                file_name="locations_2025112604_9100.json",
                file_path="/some-path/locations_2025112604_9100.json",
                file_size="43274",
                bronze_ingested_at="2025-11-26T03:37:13.326",
                silver_processed_at=datetime(2025, 11, 27, 0, 0),
            ),
        ),
        Row(
            location_id="9dab8991-c382-4749-b1d0-bbf985b112b1",
            location_name="Folkestone Superstore - Scan as you Shop",
            country_code="GB",
            store_format="Extra",
            detailed_store_format="Extra",
            created_at="2019-10-18T11:12:05.087176Z",
            modified_at="2021-03-10T13:43:59.063587Z",
            _metadata=Row(
                file_block_length="43274",
                file_block_start="0",
                file_modification_time="2025-11-26T03:36:28.000Z",
                file_name="locations_2025112604_9100.json",
                file_path="/some-path/locations_2025112604_9100.json",
                file_size="43274",
                bronze_ingested_at="2025-11-26T03:37:13.326",
                silver_processed_at=datetime(2025, 11, 27, 0, 0),
            ),
        ),
    ]
    assert actual == expected


def test_apply_schema_mappings_hashes_pii_columns_and_preserves_nulls(spark):
    data = [
        Row(identifier="1", email="user1@example.com", other="x"),
        Row(identifier="2", email=None, other="y"),
    ]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    silver_table_config.columns = [
        SilverColumnConfig(name="identifier", type="string"),
        SilverColumnConfig(name="email", type="string", create_hashed_col=True),
        SilverColumnConfig(name="other", type="string"),
    ]
    job_configs = {}

    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df
    processor.apply_schema_mappings()

    actual = processor.cleaned_df.collect()
    expected = [
        Row(
            identifier="1",
            email="user1@example.com",
            other="x",
            email_hashed_key="b36a83701f1c3191e19722d6f90274bc1b5501fe69ebf33313e440fe4b0fe210",
        ),
        Row(identifier="2", email=None, other="y", email_hashed_key=None),
    ]
    assert actual == expected


def test_apply_schema_mappings_raises_if_hashed_column_is_missing(spark):
    data = [Row(identifier="1", other="x")]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    silver_table_config.columns = [
        SilverColumnConfig(name="identifier", type="string"),
        SilverColumnConfig(name="email", type="string", create_hashed_col=True),
        SilverColumnConfig(name="other", type="string"),
    ]
    job_configs = {}

    processor = SilverDataProcessor(spark, job_configs, silver_table_config)
    processor.cleaned_df = df

    with pytest.raises(ValueError) as exc_info:
        processor.apply_schema_mappings()

    assert "Column `email` is not available after schema mapping." in str(
        exc_info.value
    )


def test_hashed_pii_no_false_join_on_nulls(spark):
    data = [
        Row(id=1, email=None),
        Row(id=2, email=None),
        Row(id=3, email="a@b.com"),
        Row(id=4, email="a@b.com"),
    ]
    df = spark.createDataFrame(data)

    silver_table_config = _default_silver_table_config()
    silver_table_config.columns = [
        SilverColumnConfig(name="id", type="int"),
        SilverColumnConfig(name="email", type="string", create_hashed_col=True),
    ]
    processor = SilverDataProcessor(spark, {}, silver_table_config)
    processor.cleaned_df = df
    processor.apply_schema_mappings()

    df1 = processor.cleaned_df.alias("df1")
    df2 = processor.cleaned_df.alias("df2")
    joined = df1.join(
        df2,
        (col("df1.email_hashed_key") == col("df2.email_hashed_key"))
        & (col("df1.id") != col("df2.id")),
        "inner",
    )

    pairs = [
        tuple(sorted((row[0], row[2])))
        for row in joined.select("df1.id", "df1.email_hashed_key", "df2.id").collect()
    ]
    unique_pairs = sorted(set(pairs))
    assert unique_pairs == [(3, 4)]


@patch("dp_core.transformer.silver_data_processor.DFReaderUtil.read_table")
def test_read_full_load_when_silver_table_does_not_exist(mock_read_table, spark):
    data = [
        ("2024-01-01", 1),
        ("2024-01-02", 2),
        ("2024-01-03", 3),
    ]
    bronze_df = spark.createDataFrame(data, ["_ingested_at", "id"])

    job_configs = {
        "bronze_table": "dummy_bronze",
        "silver_table": "dummy_silver",
    }
    silver_table_config = _default_silver_table_config()
    processor = SilverDataProcessor(spark, job_configs, silver_table_config)

    mock_read_table.return_value = bronze_df

    with patch.object(spark.catalog, "tableExists", return_value=False):
        processor.read()

    actual = processor.cleaned_df.collect()
    expected = [
        Row(_ingested_at="2024-01-01", id=1),
        Row(_ingested_at="2024-01-02", id=2),
        Row(_ingested_at="2024-01-03", id=3),
    ]

    assert actual == expected


@patch("dp_core.transformer.silver_data_processor.DFReaderUtil.read_table")
def test_read_full_load_when_silver_table_is_empty(mock_read_table, spark):
    data = [
        ("2024-01-01", 1),
        ("2024-01-02", 2),
        ("2024-01-03", 3),
    ]
    bronze_df = spark.createDataFrame(data, ["_ingested_at", "id"])
    empty_silver_df = spark.createDataFrame([], bronze_df.schema)

    job_configs = {
        "bronze_table": "dummy_bronze",
        "silver_table": "dummy_silver",
    }
    silver_table_config = _default_silver_table_config()
    processor = SilverDataProcessor(spark, job_configs, silver_table_config)

    def side_effect(*args, **kwargs):
        table_name = kwargs.get("table_name") or (args[1] if len(args) > 1 else None)
        if table_name == job_configs["bronze_table"]:
            return bronze_df
        return empty_silver_df

    mock_read_table.side_effect = side_effect

    with patch.object(spark.catalog, "tableExists", return_value=True):
        processor.read()

    actual = processor.cleaned_df.collect()
    expected = [
        Row(_ingested_at="2024-01-01", id=1),
        Row(_ingested_at="2024-01-02", id=2),
        Row(_ingested_at="2024-01-03", id=3),
    ]

    assert actual == expected


@patch("dp_core.transformer.silver_data_processor.DFReaderUtil.read_table")
def test_read_incremental_without_date_range(mock_read_table, spark):
    data = [
        ("2024-01-01", 1),
        ("2024-01-02", 2),
        ("2024-01-03", 3),
    ]
    bronze_df = spark.createDataFrame(data, ["_ingested_at", "id"])

    silver_data = [
        Row(id=1, _metadata=Row(bronze_ingested_at="2024-01-02")),
    ]
    silver_df = spark.createDataFrame(silver_data)

    job_configs = {
        "bronze_table": "dummy_bronze",
        "silver_table": "dummy_silver",
    }
    silver_table_config = _default_silver_table_config()
    processor = SilverDataProcessor(spark, job_configs, silver_table_config)

    def side_effect(*args, **kwargs):
        table_name = kwargs.get("table_name") or (args[1] if len(args) > 1 else None)
        if table_name == job_configs["bronze_table"]:
            return bronze_df
        return silver_df

    mock_read_table.side_effect = side_effect

    with patch.object(spark.catalog, "tableExists", return_value=True):
        processor.read()

    results = processor.cleaned_df.collect()

    assert len(results) == 1
    assert results == [Row(_ingested_at="2024-01-03", id=3)]


@patch("dp_core.transformer.silver_data_processor.DFReaderUtil.read_table")
def test_read_raises_when_bronze_ingested_at_missing_in_silver_metadata(
    mock_read_table, spark
):
    data = [
        ("2024-01-01", 1),
        ("2024-01-02", 2),
    ]
    bronze_df = spark.createDataFrame(data, ["_ingested_at", "id"])

    silver_data = [Row(id=1)]
    silver_df = spark.createDataFrame(silver_data)

    job_configs = {
        "bronze_table": "dummy_bronze",
        "silver_table": "dummy_silver",
    }
    silver_table_config = _default_silver_table_config()
    processor = SilverDataProcessor(spark, job_configs, silver_table_config)

    def side_effect(*args, **kwargs):
        table_name = kwargs.get("table_name") or (args[1] if len(args) > 1 else None)
        if table_name == job_configs["bronze_table"]:
            return bronze_df
        return silver_df

    mock_read_table.side_effect = side_effect

    with patch.object(spark.catalog, "tableExists", return_value=True):
        with pytest.raises(ValueError) as exc_info:
            processor.read()

    msg = str(exc_info.value)
    assert (
        "bronze_ingested_at timestamp not found in silver table dummy_silver metadata column"
        in msg
    )


@patch("dp_core.transformer.silver_data_processor.logger")
def test_process_logs_full_load_empty_when_no_bronze_data(mock_logger):
    silver_table_config = _default_silver_table_config()
    processor = SilverDataProcessor(Mock(), {}, silver_table_config)
    processor.cleaned_df = Mock()
    processor.cleaned_df.isEmpty.return_value = True
    processor._read_mode = LoadTypeConstants.FULL_LOAD

    with (
        patch.object(processor, "read", return_value=None),
        patch.object(processor, "flatten") as mock_flatten,
        patch.object(processor, "add_metadata_cols"),
        patch.object(processor, "apply_schema_mappings"),
        patch.object(processor, "select_silver_schema_columns"),
        patch.object(processor, "deduplicate"),
        patch.object(processor, "upsert") as mock_upsert,
    ):
        processor.process()

    mock_flatten.assert_called_once()
    mock_upsert.assert_called_once()
    mock_logger.info.assert_called_with(
        "No bronze data found for full load. Creating/refreshing empty target table schema."
    )


@patch("dp_core.transformer.silver_data_processor.logger")
def test_process_logs_date_range_empty_when_no_bronze_data(mock_logger):
    silver_table_config = _default_silver_table_config()
    processor = SilverDataProcessor(Mock(), {}, silver_table_config)
    processor.cleaned_df = Mock()
    processor.cleaned_df.isEmpty.return_value = True
    processor._read_mode = LoadTypeConstants.DATE_RANGE

    with (
        patch.object(processor, "read", return_value=None),
        patch.object(processor, "flatten") as mock_flatten,
    ):
        processor.process()

    mock_flatten.assert_not_called()
    mock_logger.info.assert_called_with(
        "No bronze data found in the requested date range. Skipping processing."
    )


@patch("dp_core.transformer.silver_data_processor.logger")
def test_process_logs_incremental_empty_as_up_to_date(mock_logger):
    silver_table_config = _default_silver_table_config()
    processor = SilverDataProcessor(Mock(), {}, silver_table_config)
    processor.cleaned_df = Mock()
    processor.cleaned_df.isEmpty.return_value = True
    processor._read_mode = LoadTypeConstants.INCREMENTAL

    with (
        patch.object(processor, "read", return_value=None),
        patch.object(processor, "flatten") as mock_flatten,
    ):
        processor.process()

    mock_flatten.assert_not_called()
    mock_logger.info.assert_called_with(
        "Target table is up to date. No new data to process from bronze table."
    )
