from contextlib import nullcontext as does_not_raise

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from conftest import generate_random_string
from dp_core.utils.df_utils import (
    DFReaderUtil,
    DFUtil,
    DFWriterUtil,
    DFTransformUtil,
)


@pytest.mark.parametrize(
    "mapping, expected_cols, expected_outcome",
    [
        (
            {"id": "identifier", "val": "value"},
            ["identifier", "value"],
            does_not_raise(),
        ),
        (
            {"missing": "identifier", "val": "value"},
            ["id", "val"],
            pytest.raises(ValueError),
        ),
    ],
)
def test_rename_columns_parametrized(spark, mapping, expected_cols, expected_outcome):
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "val"])
    with expected_outcome:
        renamed = DFUtil.rename_columns(df, mapping)
        expected = spark.createDataFrame([(1, "x"), (2, "y")], expected_cols)
        assertDataFrameEqual(
            renamed.orderBy(expected_cols[0]), expected.orderBy(expected_cols[0])
        )


@pytest.mark.parametrize(
    "rows, required_col_groups, expected_rows, expected_outcome",
    [
        (
            [
                ("a", "2024-01-01 00:00:00", 10),
                (None, "2024-01-02 00:00:00", 20),
                ("c", None, 30),
                ("   ", "2024-01-03 00:00:00", 40),
            ],
            [["id"], ["event_timestamp"]],
            [("a", "2024-01-01 00:00:00", 10)],
            does_not_raise(),
        ),
        (
            [
                ("1", "A", "2024-01-01", "1", 10),
                ("1", None, "2024-01-01", "1", 20),
                (None, "A", "2024-01-01", "1", 30),
                (None, None, "2024-01-01", "1", 40),
                ("1", "A", "2024-01-01", None, 50),
                ("1", "A", None, "1", 60),
                ("1", "A", None, None, 70),
                (" ", " ", "2024-01-01", "1", 80),
                ("1", "A", " ", " ", 90),
            ],
            [["id", "sub_id"], ["modified_at", "version"]],
            [
                ("1", "A", "2024-01-01", "1", 10),
                ("1", None, "2024-01-01", "1", 20),
                (None, "A", "2024-01-01", "1", 30),
                ("1", "A", "2024-01-01", None, 50),
                ("1", "A", None, "1", 60),
            ],
            does_not_raise(),
        ),
        (
            [("a", "2024-01-01 00:00:00", 10)],
            [["missing_col"]],
            None,
            pytest.raises(ValueError),
        ),
    ],
)
def test_filter_non_null_rows_parametrized(
    spark, rows, required_col_groups, expected_rows, expected_outcome
):
    schema = (
        ["id", "event_timestamp", "metric"]
        if len(rows[0]) == 3
        else ["id", "sub_id", "modified_at", "version", "metric"]
    )
    df = spark.createDataFrame(rows, schema)

    with expected_outcome:
        filtered = DFUtil.filter_non_null_rows(df, required_col_groups)
        if expected_rows is not None:
            expected_df = spark.createDataFrame(expected_rows, schema=df.schema)
            order_cols = [c for c in df.columns if c != "metric"]
            assertDataFrameEqual(
                filtered.orderBy(*order_cols), expected_df.orderBy(*order_cols)
            )


@pytest.mark.parametrize(
    "rows, expected_rows, primary_keys_cols, dedup_col_names, expected_outcome",
    [
        (
            [
                ("a", "2024-01-01 00:00:00", 10),
                ("a", "2024-01-02 00:00:00", 20),
                ("b", "2024-01-03 00:00:00", 30),
            ],
            [
                ("a", "2024-01-02 00:00:00", 20),
                ("b", "2024-01-03 00:00:00", 30),
            ],
            ["id"],
            ["event_timestamp"],
            does_not_raise(),
        ),
        (
            [
                ("a", None, 10),
                ("a", "2024-01-02 00:00:00", 20),
                ("b", "2024-01-03 00:00:00", 30),
            ],
            [
                ("a", "2024-01-02 00:00:00", 20),
                ("b", "2024-01-03 00:00:00", 30),
            ],
            ["id"],
            ["event_timestamp"],
            does_not_raise(),
        ),
        (
            [
                ("a", "2024-01-02 00:00:00", 20),
            ],
            None,
            ["missing_id"],
            ["event_timestamp"],
            pytest.raises(Exception),
        ),
        (
            [
                ("a", "2024-01-02 00:00:00", 20),
            ],
            None,
            ["id"],
            ["missing_event_timestamp"],
            pytest.raises(Exception),
        ),
    ],
)
def test_deduplicate_data_parametrized(
    spark, rows, expected_rows, primary_keys_cols, dedup_col_names, expected_outcome
):
    df = (
        spark.createDataFrame(rows, ["id", "event_timestamp", "metric"]).withColumn(
            "event_timestamp", F.to_timestamp("event_timestamp")
        )
        if rows
        else spark.createDataFrame(
            [], "id string, event_timestamp timestamp, metric int"
        )
    )
    with expected_outcome:
        primary_keys = [F.col(c) for c in primary_keys_cols]
        dedup_cols = [F.col(c) for c in dedup_col_names]
        deduped = DFUtil.deduplicate_data(
            df, primary_keys=primary_keys, dedup_cols=dedup_cols
        )
        _ = deduped.count()
        if expected_rows is not None:
            expected_df = (
                spark.createDataFrame(
                    expected_rows, ["id", "event_timestamp", "metric"]
                ).withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
                if expected_rows
                else spark.createDataFrame(
                    [], "id string, event_timestamp timestamp, metric int"
                )
            )
            assertDataFrameEqual(
                deduped.orderBy("id", "event_timestamp"),
                expected_df.orderBy("id", "event_timestamp"),
            )


@pytest.mark.parametrize(
    "table_setup, table_name, expected_outcome",
    [
        ([(1, "alpha"), (2, "beta")], "unit_test_table", does_not_raise()),
        (None, "nonexistent_table_xyz", pytest.raises(Exception)),
    ],
)
def test_read_source_table_parametrized(
    spark, table_setup, table_name, expected_outcome
):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    base_df = None
    if table_setup is not None:
        base_df = spark.createDataFrame(table_setup, ["id", "name"]).orderBy("id")
        base_df.write.mode("overwrite").saveAsTable(table_name)
    with expected_outcome:
        loaded = DFReaderUtil.read_table(spark, table_name)
        _ = loaded.count()
        if base_df is not None:
            assertDataFrameEqual(loaded.orderBy("id"), base_df)


def test_delta_merge_should_create_table_if_not_exists(spark):
    data = [
        Row(id=1, modified_at="2024-01-01 00:00:00", val="value1"),
        Row(id=2, modified_at="2024-01-01 10:10:00", val="value2"),
    ]
    df = spark.createDataFrame(data)
    sample_silver_table = (
        f"spark_catalog.default.sample_silver_table_{generate_random_string()}"
    )
    merge_key = "old.id = new.id"
    merge_update_condition = "new.modified_at >= old.modified_at"
    clustering_keys = None

    assert not spark.catalog.tableExists(sample_silver_table)

    DFWriterUtil.delta_merge(
        spark,
        df,
        sample_silver_table,
        merge_key,
        merge_update_condition,
        clustering_keys,
    )

    assert spark.catalog.tableExists(sample_silver_table)
    actual = spark.read.table(sample_silver_table).sort("id").collect()
    expected = [
        Row(id=1, modified_at="2024-01-01 00:00:00", val="value1"),
        Row(id=2, modified_at="2024-01-01 10:10:00", val="value2"),
    ]

    assert actual == expected


def test_delta_merge_should_upsert_records_if_table_exists(spark):
    existing_data = [
        Row(id=1, modified_at="2024-01-01 00:00:00", val="value1"),
        Row(id=2, modified_at="2024-01-01 10:10:00", val="value2"),
    ]
    existing_df = spark.createDataFrame(existing_data)
    sample_silver_table = (
        f"spark_catalog.default.sample_silver_table_{generate_random_string()}"
    )
    merge_key = "old.id = new.id"
    merge_update_condition = "new.modified_at >= old.modified_at"
    clustering_keys = None

    existing_df.write.format("delta").mode("overwrite").saveAsTable(sample_silver_table)

    new_data = [
        Row(id=1, modified_at="2024-01-01 00:10:00", val="value11"),
        Row(id=2, modified_at="2024-01-01 00:10:00", val="value11"),
        Row(id=3, modified_at="2024-01-01 20:20:00", val="value33"),
    ]
    df = spark.createDataFrame(new_data)

    DFWriterUtil.delta_merge(
        spark,
        df,
        sample_silver_table,
        merge_key,
        merge_update_condition,
        clustering_keys,
    )

    assert spark.catalog.tableExists(sample_silver_table)
    actual = spark.read.table(sample_silver_table).sort("id").collect()
    expected = [
        Row(id=1, modified_at="2024-01-01 00:10:00", val="value11"),
        Row(id=2, modified_at="2024-01-01 10:10:00", val="value2"),
        Row(id=3, modified_at="2024-01-01 20:20:00", val="value33"),
    ]

    assert actual == expected


def test_delta_merge_should_match_null_primary_keys_with_null_safe_merge_key(spark):
    existing_data = [
        Row(id=100, sub_id=None, modified_at="2024-01-01 00:00:00", val="value0"),
        Row(id=1, sub_id="A", modified_at="2024-01-01 10:10:00", val="value1"),
    ]
    existing_df = spark.createDataFrame(existing_data)
    sample_silver_table = (
        f"spark_catalog.default.sample_silver_table_{generate_random_string()}"
    )
    existing_df.write.format("delta").mode("overwrite").saveAsTable(sample_silver_table)

    new_data = [
        Row(id=100, sub_id=None, modified_at="2024-01-01 00:10:00", val="value00"),
        Row(id=2, sub_id="B", modified_at="2024-01-01 20:20:00", val="value2"),
    ]
    df = spark.createDataFrame(new_data)

    DFWriterUtil.delta_merge(
        spark,
        df,
        sample_silver_table,
        DFWriterUtil.get_merge_key(["id", "sub_id"]),
        "new.modified_at >= old.modified_at",
        None,
    )

    actual = (
        spark.read.table(sample_silver_table)
        .orderBy("id", F.col("sub_id").asc_nulls_first())
        .collect()
    )
    expected = [
        Row(id=1, sub_id="A", modified_at="2024-01-01 10:10:00", val="value1"),
        Row(id=2, sub_id="B", modified_at="2024-01-01 20:20:00", val="value2"),
        Row(id=100, sub_id=None, modified_at="2024-01-01 00:10:00", val="value00"),
    ]

    assert actual == expected


def test_flatten_json(spark):
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
    array_cols_to_explode = ["locations"]
    exclude_cols_from_flattening = ["_metadata"]

    df = spark.createDataFrame(data)

    actual_flatten_df = DFUtil.recursive_flatten(
        df, array_cols_to_explode, exclude_cols_from_flattening
    )
    actual = actual_flatten_df.collect()
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


@pytest.mark.parametrize(
    ("primary_keys,expected"),
    [
        (["id"], "old.id <=> new.id"),
        (["id", "date"], "old.id <=> new.id AND old.date <=> new.date"),
        ([], ""),  # No primary keys should return empty string
    ],
)
def test_get_merge_key(primary_keys, expected):
    assert DFWriterUtil.get_merge_key(primary_keys) == expected


@pytest.mark.parametrize(
    ("dedup_keys,expected"),
    [
        (["updated_at"], "old.updated_at <= new.updated_at"),
        (
            ["updated_at", "version"],
            "old.updated_at <= new.updated_at AND old.version <= new.version",
        ),
        ([], None),  # No dedup keys should return None
    ],
)
def test_get_merge_update_condition(dedup_keys, expected):
    assert DFWriterUtil.get_merge_update_condition(dedup_keys) == expected


def test_recursive_flatten_with_two_array_of_structs_should_yield_all_rows(spark):
    data = [
        Row(
            id=1,
            balanceUpdateArray=[
                Row(balanceClassId=826, amount=1.1),
                Row(balanceClassId=20007, amount=2.2),
                Row(balanceClassId=999, amount=3.3),
            ],
            usageQuantityList=[
                Row(msgAmount=10.0, ratingAmount=100.0),
                Row(msgAmount=20.0, ratingAmount=200.0),
            ],
            _metadata=Row(file_name="unit.json"),
        )
    ]

    df = spark.createDataFrame(data)

    flattened = DFUtil.recursive_flatten(
        df,
        array_cols_to_explode=["balanceUpdateArray", "usageQuantityList"],
        cols_to_exclude_from_flattening=["_metadata"],
    )

    assert flattened.count() == 6

    seen = {
        (
            r["balanceUpdateArray__balanceClassId"],
            r["usageQuantityList__msgAmount"],
        )
        for r in flattened.collect()
    }
    expected = {
        (826, 10.0),
        (826, 20.0),
        (20007, 10.0),
        (20007, 20.0),
        (999, 10.0),
        (999, 20.0),
    }
    assert seen == expected


@pytest.mark.parametrize(
    "src,expected",
    [
        ("Transaction Date", "Transaction_Date"),
        (" Store Name ", "Store_Name"),
        ("A,B;C{D}(E)=F", "A_B_C_D_E_F"),
        ("a\tb\nc", "a_b_c"),
        ("___Already__ok__", "___Already__ok__"),
        ("10abc", "10abc"),
        ("!!!", "col"),
        ("", "col"),
        (None, "col"),
        ("VAT Amount", "VAT_Amount"),
        ("a__b", "a__b"),
        ("A-B", "A_B"),
        ("already_ok", "already_ok"),
    ],
)
def test_sanitize_column_name_parametrized(src, expected):
    assert DFTransformUtil.sanitize_column_name(src) == expected


def test_sanitize_column_names_produces_unique_and_expected_names(spark):
    cols = ["A B", "a_b", "A-B", "10X", "!!!", "VAT Amount", "a__b"]
    df = spark.createDataFrame([("X1", "X2", "X3", "X4", "X5", "X6", "X7")], cols)

    sanitized = DFTransformUtil.sanitize_column_names(df)

    expected_cols = ["A_B", "a_b_1", "A_B_2", "10X", "col", "VAT_Amount", "a__b"]
    expected_df = spark.createDataFrame(
        [("X1", "X2", "X3", "X4", "X5", "X6", "X7")], expected_cols
    )

    assert sanitized.columns == expected_cols
    assertDataFrameEqual(
        sanitized.select(*expected_cols), expected_df.select(*expected_cols)
    )


def test_sanitize_column_names_idempotent_when_no_changes_needed(spark):
    df = spark.createDataFrame([(1, "v")], ["id", "value"])
    sanitized = DFTransformUtil.sanitize_column_names(df)
    assert sanitized is df
    assert sanitized.columns == ["id", "value"]


def test_recursive_flatten_when_array_cols_to_explode_is_none_should_not_explode_arrays(
    spark,
):
    data = [
        Row(
            total_count=2,
            locations=[
                Row(id=1, name="test1", nested=Row(level_two="value1")),
                Row(id=2, name="test2", nested=Row(level_two="value2")),
            ],
            _metadata=Row(file_name="unit.json"),
        )
    ]
    df = spark.createDataFrame(data)

    flattened = DFUtil.recursive_flatten(
        df,
        array_cols_to_explode=None,
        cols_to_exclude_from_flattening=["_metadata"],
    )

    actual = flattened.collect()
    expected = [
        Row(
            total_count=2,
            locations=[
                Row(id=1, name="test1", nested=Row(level_two="value1")),
                Row(id=2, name="test2", nested=Row(level_two="value2")),
            ],
            _metadata=Row(file_name="unit.json"),
        )
    ]

    assert actual == expected


def test_recursive_flatten_adds_index_when_configured(spark):
    data = [
        Row(
            id=1,
            appliedBundleArray=[
                Row(bundleId=10, nested=Row(val="a")),
                Row(bundleId=20, nested=Row(val="b")),
            ],
        )
    ]
    df = spark.createDataFrame(data)

    flattened = DFUtil.recursive_flatten(
        df,
        array_cols_to_explode=["appliedBundleArray"],
        cols_to_exclude_from_flattening=None,
        explode_with_index_col=True,
    )

    actual = flattened.collect()
    expected = [
        Row(
            id=1,
            appliedBundleArray__index=0,
            appliedBundleArray__bundleId=10,
            appliedBundleArray__nested__val="a",
        ),
        Row(
            id=1,
            appliedBundleArray__index=1,
            appliedBundleArray__bundleId=20,
            appliedBundleArray__nested__val="b",
        ),
    ]

    assert actual == expected


def test_recursive_flatten_defaults_without_index_flag(spark):
    data = [
        Row(
            id=1,
            appliedBundleArray=[
                Row(bundleId=10, nested=Row(val="a")),
            ],
        )
    ]
    df = spark.createDataFrame(data)

    flattened = DFUtil.recursive_flatten(
        df,
        array_cols_to_explode=["appliedBundleArray"],
        cols_to_exclude_from_flattening=None,
    )

    assert "appliedBundleArray__index" not in flattened.columns
