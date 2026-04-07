from unittest.mock import MagicMock

import pytest
import yaml

from dp_core.housekeeping.table_metadata_updater import (
    TableMetadataUpdater,
    TableMetadata,
    ColumnMetadata,
)


@pytest.fixture
def mock_spark():
    mock_spark = MagicMock()
    mock_spark.sql = MagicMock()
    return mock_spark


def test_update_table_metadata_executes_expected_sql(mock_spark):
    table_name = "tesco_products_cleaned"
    catalog = "db"
    schema_name = "schema"
    fqdn = f"{catalog}.{schema_name}.{table_name}"
    workspace_root = "some-path"
    table_configs = {
        table_name: {
            "table": {
                "description": "Registry of all Tesco Mobile products",
                "tags": {"owner__tech": "Datahub", "data__domain": "finance"},
                "columns": [
                    {
                        "target_name": "product_id",
                        "description": "A unique key for tesco mobile product",
                        "tags": {"some__tag": "some_value"},
                    },
                    {"target_name": "item_number", "description": "Tesco item number"},
                ],
            }
        }
    }
    table_def = table_configs[table_name]["table"]
    mocked_table_metadata = [
        TableMetadata(
            name=table_name,
            fqdn=fqdn,
            description=table_def.get("description"),
            tags=table_def.get("tags", {}),
            columns=[
                ColumnMetadata(
                    name=c["target_name"],
                    description=c.get("description"),
                    tags=c.get("tags"),
                )
                for c in table_def.get("columns", [])
            ],
        )
    ]

    updater = TableMetadataUpdater(
        spark=mock_spark,
        workspace_root_path=workspace_root,
        layer="silver",
        catalog=catalog,
        schema_prefix="",
    )
    updater._read_yaml = MagicMock(return_value=mocked_table_metadata)
    updater.update_table_metadata()

    expected_sql_statements = [
        f"COMMENT ON TABLE {fqdn} IS 'Registry of all Tesco Mobile products'",
        f"COMMENT ON COLUMN {fqdn}.`product_id` IS 'A unique key for tesco mobile product'",
        f"COMMENT ON COLUMN {fqdn}.`item_number` IS 'Tesco item number'",
        f"ALTER TABLE {fqdn} SET TAGS ('owner__tech' = 'Datahub', 'data__domain' = 'finance')",
        f"ALTER TABLE {fqdn} ALTER COLUMN `product_id` SET TAGS ('some__tag' = 'some_value')",
    ]

    actual_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
    for stmt in expected_sql_statements:
        assert stmt in actual_calls, (
            f"Expected SQL '{stmt}' not executed. Actual: {actual_calls}"
        )
    assert len(actual_calls) == len(expected_sql_statements)


def test_read_yaml_silver_and_return_correct_table_metadata(mock_spark, tmp_path):
    table1 = {
        "table": {
            "name": "table1",
            "description": "Registry of all Tesco Mobile products",
            "tags": {"owner__tech": "Datahub", "data__domain": "finance"},
            "columns": [
                {
                    "target_name": "product_id",
                    "description": "A unique key for tesco mobile product",
                    "tags": {"some__tag": "some_value"},
                },
                {"target_name": "item_number", "description": "Tesco item number"},
            ],
        }
    }
    table2 = {
        "table": {
            "name": "table2",
            "description": "Registry of all Tesco Mobile locations",
            "tags": {"owner__tech": "Datahub", "data__domain": "finance"},
            "columns": [
                {
                    "target_name": "location_id",
                },
            ],
        }
    }
    (tmp_path / "configs" / "schemas" / "sample1_silver").mkdir(parents=True)
    (tmp_path / "configs" / "schemas" / "sample2_silver").mkdir(parents=True)
    (tmp_path / "configs" / "schemas" / "sample1_silver" / "table1.yml").write_text(
        yaml.safe_dump(table1), encoding="utf-8"
    )
    (tmp_path / "configs" / "schemas" / "sample2_silver" / "table2.yml").write_text(
        yaml.safe_dump(table2), encoding="utf-8"
    )
    catalog = "db"
    schema_prefix = ""

    updater = TableMetadataUpdater(
        spark=mock_spark,
        workspace_root_path=str(tmp_path),
        layer="silver",
        catalog=catalog,
        schema_prefix=schema_prefix,
    )

    actual_table_metadata = updater._read_yaml()
    expected_table_metadata = [
        TableMetadata(
            name="table1",
            fqdn="db.sample1_silver.table1",
            description="Registry of all Tesco Mobile products",
            tags={"owner__tech": "Datahub", "data__domain": "finance"},
            columns=[
                ColumnMetadata(
                    name="product_id",
                    description="A unique key for tesco mobile product",
                    tags={"some__tag": "some_value"},
                ),
                ColumnMetadata(
                    name="item_number",
                    description="Tesco item number",
                    tags=None,
                ),
            ],
        ),
        TableMetadata(
            name="table2",
            fqdn="db.sample2_silver.table2",
            description="Registry of all Tesco Mobile locations",
            tags={"owner__tech": "Datahub", "data__domain": "finance"},
            columns=[
                ColumnMetadata(
                    name="location_id",
                    description=None,
                    tags=None,
                ),
            ],
        ),
    ]

    assert (
        sorted(actual_table_metadata, key=lambda x: x.name) == expected_table_metadata
    )


def test_update_table_metadata_quotes_column_identifiers(mock_spark):
    table_metadata = [
        TableMetadata(
            name="blugem_ccs_usage_reconciliation",
            fqdn="db.revenue_assurance_gold.blugem_ccs_usage_reconciliation",
            columns=[
                ColumnMetadata(
                    name="segment",
                    description="Subscriber segment from the primary event attributes",
                    tags={"data__domain": "finance"},
                )
            ],
        )
    ]

    updater = TableMetadataUpdater(
        spark=mock_spark,
        workspace_root_path="some-path",
        layer="gold",
        catalog="db",
        schema_prefix="",
    )
    updater._read_yaml = MagicMock(return_value=table_metadata)

    updater.update_table_metadata()

    actual_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
    assert (
        "COMMENT ON COLUMN "
        "db.revenue_assurance_gold.blugem_ccs_usage_reconciliation.`segment` "
        "IS 'Subscriber segment from the primary event attributes'"
    ) in actual_calls
    assert (
        "ALTER TABLE "
        "db.revenue_assurance_gold.blugem_ccs_usage_reconciliation "
        "ALTER COLUMN `segment` SET TAGS ('data__domain' = 'finance')"
    ) in actual_calls


def test_read_yaml_dbt_gold_and_return_correct_table_metadata(mock_spark, tmp_path):
    table1 = {
        "models": [
            {
                "name": "table1",
                "description": "Registry of all Tesco Mobile products",
                "config": {
                    "meta": {"owner__tech": "Datahub", "data__domain": "finance"}
                },
                "columns": [
                    {
                        "name": "product_id",
                        "description": "A unique key for tesco mobile product",
                        "config": {"meta": {"some__tag": "some_value"}},
                    },
                    {"name": "item_number", "description": "Tesco item number"},
                ],
            },
        ]
    }
    table2 = {
        "models": [
            {
                "name": "table2",
                "description": "Registry of all Tesco Mobile locations",
                "config": {
                    "meta": {"owner__tech": "Datahub", "data__domain": "finance"}
                },
                "columns": [
                    {
                        "name": "location_id",
                    },
                ],
            },
        ]
    }
    (tmp_path / "dbt_project" / "models" / "finance_gold").mkdir(parents=True)
    (tmp_path / "dbt_project" / "models" / "finance_gold" / "table1.yml").write_text(
        yaml.safe_dump(table1), encoding="utf-8"
    )
    (tmp_path / "dbt_project" / "models" / "finance_gold" / "table2.yml").write_text(
        yaml.safe_dump(table2), encoding="utf-8"
    )
    (tmp_path / "dbt_project" / "models" / "source.yml").write_text(
        yaml.safe_dump("{source: {}}"), encoding="utf-8"
    )
    catalog = "db"
    schema_prefix = ""

    updater = TableMetadataUpdater(
        spark=mock_spark,
        workspace_root_path=str(tmp_path),
        layer="gold",
        catalog=catalog,
        schema_prefix=schema_prefix,
    )

    actual_table_metadata = updater._read_yaml()
    expected_table_metadata = [
        TableMetadata(
            name="table1",
            fqdn="db.finance_gold.table1",
            description="Registry of all Tesco Mobile products",
            tags={"owner__tech": "Datahub", "data__domain": "finance"},
            columns=[
                ColumnMetadata(
                    name="product_id",
                    description="A unique key for tesco mobile product",
                    tags={"some__tag": "some_value"},
                ),
                ColumnMetadata(
                    name="item_number",
                    description="Tesco item number",
                    tags=None,
                ),
            ],
        ),
        TableMetadata(
            name="table2",
            fqdn="db.finance_gold.table2",
            description="Registry of all Tesco Mobile locations",
            tags={"owner__tech": "Datahub", "data__domain": "finance"},
            columns=[
                ColumnMetadata(
                    name="location_id",
                    description=None,
                    tags=None,
                ),
            ],
        ),
    ]

    assert (
        sorted(actual_table_metadata, key=lambda x: x.name) == expected_table_metadata
    )


def test_read_yaml_dbt_gold_nested_and_return_correct_table_metadata(
    mock_spark, tmp_path
):
    nested_table = {
        "models": [
            {
                "name": "blugem_ccs_usage_reconciliation",
                "description": "Nested gold model metadata",
                "config": {
                    "meta": {
                        "owner__tech": "Revenue Assurance Team",
                        "data__domain": "finance",
                    }
                },
                "columns": [
                    {
                        "name": "event_id",
                        "description": "Event identifier",
                    },
                ],
            },
        ]
    }
    nested_dir = (
        tmp_path
        / "dbt_project"
        / "models"
        / "revenue_assurance_gold"
        / "blugem_ccs_usage_reconciliation"
    )
    nested_dir.mkdir(parents=True)
    (nested_dir / "blugem_ccs_usage_reconciliation.yml").write_text(
        yaml.safe_dump(nested_table), encoding="utf-8"
    )
    catalog = "db"
    schema_prefix = ""

    updater = TableMetadataUpdater(
        spark=mock_spark,
        workspace_root_path=str(tmp_path),
        layer="gold",
        catalog=catalog,
        schema_prefix=schema_prefix,
    )

    actual_table_metadata = updater._read_yaml()
    expected_table_metadata = [
        TableMetadata(
            name="blugem_ccs_usage_reconciliation",
            fqdn=("db.revenue_assurance_gold.blugem_ccs_usage_reconciliation"),
            description="Nested gold model metadata",
            tags={
                "owner__tech": "Revenue Assurance Team",
                "data__domain": "finance",
            },
            columns=[
                ColumnMetadata(
                    name="event_id",
                    description="Event identifier",
                    tags=None,
                ),
            ],
        ),
    ]

    assert actual_table_metadata == expected_table_metadata
