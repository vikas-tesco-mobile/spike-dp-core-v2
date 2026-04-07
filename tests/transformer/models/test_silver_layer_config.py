import pytest

from dp_core.transformer.models.silver_layer_config import (
    ColumnFlattenerConfig,
    SilverColumnConfig,
    SilverTableConfig,
)
from dp_core.utils.constants import SuffixConstants


def test_get_all_column_names_includes_hashed_columns_when_enabled():
    cfg = SilverTableConfig(
        name="my_table",
        columns=[
            SilverColumnConfig(name="id", type="string"),
            SilverColumnConfig(name="email", type="string", create_hashed_col=True),
            SilverColumnConfig(name="phone", type="string", create_hashed_col=False),
        ],
    )

    assert cfg.get_all_column_names() == [
        "id",
        "email",
        f"email{SuffixConstants.HASHED_COLUMN_SUFFIX}",
        "phone",
    ]


def test_get_all_column_names_returns_only_base_columns_when_none_hashed():
    cfg = SilverTableConfig(
        name="my_table",
        columns=[
            SilverColumnConfig(name="id", type="string"),
            SilverColumnConfig(name="email", type="string"),
        ],
    )

    assert cfg.get_all_column_names() == ["id", "email"]


def test_get_all_column_names_returns_empty_when_no_columns():
    cfg = SilverTableConfig(name="my_table")
    assert cfg.get_all_column_names() == []


def test_get_columns_to_be_hashed_returns_only_columns_marked_for_hashing():
    cfg = SilverTableConfig(
        name="my_table",
        columns=[
            SilverColumnConfig(name="id", type="string"),
            SilverColumnConfig(name="email", type="string", create_hashed_col=True),
            SilverColumnConfig(name="phone", type="string", create_hashed_col=True),
        ],
    )

    assert cfg.get_columns_to_be_hashed() == ["email", "phone"]


def test_get_columns_to_be_hashed_returns_empty_when_none_marked():
    cfg = SilverTableConfig(
        name="my_table",
        columns=[
            SilverColumnConfig(name="id", type="string"),
            SilverColumnConfig(name="email", type="string", create_hashed_col=False),
        ],
    )

    assert cfg.get_columns_to_be_hashed() == []


def test_get_columns_to_explode_and_exclude_from_flattening_returns_values_from_flatten_config():
    cfg = SilverTableConfig(
        name="my_table",
        flatten_config=ColumnFlattenerConfig(
            cols_to_explode=["a", "b"],
            cols_to_exclude=["c"],
        ),
    )

    assert cfg.get_columns_to_explode() == ["a", "b"]
    assert cfg.get_columns_to_exclude_from_flattening() == ["c"]


def test_get_columns_to_explode_and_exclude_from_flattening_when_lists_are_none():
    cfg = SilverTableConfig(
        name="my_table",
        flatten_config=ColumnFlattenerConfig(
            cols_to_explode=None,
            cols_to_exclude=None,
        ),
    )

    assert cfg.get_columns_to_explode() is None
    assert cfg.get_columns_to_exclude_from_flattening() is None


def test_get_columns_to_explode_and_exclude_from_flattening_as_none_when_flatten_config_is_undefined():
    cfg = SilverTableConfig(name="my_table", flatten_config=None)
    assert cfg.get_columns_to_explode() is None
    assert cfg.get_columns_to_exclude_from_flattening() is None


def test_column_config_transform_is_optional():
    cfg = SilverColumnConfig(name="my_col", type="string")
    assert cfg.transform is None


def test_column_config_transform_is_supported():
    cfg = SilverColumnConfig(name="my_col", type="string", transform="upper(name)")
    assert cfg.transform == "upper(name)"


def test_column_config_transform_rejects_unsafe_sql():
    with pytest.raises(ValueError) as exc_info:
        SilverColumnConfig(
            name="my_col",
            type="string",
            transform="id; DROP TABLE some_table",
        )

    assert "Unsafe SQL expression configured for target column `my_col`." in str(
        exc_info.value
    )


def test_column_config_transform_rejects_invalid_sql_syntax():
    with pytest.raises(ValueError) as exc_info:
        SilverColumnConfig(
            name="my_col",
            type="string",
            transform="id +",
        )

    assert "Invalid SQL syntax configured for target column `my_col`." in str(
        exc_info.value
    )
