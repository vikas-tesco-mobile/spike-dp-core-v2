import pytest

from conftest import (
    project_root,
    get_all_silver_schema_files,
    get_all_dbt_gold_schema_files,
)
from dp_core.utils.config_loader import load_configs

REQUIRED_TAGS = [
    "owner__tech",
    "data__supplier",
    "data__domain",
    "pii__exists",
    "pii__classification",
]


@pytest.mark.parametrize(("schema_file", "subfolder"), get_all_silver_schema_files())
def test_all_silver_tables_have_description_and_required_tags(schema_file, subfolder):
    config_path = str(project_root() / "configs")
    schema_config = load_configs(
        config_file_name=schema_file, config_path_str=config_path, subfolder=subfolder
    )
    table_config = schema_config.get("table", {})

    description = table_config.get("description")
    assert description is not None and description.strip() != "", (
        f"Silver table '{schema_file}' must have a non-empty 'description'"
    )

    tags = table_config.get("tags", {})
    missing_tags = [t for t in REQUIRED_TAGS if t not in tags]
    assert not missing_tags, (
        f"Silver table '{schema_file}' is missing required tags: {missing_tags}"
    )

    for tag_key in REQUIRED_TAGS:
        tag_value = tags.get(tag_key)
        assert tag_value is not None and str(tag_value).strip() != "", (
            f"Tag '{tag_key}' for silver table '{schema_file}' must have a non-empty value"
        )


@pytest.mark.parametrize(("schema_file", "subfolder"), get_all_dbt_gold_schema_files())
def test_all_dbt_gold_models_have_description_and_required_tags(schema_file, subfolder):
    config_path = str(project_root() / "dbt_project")
    schema_config = load_configs(
        config_file_name=schema_file, config_path_str=config_path, subfolder=subfolder
    )
    models_config = schema_config.get("models", {})
    for model in models_config:
        description = model.get("description")
        assert description is not None and description.strip() != "", (
            f"Gold table '{schema_file}' must have a non-empty 'description'"
        )

        tags = model.get("config", {}).get("meta", {})
        missing_tags = [t for t in REQUIRED_TAGS if t not in tags]
        assert not missing_tags, (
            f"Gold table '{schema_file}' is missing required tags: {missing_tags}"
        )

        for tag_key in REQUIRED_TAGS:
            tag_value = tags.get(tag_key)
            assert tag_value is not None and str(tag_value).strip() != "", (
                f"Tag '{tag_key}' for gold table '{schema_file}' must have a non-empty value"
            )
