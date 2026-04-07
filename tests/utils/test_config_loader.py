import pytest
import yaml

from dp_core.transformer.models.silver_layer_config import SilverSchemaConfig
from dp_core.utils.config_loader import (
    load_configs,
    load_silver_schema_config,
    resolve_silver_schema_config_path,
)
from dp_core.utils.yaml_utils import (
    get_nearest_line_for_yaml_path,
    load_yaml_with_line_numbers,
)


def test_load_config_should_throw_error_if_path_is_empty():
    with pytest.raises(ValueError) as exc_info:
        load_configs(config_file_name="dev", config_path_str="")
    assert "No configuration path provided" in str(exc_info.value)


def test_load_config_with_custom_path_success(tmp_path):
    envs_dir = tmp_path / "envs"
    envs_dir.mkdir()
    plain_config = envs_dir / "dev.yml"
    plain_config.write_text("database:\n  host: localhost\n  port: 5432")

    configs = load_configs("dev", str(tmp_path))
    assert configs["database"]["host"] == "localhost"
    assert configs["database"]["port"] == 5432


def test_load_config_with_custom_path_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError) as exc_info:
        load_configs("missing", str(tmp_path))
    assert "Configuration file not found at" in str(exc_info.value)


def test_load_real_config_template_with_missing_template(tmp_path):
    envs_dir = tmp_path / "envs"
    envs_dir.mkdir()
    profiles_content = {
        "datahub": {"target": "dev", "outputs": {"dev": {"developer_prefix": "abc"}}},
    }
    (envs_dir / "profiles.yml").write_text(yaml.safe_dump(profiles_content))

    with pytest.raises(FileNotFoundError) as exc_info:
        load_configs("config", str(tmp_path), profile="dev")
    assert "Configuration template not found" in str(exc_info.value)


def test_load_config_with_env_file_for_local_profile(tmp_path):
    envs_dir = tmp_path / "envs"
    envs_dir.mkdir()

    env_file = envs_dir / ".env"
    env_file.write_text("""
        ENV=dev
        DEVELOPER_PREFIX=abc
        CATALOG=eun_dev_906_dh_data_db
        """)

    profiles_content = {
        "datahub": {
            "target": "local",
            "outputs": {
                "local": {
                    "type": "databricks",
                    "env": "dev",
                    "developer_prefix": "",
                    "catalog": "",
                },
            },
        }
    }
    (envs_dir / "profiles.yml").write_text(yaml.safe_dump(profiles_content))

    jinja_content = """
    {%- set prefix = developer_prefix ~ "_" if developer_prefix else "" -%}
    result:
      checkpoint_path: /Volumes/{{ catalog }}/checkpoints/{{ prefix }}finance
    """
    (envs_dir / "config.yml.j2").write_text(jinja_content)

    configs = load_configs("config", str(tmp_path), profile="local")
    assert configs["result"]["checkpoint_path"].endswith("abc_finance")
    assert "/Volumes/eun_dev_906_dh_data_db" in configs["result"]["checkpoint_path"]


def test_load_config_with_dev_profile_should_clear_prefix_and_render_successfully(
    tmp_path,
):
    envs_dir = tmp_path / "envs"
    envs_dir.mkdir()

    profiles_content = {
        "datahub": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "env": "dev",
                    "catalog": "dev",
                },
            },
        }
    }
    (envs_dir / "profiles.yml").write_text(yaml.safe_dump(profiles_content))

    jinja_content = """
    {%- set prefix = developer_prefix ~ "_" if developer_prefix else "" -%}
    output:
      checkpoint_path: /Volumes/{{ catalog }}/checkpoints/{{ prefix }}finance
    """
    (envs_dir / "config.yml.j2").write_text(jinja_content)
    configs = load_configs("config", str(tmp_path), profile="dev")

    checkpoint_path = configs["output"]["checkpoint_path"]
    assert "/Volumes/dev/checkpoints/finance" in checkpoint_path
    assert not checkpoint_path.endswith("should_ignore_finance")
    assert not checkpoint_path.split("/")[-1].startswith("should_ignore")


def test_load_silver_schema_config_loads_from_nested_schema_folder(tmp_path):
    configs_dir = tmp_path / "configs"
    schema_dir = configs_dir / "schemas" / "finance_silver"
    schema_dir.mkdir(parents=True)

    schema_payload = {
        "version": 1.0,
        "table": {
            "name": "my_table",
            "description": "some description",
            "tags": {"owner__tech": "Datahub", "data__domain": "finance"},
            "columns": [{"name": "id", "type": "string"}],
        },
    }
    (schema_dir / "my_schema.yml").write_text(
        yaml.safe_dump(schema_payload), encoding="utf-8"
    )
    expected = SilverSchemaConfig(**schema_payload)
    actual = load_silver_schema_config(
        config_path_str=str(configs_dir), schema_config_file_name="my_schema"
    )

    assert isinstance(actual, SilverSchemaConfig)
    assert actual == expected


def test_resolve_silver_schema_config_path_returns_correct_path(tmp_path):
    configs_dir = tmp_path / "configs"
    schema_dir = configs_dir / "schemas" / "finance_silver"
    schema_dir.mkdir(parents=True)

    schema_payload = {
        "version": 1.0,
        "table": {
            "name": "my_table",
            "description": "some description",
            "tags": {"owner__tech": "Datahub", "data__domain": "finance"},
            "columns": [{"name": "id", "type": "string"}],
        },
    }
    (schema_dir / "my_schema.yml").write_text(
        yaml.safe_dump(schema_payload), encoding="utf-8"
    )

    actual_path = resolve_silver_schema_config_path(
        config_path_str=str(configs_dir), schema_config_file_name="my_schema"
    )

    assert actual_path == schema_dir / "my_schema.yml"


def test_load_silver_schema_config_raises_when_not_found(tmp_path):
    configs_dir = tmp_path / "configs"
    schema_dir = configs_dir / "schemas" / "finance_silver"
    schema_dir.mkdir(parents=True)
    schema_config_file_name = "missing"

    with pytest.raises(FileNotFoundError) as exc:
        load_silver_schema_config(
            config_path_str=str(configs_dir),
            schema_config_file_name=schema_config_file_name,
        )

    assert f"Schema config '{schema_config_file_name}' not found under" in str(
        exc.value
    )
    assert "Available schema files:" in str(exc.value)


def test_load_silver_schema_config_raises_when_ambiguous(tmp_path):
    configs_dir = tmp_path / "configs"
    schema_dir_1 = configs_dir / "schemas" / "finance_silver"
    schema_dir_2 = configs_dir / "schemas" / "revenue_silver"
    schema_dir_1.mkdir(parents=True)
    schema_dir_2.mkdir(parents=True)

    schema_payload = {
        "table": {
            "name": "my_table",
            "description": "some description",
            "tags": {"owner__tech": "Datahub", "data__domain": "finance"},
            "columns": [{"name": "id", "type": "string"}],
        }
    }

    (schema_dir_1 / "dup.yml").write_text(
        yaml.safe_dump(schema_payload), encoding="utf-8"
    )
    (schema_dir_2 / "dup.yml").write_text(
        yaml.safe_dump(schema_payload), encoding="utf-8"
    )

    with pytest.raises(ValueError) as exc:
        load_silver_schema_config(
            config_path_str=str(configs_dir), schema_config_file_name="dup"
        )

    assert "Schema config 'dup' is ambiguous." in str(exc.value)
    assert "Matches:" in str(exc.value)


def test_load_yaml_with_line_numbers_returns_content_and_line_map(tmp_path):
    schema_file = tmp_path / "schema.yml"
    schema_file.write_text(
        "table:\n  name: sample_table\n  columns:\n    - name: id\n      type: string\n"
    )

    content, line_map = load_yaml_with_line_numbers(schema_file)

    assert content["table"]["name"] == "sample_table"
    assert line_map[("table",)] == 1
    assert line_map[("table", "name")] == 2
    assert line_map[("table", "columns", 0, "type")] == 5


def test_get_nearest_line_for_yaml_path_falls_back_to_parent():
    line_map = {
        ("table",): 1,
        ("table", "columns"): 10,
        ("table", "columns", 0): 11,
    }

    line = get_nearest_line_for_yaml_path(
        line_map, ("table", "columns", 0, "transform")
    )

    assert line == 11
