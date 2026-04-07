from unittest.mock import patch, MagicMock

import pytest

from dp_core.utils.constants import ExporterConstants
from dp_core.utils.databricks_utils import (
    get_dbutils,
    get_dbutils_credentials,
    write_json,
    rename_spark_part_files,
)


@patch("dp_core.utils.databricks_utils.WorkspaceClient")
def test_get_dbutils(mock_ws_client):
    fake_dbutils = MagicMock()
    mock_ws_client.return_value.dbutils = fake_dbutils

    result = get_dbutils()

    assert result == fake_dbutils
    mock_ws_client.assert_called_once()


@patch("dp_core.utils.databricks_utils.get_dbutils")
def test_get_dbutils_credentials_success(mock_get_dbutils):
    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.side_effect = ["client-id-value", "client-secret-value"]
    mock_get_dbutils.return_value = mock_dbutils

    scope = "my-scope"
    cid_key = "client-id"
    secret_key = "client-secret"

    cid = get_dbutils_credentials(scope, cid_key)
    secret = get_dbutils_credentials(scope, secret_key)
    assert cid == "client-id-value"
    assert secret == "client-secret-value"

    mock_dbutils.secrets.get.assert_any_call(scope=scope, key=cid_key)
    mock_dbutils.secrets.get.assert_any_call(scope=scope, key=secret_key)


@patch("dp_core.utils.databricks_utils.get_dbutils")
def test_get_dbutils_credentials_no_dbutils(mock_get_dbutils):
    mock_get_dbutils.return_value = None

    with pytest.raises(RuntimeError) as exc:
        get_dbutils_credentials("scope", "id")

    assert "requires Databricks environment" in str(exc.value)


def test_write_json_creates_directory_and_writes_file(tmp_path):
    nested_dir = tmp_path / "some" / "nested" / "dir"
    file_path = nested_dir / "out.json"
    content = '{"hello": "world"}'

    assert not nested_dir.exists()

    write_json(str(file_path), content)

    assert nested_dir.exists() and nested_dir.is_dir()
    assert file_path.exists() and file_path.is_file()
    with open(file_path, "r", encoding="utf-8") as f:
        written = f.read()
    assert written == content


def test_write_json_overwrites_existing_file(tmp_path):
    d = tmp_path / "dir2"
    d.mkdir()
    file_path = d / "existing.json"

    with open(file_path, "w", encoding="utf-8") as f:
        f.write('{"initial": 1}')

    assert file_path.exists()

    new_content = '{"initial": 2, "added": true}'
    write_json(str(file_path), new_content)

    with open(file_path, "r", encoding="utf-8") as f:
        written = f.read()
    assert written == new_content


@patch("dp_core.utils.databricks_utils.get_dbutils")
@pytest.mark.parametrize(
    ("target_extension", "source_extension"),
    [
        (
            config["target_file_extension"],
            config["spark_write_file_extension"],
        )
        for config in ExporterConstants.SUPPORTED_FORMATS.values()
    ],
)
def test_rename_spark_part_files_renames_and_deletes(
    mock_get_dbutils, target_extension, source_extension
):
    mock_dbutils = MagicMock()

    part_file_info = MagicMock()
    part_file_info.name = f"part-00000-tid-12345-abc123-c000{source_extension}"
    part_file_info.path = (
        f"dbfs:/tmp/path/part-00000-tid-12345-abc123-c000{source_extension}"
    )

    non_part_file_info = MagicMock()
    non_part_file_info.name = "_SUCCESS"
    non_part_file_info.path = "dbfs:/tmp/path/_SUCCESS"

    mock_dbutils.fs.ls.return_value = [part_file_info, non_part_file_info]
    mock_get_dbutils.return_value = mock_dbutils

    rename_spark_part_files(
        "dbfs:/tmp/path",
        "new_base_name",
        target_extension,
        source_extension=source_extension,
    )

    expected_new_path = f"dbfs:/tmp/path/new_base_name{target_extension}"
    mock_dbutils.fs.mv.assert_called_once_with(part_file_info.path, expected_new_path)

    mock_dbutils.fs.rm.assert_called_once_with(non_part_file_info.path)
