import re
from pathlib import Path

from databricks.sdk import WorkspaceClient

from dp_core.utils.logger import get_logger

logger = get_logger(__name__)


def get_dbutils():
    workspace_client = WorkspaceClient()
    return workspace_client.dbutils


def get_dbutils_credentials(scope, secret_key):
    dbutils = get_dbutils()
    if not dbutils:
        raise RuntimeError(
            "This authentication requires Databricks environment with dbutils.secrets enabled."
        )
    secret_value = dbutils.secrets.get(scope=scope, key=secret_key)
    return secret_value


def get_keyvault_credentials():
    pass


def write_json(path: str, json_text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as f:
        f.write(json_text)


def rename_spark_part_files(
    path: str,
    new_base_name: str,
    extension: str = ".csv",
    source_extension: str | None = None,
) -> None:
    """
    Renames Spark-generated part files in the given directory on Databricks.
    Will overwrite existing files if necessary and delete non-part files.
    Example:
        part-00000-tid-12345-abc123-c000.csv -> new_base_name.csv
    """
    dbutils = get_dbutils()
    if not dbutils:
        raise RuntimeError("This authentication requires Databricks environment")

    source_extension = source_extension or extension
    part_file_pattern = re.compile(r"part-(\d+).*" + re.escape(source_extension) + r"$")

    files = dbutils.fs.ls(path)
    for file_info in files:
        file_name = Path(file_info.name).name
        match = part_file_pattern.match(file_name)
        if match:
            new_file_name = f"{new_base_name}{extension}"
            new_file_path = f"{path.rstrip('/')}/{new_file_name}"
            logger.info(f"Renaming {file_name} -> {new_file_name}")
            dbutils.fs.mv(file_info.path, new_file_path)
        else:
            logger.info(f"Deleting non-part file: {file_name}")
            try:
                dbutils.fs.rm(file_info.path)
            except Exception as e:
                logger.error(f"Failed to delete {file_name}: {e}")
