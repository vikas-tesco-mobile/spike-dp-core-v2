import argparse
from pathlib import Path
from typing import List, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, AliasChoices, AliasPath
from pyspark.sql import SparkSession

from dp_core.utils.config_loader import load_configs
from dp_core.utils.logger import get_logger
from dp_core.utils.schema_name_generator import SchemaNameGenerator
from dp_core.utils.spark_session import get_spark_session

logger = get_logger(__name__)


class ColumnMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str = Field(validation_alias=AliasChoices("name", "target_name"))
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = Field(
        default=None,
        validation_alias=AliasChoices(
            "tags",
            AliasPath("config", "meta"),
        ),
    )


class TableMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    fqdn: Optional[str] = None
    description: Optional[str] = None
    tags: Dict[str, str] = Field(
        default_factory=dict,
        validation_alias=AliasChoices(
            "tags",
            AliasPath("config", "meta"),
        ),
    )
    columns: List[ColumnMetadata] = Field(default_factory=list)


class MetadataParser:
    @staticmethod
    def parse_dbt_gold_models_yaml(
        yml_config: Dict, qualified_schema_name: str
    ) -> List[TableMetadata]:
        table_metadata_list: List[TableMetadata] = []
        for model in yml_config.get("models", []):
            table_metadata = TableMetadata(**model)
            fqdn = f"{qualified_schema_name}.{table_metadata.name}"
            table_metadata = table_metadata.model_copy(update={"fqdn": fqdn})
            table_metadata_list.append(table_metadata)
        return table_metadata_list

    @staticmethod
    def parse_silver_tables_yaml(
        yml_config: Dict, qualified_schema_name: str
    ) -> Optional[TableMetadata]:
        table: Dict = yml_config.get("table", {})
        table_metadata = TableMetadata(**table)
        fqdn = f"{qualified_schema_name}.{table_metadata.name}"
        return table_metadata.model_copy(update={"fqdn": fqdn})


class TableMetadataUpdater:
    def __init__(
        self,
        spark: SparkSession,
        workspace_root_path: str,
        layer: str,
        catalog: str,
        schema_prefix: Optional[str],
    ):
        self.spark = spark
        self.workspace_root_path = workspace_root_path
        self.layer = layer
        self.catalog = catalog
        self.schema_prefix = schema_prefix

    def update_table_metadata(self):
        table_metadata = self._read_yaml()
        self._apply_description_and_tags(table_metadata)

    def _read_yaml(self) -> List[TableMetadata]:
        root_path = Path(self.workspace_root_path)
        if self.layer == "silver":
            metadata_path = root_path / "configs" / "schemas"
            base_path = root_path / "configs"
            yml_paths = list(metadata_path.rglob("*.yml"))
        elif self.layer == "gold":
            metadata_path = root_path / "dbt_project" / "models"
            base_path = root_path / "dbt_project"
            yml_paths = [
                p for p in metadata_path.rglob("*.yml") if p.parent != metadata_path
            ]
        else:
            logger.error(f"Unsupported layer: {self.layer}")
            raise ValueError(f"Unsupported layer: {self.layer}")

        table_metadata: List[TableMetadata] = []
        for yml_path in yml_paths:
            try:
                schema_name = SchemaNameGenerator.generate(
                    yml_file_path=yml_path,
                    prefix=self.schema_prefix,
                    root_folder="models" if self.layer == "gold" else None,
                )
                qualified_schema_name = f"{self.catalog}.{schema_name}"
                subfolder_relative_to_base_path = str(
                    yml_path.parent.relative_to(base_path)
                )
                yml_config = load_configs(
                    config_file_name=yml_path.stem,
                    config_path_str=str(base_path),
                    subfolder=subfolder_relative_to_base_path,
                )
                if self.layer == "silver":
                    parsed = MetadataParser.parse_silver_tables_yaml(
                        yml_config, qualified_schema_name
                    )
                    if parsed:
                        table_metadata.append(parsed)
                else:
                    table_metadata.extend(
                        MetadataParser.parse_dbt_gold_models_yaml(
                            yml_config, qualified_schema_name
                        )
                    )
            except Exception:
                logger.exception("Failed parsing yaml: %s", yml_path)
        return table_metadata

    def _apply_description_and_tags(self, table_metadata: List[TableMetadata]):
        for table in table_metadata:
            try:
                logger.info(
                    "Applying available metadata for table=%s fqdn=%s",
                    table.name,
                    table.fqdn,
                )
                if table.description:
                    self.spark.sql(
                        f"COMMENT ON TABLE {table.fqdn} IS '{table.description}'"
                    )

                if table.tags:
                    tags_sql = ", ".join(
                        [f"'{key}' = '{value}'" for key, value in table.tags.items()]
                    )
                    self.spark.sql(f"ALTER TABLE {table.fqdn} SET TAGS ({tags_sql})")

                for col in table.columns:
                    quoted_column_name = self._quote_identifier(col.name)
                    if col.description:
                        self.spark.sql(
                            f"COMMENT ON COLUMN {table.fqdn}.{quoted_column_name} IS '{col.description}'"
                        )
                    if col.tags:
                        tags_sql = ", ".join(
                            [f"'{key}' = '{value}'" for key, value in col.tags.items()]
                        )
                        self.spark.sql(
                            f"ALTER TABLE {table.fqdn} ALTER COLUMN {quoted_column_name} SET TAGS ({tags_sql})"
                        )
            except Exception as e:
                logger.error(
                    "Failed updating metadata for table=%s fqdn=%s err=%s",
                    table.name,
                    table.fqdn,
                    e,
                )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return f"`{identifier.replace('`', '``')}`"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--workspace_root", required=True)
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--layer", required=True)
    input_params = vars(parser.parse_args())

    spark = get_spark_session()

    config_path_str = f"{input_params['workspace_root']}/configs"
    configs = load_configs(
        config_file_name="config",
        config_path_str=config_path_str,
        profile=input_params["profile"],
    )
    job_configs = configs[input_params["job_name"]]
    catalog = job_configs["catalog"]
    schema_prefix = job_configs["schema_prefix"]
    table_metadata_updater = TableMetadataUpdater(
        spark,
        input_params["workspace_root"],
        input_params["layer"],
        catalog,
        schema_prefix,
    )
    table_metadata_updater.update_table_metadata()
