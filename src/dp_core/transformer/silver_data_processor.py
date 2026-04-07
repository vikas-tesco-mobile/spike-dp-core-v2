import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max, current_timestamp, struct, sha2, expr

from dp_core.transformer.models.silver_layer_config import (
    SilverColumnConfig,
    SilverTableConfig,
)
from dp_core.utils.config_loader import (
    load_configs,
    load_silver_schema_config,
    resolve_silver_schema_config_path,
)
from dp_core.utils.schema_name_generator import SchemaNameGenerator
from dp_core.utils.df_utils import DFReaderUtil, DFUtil, DFWriterUtil
from dp_core.utils.logger import get_logger
from dp_core.utils.spark_session import get_spark_session
from dp_core.utils.constants import LoadTypeConstants, SuffixConstants

logger = get_logger(__name__)


class SilverDataProcessor:
    def __init__(
        self,
        spark: SparkSession,
        job_configs: dict,
        silver_table_config: SilverTableConfig,
    ):
        self.spark = spark
        self.job_configs = job_configs
        self.cleaned_df: DataFrame | None = None
        self.silver_table_config: SilverTableConfig = silver_table_config
        self._read_mode = LoadTypeConstants.INCREMENTAL

    def read(self):
        bronze_table_name = self.job_configs.get("bronze_table")
        silver_table_name = self.job_configs.get("silver_table")

        bronze_df = DFReaderUtil.read_table(self.spark, table_name=bronze_table_name)

        start_date = self.job_configs.get("start_date")
        end_date = self.job_configs.get("end_date")
        if start_date and end_date:
            self._read_mode = LoadTypeConstants.DATE_RANGE
            logger.info(
                f"Reading bronze data between dates {start_date} and {end_date}"
            )
            self.cleaned_df = bronze_df.filter(
                (col("_ingested_at") >= start_date) & (col("_ingested_at") <= end_date)
            )
            return

        if not self.spark.catalog.tableExists(silver_table_name):
            self._read_mode = LoadTypeConstants.FULL_LOAD
            logger.info(
                f"Target silver table {silver_table_name} does not exist"
                f" Reading full load from bronze table {bronze_table_name}"
            )
            self.cleaned_df = bronze_df
            return

        silver_df = DFReaderUtil.read_table(self.spark, table_name=silver_table_name)

        if silver_df.isEmpty():
            self._read_mode = LoadTypeConstants.FULL_LOAD
            logger.info(
                f"Target silver table {silver_table_name} is empty,"
                f"Reading full load from bronze table {bronze_table_name}."
            )
            self.cleaned_df = bronze_df
            return

        max_bronze_ingested_at = None
        if "_metadata" in silver_df.columns:
            max_bronze_ingested_at = silver_df.agg(
                max(col("_metadata.bronze_ingested_at"))
            ).collect()[0][0]

        if max_bronze_ingested_at is None:
            raise ValueError(
                f"bronze_ingested_at timestamp not found in silver table {silver_table_name} metadata column;"
                f" cannot perform incremental load."
            )

        logger.info(
            f"Reading incremental data from bronze table {bronze_table_name} where _ingested_at > {max_bronze_ingested_at}"
        )
        self._read_mode = LoadTypeConstants.INCREMENTAL

        self.cleaned_df = bronze_df.filter(col("_ingested_at") > max_bronze_ingested_at)

    def flatten(self):
        cols_to_explode = self.silver_table_config.get_columns_to_explode()
        cols_to_exclude_from_flattening = (
            self.silver_table_config.get_columns_to_exclude_from_flattening()
        )
        flatten_config = self.silver_table_config.flatten_config
        explode_with_index_col = bool(
            flatten_config and flatten_config.explode_with_index_col
        )
        logger.info("Flattening columns")
        self.cleaned_df = DFUtil.recursive_flatten(
            self.cleaned_df,
            cols_to_explode,
            cols_to_exclude_from_flattening,
            explode_with_index_col,
        )

    def add_metadata_cols(self):
        logger.info("Transforming data with additional metadata columns")
        if "_metadata" in self.cleaned_df.columns:
            self.cleaned_df = self.cleaned_df.withColumn(
                "_metadata",
                col("_metadata")
                .withField("bronze_ingested_at", col("_ingested_at"))
                .withField("silver_processed_at", current_timestamp()),
            )
        else:
            self.cleaned_df = self.cleaned_df.withColumn(
                "_metadata",
                struct(
                    col("_ingested_at").alias("bronze_ingested_at"),
                    current_timestamp().alias("silver_processed_at"),
                ),
            )

    def apply_schema_mappings(self):
        logger.info(
            "Enforcing schema mappings and transformations as per configuration"
        )
        for column_config in self.silver_table_config.columns:
            self._enforce_column_schema(column_config)

    def _require_cleaned_df(self) -> DataFrame:
        if self.cleaned_df is None:
            raise ValueError(
                "cleaned_df is empty, ensure read() is called before this operation."
            )
        return self.cleaned_df

    def _enforce_column_schema(self, column_config: SilverColumnConfig) -> None:
        target_col_name = column_config.name
        self._validate_mapping_inputs(column_config)
        self._add_target_column(column_config)
        self._cast_target_column(target_col_name, column_config.type)
        self._add_pii_hashed_column(column_config)

    def _validate_mapping_inputs(self, column_config: SilverColumnConfig) -> None:
        cleaned_df = self._require_cleaned_df()
        target_col_name = column_config.name
        source_col_name = column_config.source_col_name

        if column_config.transform:
            return

        if source_col_name:
            if (
                source_col_name not in cleaned_df.columns
                and target_col_name not in cleaned_df.columns
            ):
                raise ValueError(
                    f"Column `{target_col_name}` is not available after schema mapping. "
                    "Provide a valid `source_col_name`."
                )
            return

        if target_col_name not in cleaned_df.columns:
            raise ValueError(
                f"Column `{target_col_name}` is not available after schema mapping. "
                "Provide a valid `source_col_name`."
            )

    def _add_target_column(self, column_config: SilverColumnConfig) -> None:
        cleaned_df = self._require_cleaned_df()
        if column_config.transform:
            self._apply_transform_expression(
                column_config.name, column_config.transform
            )
            return

        if (
            column_config.source_col_name
            and column_config.source_col_name in cleaned_df.columns
        ):
            self.cleaned_df = cleaned_df.withColumn(
                column_config.name, col(column_config.source_col_name)
            )

    def _apply_transform_expression(
        self, target_col_name: str, transform_expression: str
    ) -> None:
        cleaned_df = self._require_cleaned_df()
        self.cleaned_df = cleaned_df.withColumn(
            target_col_name, expr(transform_expression)
        )

    def _cast_target_column(self, target_col_name: str, required_type: str) -> None:
        cleaned_df = self._require_cleaned_df()
        if target_col_name not in cleaned_df.columns:
            raise ValueError(
                f"Column `{target_col_name}` is not available after schema mapping. "
                "Provide a valid `source_col_name`."
            )
        actual_type = cleaned_df.schema[target_col_name].dataType.simpleString()
        if actual_type != required_type:
            self.cleaned_df = cleaned_df.withColumn(
                target_col_name, col(target_col_name).cast(required_type)
            )

    def _add_pii_hashed_column(self, column_config: SilverColumnConfig) -> None:
        if not column_config.create_hashed_col:
            return

        cleaned_df = self._require_cleaned_df()
        if column_config.name not in cleaned_df.columns:
            raise ValueError(
                f"Configured PII column not found in DataFrame: {column_config.name}"
            )

        hashed_col_name = f"{column_config.name}{SuffixConstants.HASHED_COLUMN_SUFFIX}"
        self.cleaned_df = cleaned_df.withColumn(
            hashed_col_name, sha2(col(column_config.name).cast("string"), 256)
        )

    def select_silver_schema_columns(self):
        silver_columns = self.silver_table_config.get_all_column_names()
        if "_metadata" in self.cleaned_df.columns:
            silver_columns.append("_metadata")
        self.cleaned_df = self.cleaned_df.select(*silver_columns)
        logger.info("Final schema:")
        self.cleaned_df.printSchema()

    def _filter_invalid_key_groups(self):
        required_key_groups = [
            self.silver_table_config.primary_keys,
            self.silver_table_config.deduplication_keys,
        ]
        required_key_groups = [group for group in required_key_groups if group]
        if not required_key_groups:
            return

        logger.info(
            f"Filtering rows with invalid primary/deduplication key groups: {required_key_groups}"
        )
        self.cleaned_df = DFUtil.filter_non_null_rows(
            self.cleaned_df, required_key_groups
        )

    def deduplicate(self):
        self._filter_invalid_key_groups()
        primary_keys = [col(c) for c in self.silver_table_config.primary_keys]
        deduplication_keys = [
            col(c) for c in self.silver_table_config.deduplication_keys
        ]
        logger.info(
            f"Deduplicating data using primary keys: {primary_keys} and deduplication keys: {deduplication_keys}"
        )
        self.cleaned_df = DFUtil.deduplicate_data(
            self.cleaned_df, primary_keys, deduplication_keys
        )

    def upsert(self):
        silver_table_fqdn = self.job_configs.get("silver_table")
        merge_key = DFWriterUtil.get_merge_key(self.silver_table_config.primary_keys)
        merge_update_condition = DFWriterUtil.get_merge_update_condition(
            self.silver_table_config.deduplication_keys
        )
        DFWriterUtil.delta_merge(
            self.spark,
            df_to_upsert=self.cleaned_df,
            target_table_name=silver_table_fqdn,
            merge_key=merge_key,
            merge_update_condition=merge_update_condition,
            clustering_keys=self.silver_table_config.clustering_keys,
        )

    def process(self):
        self.read()
        if self.cleaned_df.isEmpty():
            if self._read_mode == LoadTypeConstants.INCREMENTAL:
                logger.info(
                    "Target table is up to date. No new data to process from bronze table."
                )
                return
            if self._read_mode == LoadTypeConstants.DATE_RANGE:
                logger.info(
                    "No bronze data found in the requested date range. Skipping processing."
                )
                return
            logger.info(
                "No bronze data found for full load. Creating/refreshing empty target table schema."
            )
        self.flatten()
        self.add_metadata_cols()
        self.apply_schema_mappings()
        self.select_silver_schema_columns()
        self.deduplicate()
        self.upsert()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--config_path", required=True)
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--task_name", required=True)
    parser.add_argument("--start_date", required=False)
    parser.add_argument("--end_date", required=False)
    input_params = vars(parser.parse_args())

    spark = get_spark_session()

    config_path_str = input_params["config_path"]
    configs = load_configs(
        config_file_name="config",
        config_path_str=config_path_str,
        profile=input_params["profile"],
    )
    task_configs = configs[input_params["job_name"]][input_params["task_name"]]
    job_configs = {**input_params, **task_configs}
    logger.info(
        f"Running {input_params['job_name']}-{input_params['task_name']} with configurations: \n{job_configs}"
    )
    silver_schema_config = load_silver_schema_config(
        config_path_str, task_configs["schema_config_file_name"]
    )
    logger.info(f"Schema configurations: \n{silver_schema_config}")

    schema_config_path = resolve_silver_schema_config_path(
        config_path_str, task_configs["schema_config_file_name"]
    )
    schema_name = SchemaNameGenerator.generate(
        schema_config_path,
        prefix=task_configs.get("schema_prefix"),
        root_folder="schemas",
    )
    silver_table_fqdn = (
        f"{task_configs['catalog']}.{schema_name}.{silver_schema_config.table.name}"
    )
    job_configs["silver_table"] = silver_table_fqdn
    logger.info(f"Derived silver table FQDN: {silver_table_fqdn}")

    silver_processor = SilverDataProcessor(
        spark, job_configs, silver_schema_config.table
    )
    silver_processor.process()
