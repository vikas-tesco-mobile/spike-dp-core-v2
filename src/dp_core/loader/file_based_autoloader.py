import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_json, struct, current_date, col

from dp_core.utils.config_loader import load_configs
from dp_core.utils.logger import get_logger
from dp_core.utils.spark_session import get_spark_session
from dp_core.utils.df_utils import CatalogUtil, DFTransformUtil

logger = get_logger(__name__)


class FileBasedAutoloader:
    def __init__(self, spark: SparkSession, configs: dict):
        self.spark: SparkSession = spark
        self.configs = configs
        self.format = self.configs.get("file_format")
        self.source_path = self.configs.get("source_path")
        self.target_table = self.configs.get("target_table")
        self.schema_location = self.configs.get("schema_location")
        self.checkpoint_location = self.configs.get("checkpoint_location")
        self.infer_column_types = self.configs.get("infer_column_types")
        self.trigger_option_available_now = self.configs.get(
            "trigger_option_available_now"
        )
        self.additional_read_options = self.configs.get("additional_read_options")
        self.raw_df = None

    def read(self):
        self.raw_df = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", self.format)
            .option("cloudFiles.schemaLocation", self.schema_location)
            .option("cloudFiles.inferColumnTypes", self.infer_column_types)
            .options(**self.additional_read_options)
            .load(self.source_path)
        )
        return self

    def transform(self):
        data_columns = [c for c in self.raw_df.columns if not c.startswith("_metadata")]
        self.raw_df = (
            self.raw_df.withColumn("_raw_data", to_json(struct(*data_columns)))
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_ingestion_date", current_date())
            .withColumn("_metadata", col("_metadata"))
        )
        self.raw_df = DFTransformUtil.sanitize_column_names(self.raw_df)
        return self

    def write(self):
        CatalogUtil.create_schema_if_not_exists(self.spark, self.target_table)
        stream_writer = (
            self.raw_df.writeStream.format("delta")
            .option("checkpointLocation", self.checkpoint_location)
            .option("mergeSchema", "true")
            .outputMode("append")
        )

        if self.trigger_option_available_now:
            stream_writer = stream_writer.trigger(availableNow=True)

        query = stream_writer.toTable(self.target_table)
        query.awaitTermination()
        return self


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--config_path", required=True)
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--task_name", required=True)
    args = parser.parse_args()

    spark = get_spark_session()

    configs = load_configs(
        config_file_name="config",
        config_path_str=args.config_path,
        profile=args.profile,
    )
    job_configs = configs[args.job_name][args.task_name]
    logger.info(
        f"Running {args.job_name}-{args.task_name} with configurations: {job_configs}"
    )

    job = FileBasedAutoloader(spark, job_configs)
    job.read().transform().write()
