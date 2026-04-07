import argparse

from pyspark.sql import SparkSession

from dp_core.utils.config_loader import load_configs
from dp_core.utils.constants import ExporterConstants
from dp_core.utils.databricks_utils import rename_spark_part_files
from dp_core.utils.df_utils import DFReaderUtil
from dp_core.utils.logger import get_logger
from dp_core.utils.spark_session import get_spark_session
from dp_core.utils.time_utils import build_dates_for_path

logger = get_logger(__name__)


class DataExporter:
    def __init__(self, spark: SparkSession, configs: dict):
        self.spark = spark
        self.configs = configs
        self.table_name = self.configs.get("table_name")
        self.filename_prefix = self.configs.get("filename_prefix")
        self.output_location = self.configs.get("output_location")
        self.additional_write_options = self.configs.get("additional_write_options", {})
        self.write_mode = self.configs.get("write_mode", "overwrite")
        self.file_format = self.configs.get("file_format", "csv").lower()

        format_config = ExporterConstants.SUPPORTED_FORMATS.get(self.file_format)
        if not format_config:
            supported_formats = ", ".join(sorted(ExporterConstants.SUPPORTED_FORMATS))
            raise ValueError(
                f"Unsupported file_format '{self.file_format}'. "
                f"Supported formats: {supported_formats}"
            )

        self.spark_format = format_config["spark_format"]
        self.target_file_extension = format_config["target_file_extension"]
        self.spark_write_file_extension = format_config["spark_write_file_extension"]

    def export(self):
        current_date, _, current_datetime = build_dates_for_path()
        output_path = f"{self.output_location}/{current_date}"
        output_filename_prefix = f"{self.filename_prefix}_{current_datetime}"
        table = DFReaderUtil.read_table(self.spark, self.table_name)
        (
            table.coalesce(1)
            .write.mode(self.write_mode)
            .options(**self.additional_write_options)
            .format(self.spark_format)
            .save(output_path)
        )
        rename_kwargs = {}
        if self.spark_write_file_extension != self.target_file_extension:
            rename_kwargs["source_extension"] = self.spark_write_file_extension

        rename_spark_part_files(
            output_path,
            output_filename_prefix,
            self.target_file_extension,
            **rename_kwargs,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--config_path", required=True)
    parser.add_argument("--job_name", required=True)
    args = parser.parse_args()

    configs = load_configs(
        config_file_name="config",
        config_path_str=args.config_path,
        profile=args.profile,
    )
    job_configs = configs[args.job_name]
    logger.info(f"Running {args.job_name}")
    spark = get_spark_session()
    exporter = DataExporter(spark, job_configs)
    exporter.export()
