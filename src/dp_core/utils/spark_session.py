from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    builder = SparkSession.builder.config(
        "spark.databricks.delta.schema.autoMerge.enabled", True
    ).config("spark.sql.session.timeZone", "UTC")
    return builder.getOrCreate()
