class TimeStringConstants:
    DATE_FORMAT = "%Y%m%d"
    DATETIME_FORMAT = "%Y%m%d%H%M%S"
    HOUR_FORMAT = "%H"


class SuffixConstants:
    HASHED_COLUMN_SUFFIX = "_hashed_key"
    INDEX_SUFFIX = "__index"


class LoadTypeConstants:
    INCREMENTAL = "incremental"
    DATE_RANGE = "date_range"
    FULL_LOAD = "full_load"


class ExporterConstants:
    SUPPORTED_FORMATS = {
        "csv": {
            "spark_format": "csv",
            "target_file_extension": ".csv",
            "spark_write_file_extension": ".csv",
        },
        "json": {
            "spark_format": "json",
            "target_file_extension": ".json",
            "spark_write_file_extension": ".json",
        },
        "jsonl": {
            "spark_format": "json",
            "target_file_extension": ".jsonl",
            "spark_write_file_extension": ".json",
        },
        "parquet": {
            "spark_format": "parquet",
            "target_file_extension": ".parquet",
            "spark_write_file_extension": ".parquet",
        },
    }
