import random
import string
import sys
import types
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    sql_warehouse = tmp_path_factory.mktemp("sql-warehouse")
    builder = SparkSession.builder.appName("test")
    builder = (
        builder.config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(sql_warehouse))
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture()
def patch_pipelines(monkeypatch):
    fake = types.ModuleType("pyspark.pipelines")

    def create_table(**kwargs):
        return None

    def table(**outer_kwargs):
        def decorator(fn):
            return fn

        return decorator

    fake.create_table = create_table  # type: ignore
    fake.table = table  # type: ignore

    monkeypatch.setitem(sys.modules, "pyspark.pipelines", fake)
    return fake


def generate_random_string():
    random_str_generator = "".join(random.choices(string.ascii_uppercase, k=3))
    return random_str_generator


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def get_all_silver_schema_files():
    base_path = project_root() / "configs"
    silver_schema_dir = base_path / "schemas"
    return [
        (p.stem, str(p.parent.relative_to(base_path)))
        for p in silver_schema_dir.rglob("*.yml")
        if p.parent != silver_schema_dir
    ]


def get_all_dbt_gold_schema_files():
    gold_models_path = project_root() / "dbt_project" / "models"
    base_path = project_root() / "dbt_project"
    return [
        (p.stem, str(p.parent.relative_to(base_path)))
        for p in gold_models_path.rglob("*.yml")
        if p.parent != gold_models_path
    ]
