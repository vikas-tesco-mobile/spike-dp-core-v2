from functools import reduce
from operator import and_, or_
from typing import Optional
import re

from delta import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import col, posexplode
from pyspark.sql.types import StructType, ArrayType, StringType
from pyspark.sql.window import Window

from dp_core.utils.logger import get_logger
from dp_core.utils.constants import SuffixConstants

logger = get_logger(__name__)


class DFReaderUtil:
    @staticmethod
    def read_table(spark: SparkSession, table_name: str):
        return spark.read.table(table_name)


class CatalogUtil:
    @classmethod
    def create_schema_if_not_exists(cls, spark: SparkSession, table_name: str):
        catalog_name = table_name.split(".")[0]
        schema_name = table_name.split(".")[1]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")


class DFWriterUtil:
    @staticmethod
    def get_merge_key(primary_keys: list[str]) -> str:
        merge_keys = []
        for pk in primary_keys:
            merge_keys.append(f"old.{pk} <=> new.{pk}")
        return " AND ".join(merge_keys)

    @staticmethod
    def get_merge_update_condition(dedup_keys: list[str]) -> Optional[str]:
        if not dedup_keys:
            return None
        merge_update_conditions = []
        for dk in dedup_keys:
            merge_update_conditions.append(f"old.{dk} <= new.{dk}")
        return " AND ".join(merge_update_conditions)

    @staticmethod
    def delta_merge(
        spark: SparkSession,
        df_to_upsert: DataFrame,
        target_table_name: str,
        merge_key: str,
        merge_update_condition: Optional[str] = None,
        clustering_keys: Optional[list[str]] = None,
    ):
        CatalogUtil.create_schema_if_not_exists(spark, target_table_name)
        if not spark.catalog.tableExists(target_table_name):
            logger.info(
                f"Given table does not exists. Creating new table {target_table_name}"
            )
            writer = df_to_upsert.write.format("delta").mode("overwrite")
            if clustering_keys:
                writer.clusterBy(*clustering_keys)

            writer.saveAsTable(target_table_name)
        else:
            delta_table = DeltaTable.forName(spark, target_table_name)
            (
                delta_table.alias("old")
                .merge(df_to_upsert.alias("new"), merge_key)
                .withSchemaEvolution()
                .whenMatchedUpdateAll(condition=merge_update_condition)
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info(
                f"""MERGE successful for target table: {target_table_name} with merge key {merge_key} """
                f"""and merge update condition {merge_update_condition}"""
            )

    @staticmethod
    def write(
        spark: SparkSession,
        df: DataFrame,
        target_table_name: str,
        cluster_by_cols: Optional[list[str]] = None,
    ):
        logger.info(
            f"Writing DataFrame to {target_table_name} (mode=overwrite, cluster_by={cluster_by_cols})"
        )
        write_statement = df.write.format("delta").mode("overwrite")
        if cluster_by_cols:
            (
                write_statement.clusterBy(cluster_by_cols).option(
                    "overwriteSchema", "true"
                )
            )
        CatalogUtil.create_schema_if_not_exists(spark, target_table_name)
        write_statement.saveAsTable(target_table_name)


class DFTransformUtil:
    @classmethod
    def sanitize_column_name(cls, name: str) -> str:
        """
        - Replace spaces, punctuation & symbols, tabs/newlines (anything not [A-Za-z0-9_]) with underscore
        - Fallback to "col" if the result has no alphanumeric characters
        """
        col_name = (name or "").strip()
        snake_case_col_name = re.sub(r"[^A-Za-z0-9_]+", "_", col_name)
        if not re.search(r"[A-Za-z0-9]", snake_case_col_name):
            snake_case_col_name = "col"
        return snake_case_col_name

    @classmethod
    def sanitize_column_names(cls, df: DataFrame) -> DataFrame:
        used_lower: set[str] = set()
        counts: dict[str, int] = {}
        mapping: dict[str, str] = {}

        for c in df.columns:
            base = cls.sanitize_column_name(c)
            key = base.lower()
            count = counts.get(key, 0)

            if count == 0:
                proposed = base
            else:
                proposed = f"{base}_{count}"

            while proposed.lower() in used_lower:
                count += 1
                proposed = f"{base}_{count}"

            used_lower.add(proposed.lower())
            counts[key] = count + 1
            mapping[c] = proposed

        if all(old == new for old, new in mapping.items()):
            return df

        logger.info(f"Sanitizing column names: {mapping}")
        return df.select([col(old).alias(mapping[old]) for old in df.columns])


class DFUtil:
    @staticmethod
    def rename_columns(df: DataFrame, col_map: dict[str, str]) -> DataFrame:
        missing = [c for c in col_map.keys() if c not in df.columns]
        if missing:
            raise ValueError(f"Columns not found for renaming: {missing}")
        renamed_df = df
        for old_name, new_name in col_map.items():
            if old_name != new_name:
                renamed_df = renamed_df.withColumnRenamed(old_name, new_name)
        return renamed_df

    @staticmethod
    def _build_non_empty_col_condition(df: DataFrame, col_name: str) -> Column:
        current_condition = col(col_name).isNotNull()
        if isinstance(df.schema[col_name].dataType, StringType):
            current_condition = current_condition & (F.trim(col(col_name)) != "")
        return current_condition

    @staticmethod
    def _build_pk_dedupe_group_filter(df: DataFrame, col_group: list[str]) -> Column:
        return reduce(
            or_,
            [
                DFUtil._build_non_empty_col_condition(df, col_name)
                for col_name in col_group
            ],
        )

    @staticmethod
    def filter_non_null_rows(
        df: DataFrame, required_col_groups: list[list[str]]
    ) -> DataFrame:
        """
        Filter rows by primary-key or deduplication-key groups.

        Each inner list represents one required group of columns. A row is kept only
        when every group has at least one populated column. Within a group, columns are
        combined with OR. Across groups, the group filters are combined with AND.
        ``NULL`` values are treated as empty, and string columns are also treated as
        empty when they are blank after trimming whitespace.
        """
        if not required_col_groups:
            return df

        flat_cols = {col_name for group in required_col_groups for col_name in group}
        missing = [c for c in flat_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Columns not found for non-null filtering: {missing}")

        group_filters = [
            DFUtil._build_pk_dedupe_group_filter(df, col_group)
            for col_group in required_col_groups
            if col_group
        ]
        final_filter = reduce(and_, group_filters)

        return df.filter(final_filter)

    @staticmethod
    def deduplicate_data(
        df: DataFrame, primary_keys: list[Column], dedup_cols: list[Column]
    ) -> DataFrame:
        # Order by all dedup columns in descending order with nulls last
        windowSpec = Window.partitionBy(*primary_keys).orderBy(
            *[c.desc_nulls_last() for c in dedup_cols]
        )
        deduped_df = (
            df.withColumn("row_num", F.row_number().over(windowSpec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )
        return deduped_df

    @staticmethod
    def recursive_flatten(
        df: DataFrame,
        array_cols_to_explode: Optional[list[str]] = None,
        cols_to_exclude_from_flattening: Optional[list[str]] = None,
        explode_with_index_col: bool = False,
    ) -> DataFrame:
        array_cols_to_explode = array_cols_to_explode or []
        fields = []
        for field in df.schema.fields:
            col_name = field.name

            if (
                cols_to_exclude_from_flattening
                and col_name in cols_to_exclude_from_flattening
            ):
                fields.append(col(col_name))
                continue

            if isinstance(field.dataType, StructType):
                new_fields = [
                    col(f"{col_name}.{nested_field.name}").alias(
                        f"{col_name}__{nested_field.name}"
                    )
                    for nested_field in field.dataType.fields
                ]
                fields.extend(new_fields)
            elif isinstance(field.dataType, ArrayType) and isinstance(
                field.dataType.elementType, StructType
            ):
                if col_name in array_cols_to_explode:
                    if explode_with_index_col:
                        index_alias = f"{col_name}{SuffixConstants.INDEX_SUFFIX}"
                        other_columns = [col(c) for c in df.columns if c != col_name]
                        df_exploded = df.select(
                            *other_columns,
                            posexplode(col(col_name)).alias(index_alias, col_name),
                        )
                    else:
                        df_exploded = df.withColumn(col_name, F.explode(col(col_name)))
                    return DFUtil.recursive_flatten(
                        df_exploded,
                        array_cols_to_explode,
                        cols_to_exclude_from_flattening,
                        explode_with_index_col,
                    )
                else:
                    fields.append(col(col_name))
            else:
                fields.append(col(col_name))

        if not fields:
            return df
        df_flattened = df.select(*fields)
        has_structs_to_flatten = any(
            isinstance(f.dataType, StructType)
            and f.name not in (cols_to_exclude_from_flattening or [])
            for f in df_flattened.schema.fields
        )
        if has_structs_to_flatten:
            return DFUtil.recursive_flatten(
                df_flattened,
                array_cols_to_explode,
                cols_to_exclude_from_flattening,
                explode_with_index_col,
            )
        return df_flattened
