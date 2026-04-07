from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator
from dp_core.utils.constants import SuffixConstants
from dp_core.utils.sql_expression_validator import SQLExpressionValidator


class SilverColumnConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    type: str
    source_col_name: Optional[str] = None
    transform: Optional[str] = Field(
        default=None,
        description="Optional Spark SQL expression to derive this target column.",
    )
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    create_hashed_col: Optional[bool] = False

    @field_validator("transform")
    @classmethod
    def validate_transform(
        cls, value: Optional[str], info: ValidationInfo
    ) -> Optional[str]:
        if value is None:
            return value
        SQLExpressionValidator.validate(value, info.data.get("name", "transform"))
        return value


class ColumnFlattenerConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    cols_to_explode: Optional[List[str]] = None
    cols_to_exclude: Optional[List[str]] = None
    explode_with_index_col: Optional[bool] = False


class SilverTableConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    description: Optional[str] = None
    tags: Dict[str, str] = Field(default_factory=dict)

    primary_keys: List[str] = Field(default_factory=list)
    deduplication_keys: List[str] = Field(default_factory=list)

    flatten_config: Optional[ColumnFlattenerConfig] = None
    clustering_keys: Optional[List[str]] = None

    columns: List[SilverColumnConfig] = Field(default_factory=list)

    def get_all_column_names(self) -> List[str]:
        column_names = []
        for column in self.columns:
            column_names.append(column.name)
            if column.create_hashed_col:
                column_names.append(
                    f"{column.name}{SuffixConstants.HASHED_COLUMN_SUFFIX}"
                )
        return column_names

    def get_columns_to_be_hashed(self):
        columns_to_be_hashed = []
        for column in self.columns:
            if column.create_hashed_col:
                columns_to_be_hashed.append(column.name)
        return columns_to_be_hashed

    def get_columns_to_explode(self) -> Optional[List[str]]:
        if self.flatten_config:
            return self.flatten_config.cols_to_explode
        else:
            return None

    def get_columns_to_exclude_from_flattening(self) -> Optional[List[str]]:
        if self.flatten_config:
            return self.flatten_config.cols_to_exclude
        else:
            return None


class SilverSchemaConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    version: float
    table: SilverTableConfig
