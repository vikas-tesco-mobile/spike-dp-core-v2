from pathlib import Path

import pytest

from dp_core.utils.schema_name_generator import SchemaNameGenerator


@pytest.mark.parametrize(
    "file_path,prefix,root_folder,expected",
    [
        (
            Path("/tmp/configs/schemas/finance_silver/table1.yml"),
            None,
            None,
            "finance_silver",
        ),
        (
            Path("/tmp/configs/schemas/finance_silver/table1.yml"),
            "dev_",
            None,
            "dev_finance_silver",
        ),
        (
            Path("/tmp/configs/schemas/finance_silver/schema.yml"),
            " ",
            None,
            "finance_silver",
        ),
        (
            Path(
                "/tmp/dbt_project/models/revenue_assurance_gold/"
                "blugem_ccs_usage_reconciliation/blugem_ccs_usage_reconciliation.yml"
            ),
            None,
            None,
            "blugem_ccs_usage_reconciliation",
        ),
        (
            Path(
                "/tmp/dbt_project/models/revenue_assurance_gold/"
                "blugem_ccs_usage_reconciliation/blugem_ccs_usage_reconciliation.yml"
            ),
            None,
            "models",
            "revenue_assurance_gold",
        ),
        (
            Path(
                "/tmp/dbt_project/models/revenue_assurance_gold/"
                "blugem_ccs_usage_reconciliation/intermediate/intermediate_blugem_usage.yml"
            ),
            "dev_",
            None,
            "dev_intermediate",
        ),
        (
            Path(
                "/tmp/dbt_project/models/revenue_assurance_gold/"
                "blugem_ccs_usage_reconciliation/intermediate/intermediate_blugem_usage.yml"
            ),
            "dev_",
            "models",
            "dev_revenue_assurance_gold",
        ),
    ],
)
def test_generate_returns_expected_schema_name(
    file_path, prefix, root_folder, expected
):
    result = SchemaNameGenerator.generate(file_path, prefix, root_folder)
    assert result == expected, f"Expected '{expected}' but got '{result}'."


def test_generate_raises_error_on_missing_parent(tmp_path):
    file_path = Path("table.yml")
    with pytest.raises(ValueError) as error:
        SchemaNameGenerator.generate(file_path)
    assert (
        "Cannot determine schema name: file path 'table.yml' has no parent directory"
        in str(error.value)
    )
