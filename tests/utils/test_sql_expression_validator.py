import pytest

from dp_core.utils.sql_expression_validator import SQLExpressionValidator


def test_validate_accepts_safe_transform_expression():
    SQLExpressionValidator.validate("concat(first_name, ' ', last_name)", "full_name")


@pytest.mark.parametrize(
    "expression",
    [
        "",
        "   ",
    ],
)
def test_validate_rejects_empty_expression(expression):
    with pytest.raises(ValueError) as exc_info:
        SQLExpressionValidator.validate(expression, "full_name")

    assert "Empty SQL expression configured for target column `full_name`." in str(
        exc_info.value
    )


@pytest.mark.parametrize(
    "expression",
    [
        "id; DROP TABLE x",
        "id -- comment",
        "id /* comment */",
        "DROP TABLE x",
        "update x set a = 1",
        "show tables",
    ],
)
def test_validate_rejects_unsafe_expression(expression):
    with pytest.raises(ValueError) as exc_info:
        SQLExpressionValidator.validate(expression, "unsafe_col")

    assert "Unsafe SQL expression configured for target column `unsafe_col`." in str(
        exc_info.value
    )


@pytest.mark.parametrize(
    "expression",
    [
        "id +",
        "concat(first_name, ' ', last_name",
    ],
)
def test_validate_rejects_invalid_sql_syntax(expression):
    with pytest.raises(ValueError) as exc_info:
        SQLExpressionValidator.validate(expression, "invalid_syntax_col")

    assert (
        "Invalid SQL syntax configured for target column `invalid_syntax_col`."
        in str(exc_info.value)
    )
