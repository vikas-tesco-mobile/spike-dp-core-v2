from sqlglot import exp, parse
from sqlglot.errors import ParseError


class SQLExpressionValidator:
    _UNSAFE_ROOT_EXPRESSIONS: tuple[type[exp.Expression], ...] = (
        exp.Alter,
        exp.Command,
        exp.Create,
        exp.Delete,
        exp.Drop,
        exp.Grant,
        exp.Insert,
        exp.Merge,
        exp.Refresh,
        exp.Revoke,
        exp.Set,
        exp.TruncateTable,
        exp.Update,
        exp.Use,
    )

    @classmethod
    def validate(cls, expression: str, target_col_name: str) -> None:
        normalized_expression = expression.strip()
        if not normalized_expression:
            raise ValueError(
                f"Empty SQL expression configured for target column `{target_col_name}`."
            )
        if (
            ";" in normalized_expression
            or "--" in normalized_expression
            or "/*" in normalized_expression
            or "*/" in normalized_expression
        ):
            raise ValueError(
                f"Unsafe SQL expression configured for target column `{target_col_name}`."
            )

        try:
            parsed_statements = parse(normalized_expression, read="databricks")
        except ParseError as exc:
            raise ValueError(
                f"Invalid SQL syntax configured for target column `{target_col_name}`."
            ) from exc

        if len(parsed_statements) != 1:
            raise ValueError(
                f"Unsafe SQL expression configured for target column `{target_col_name}`."
            )

        parsed_expression = parsed_statements[0]
        if isinstance(parsed_expression, cls._UNSAFE_ROOT_EXPRESSIONS):
            raise ValueError(
                f"Unsafe SQL expression configured for target column `{target_col_name}`."
            )
