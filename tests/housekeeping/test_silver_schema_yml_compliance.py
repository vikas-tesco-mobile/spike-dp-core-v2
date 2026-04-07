import pytest
import yaml
from pydantic import ValidationError

from conftest import get_all_silver_schema_files, project_root
from dp_core.transformer.models.silver_layer_config import SilverSchemaConfig
from dp_core.utils.yaml_utils import (
    get_nearest_line_for_yaml_path,
    load_yaml_with_line_numbers,
)


@pytest.mark.parametrize(("schema_file", "subfolder"), get_all_silver_schema_files())
def test_silver_schema_yaml_is_compliant(schema_file: str, subfolder: str):
    repo_root = project_root()
    schema_path = repo_root / "configs" / subfolder / f"{schema_file}.yml"
    rel_schema_path = schema_path.relative_to(repo_root)

    try:
        schema_config, line_map = load_yaml_with_line_numbers(schema_path)
    except yaml.YAMLError as exc:
        problem_mark = getattr(exc, "problem_mark", None)
        line = (problem_mark.line + 1) if problem_mark else 1
        message = getattr(exc, "problem", None) or str(exc)
        pytest.fail(
            f"::error file={rel_schema_path},line={line},col=1::YAML syntax error: {message}"
        )

    assert isinstance(schema_config, dict), (
        f"'{rel_schema_path}' must parse into a dict"
    )

    try:
        SilverSchemaConfig.model_validate(schema_config)
    except ValidationError as e:
        annotation_lines: list[str] = []
        for error in e.errors():
            location_path = tuple(error.get("loc", ()))
            line = get_nearest_line_for_yaml_path(line_map, location_path) or 1
            location = ".".join(str(part) for part in location_path)
            annotation_lines.append(
                f"::error file={rel_schema_path},line={line},col=1::{location}: {error['msg']}"
            )

        pytest.fail(
            f"'{rel_schema_path}' is not compliant with SilverTableConfig:\n"
            + "\n".join(annotation_lines)
        )
