from pathlib import Path
from typing import Any

import yaml
from yaml.nodes import MappingNode, Node, SequenceNode

YamlPath = tuple[str | int, ...]


def _build_yaml_line_map(
    node: Node, path: YamlPath = (), line_map: dict[YamlPath, int] | None = None
) -> dict[YamlPath, int]:
    if line_map is None:
        line_map = {}

    if isinstance(node, MappingNode):
        for key_node, value_node in node.value:
            key = key_node.value
            key_path = path + (key,)
            line_map[key_path] = key_node.start_mark.line + 1
            _build_yaml_line_map(value_node, key_path, line_map)
    elif isinstance(node, SequenceNode):
        for index, item_node in enumerate(node.value):
            item_path = path + (index,)
            line_map[item_path] = item_node.start_mark.line + 1
            _build_yaml_line_map(item_node, item_path, line_map)

    return line_map


def load_yaml_with_line_numbers(
    config_file_path: str | Path,
) -> tuple[Any, dict[YamlPath, int]]:
    config_path = Path(config_file_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    content = config_path.read_text()
    parsed_content = yaml.safe_load(content)
    yaml_node = yaml.compose(content)

    if yaml_node is None:
        return parsed_content, {}

    return parsed_content, _build_yaml_line_map(yaml_node)


def get_nearest_line_for_yaml_path(
    line_map: dict[YamlPath, int], path: YamlPath
) -> int | None:
    current_path = path
    while current_path:
        if current_path in line_map:
            return line_map[current_path]
        current_path = current_path[:-1]

    return None
