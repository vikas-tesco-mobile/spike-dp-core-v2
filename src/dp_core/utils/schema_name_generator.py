from pathlib import Path
from typing import Optional


class SchemaNameGenerator:
    @staticmethod
    def generate(
        yml_file_path: Path,
        prefix: Optional[str] = None,
        root_folder: Optional[str] = None,
    ) -> str:
        """
        Generate schema name based on the immediate parent folder of the YAML file.
        When `root_folder` is provided, use the first folder beneath that root.
        """
        parent_folder = SchemaNameGenerator._resolve_schema_folder(
            yml_file_path=yml_file_path,
            root_folder=root_folder,
        )

        if not parent_folder:
            raise ValueError(
                f"Cannot determine schema name: file path '{yml_file_path}' has no parent directory"
            )

        schema_name = parent_folder

        if prefix and prefix.strip():
            schema_name = f"{prefix}{schema_name}"

        return schema_name

    @staticmethod
    def _resolve_schema_folder(
        yml_file_path: Path, root_folder: Optional[str] = None
    ) -> str:
        if root_folder:
            path_parts = yml_file_path.parts
            if root_folder in path_parts:
                root_index = path_parts.index(root_folder)
                schema_index = root_index + 1
                if schema_index < len(path_parts) - 1:
                    return path_parts[schema_index]

        return yml_file_path.parent.name
