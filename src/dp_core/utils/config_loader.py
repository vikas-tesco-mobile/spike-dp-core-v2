from pathlib import Path

import yaml
from dotenv import dotenv_values
from jinja2 import Environment, FileSystemLoader, select_autoescape

from dp_core.transformer.models.silver_layer_config import SilverSchemaConfig
from dp_core.utils.logger import get_logger

logger = get_logger(__name__)


def _load_profile_variables(
    config_path_str: str, profile: str | None, subfolder: str
) -> dict:
    """
    Load environment-specific variables from profiles.yml or .env for local profile.

    Updated Behavior:
      • For 'local' profile: variables are loaded from .env file using dotenv_values.
      • For other profiles (dev/prod): variables are loaded from profiles.yml.
    """
    if profile == "local":
        env_path = Path(config_path_str) / subfolder / ".env"
        if not env_path.exists():
            raise FileNotFoundError(f".env file not found at: {env_path}")

        env_variables = dotenv_values(env_path)
        variables = {
            "env": env_variables.get("ENV"),
            "developer_prefix": env_variables.get("DEVELOPER_PREFIX"),
            "catalog": env_variables.get("CATALOG"),
        }
        logger.info("Loaded local profile variables from .env file")

        if not variables["developer_prefix"]:
            raise ValueError("developer_prefix cannot be empty for 'local' profile")

        return variables

    profile_path = Path(config_path_str) / subfolder / "profiles.yml"
    if not profile_path.exists():
        raise FileNotFoundError(f"profiles.yml not found at: {profile_path}")

    with open(profile_path, "r") as file:
        all_profiles = yaml.safe_load(file) or {}

    project_name, project_config = next(iter(all_profiles.items()))
    outputs = project_config.get("outputs", {})
    target_env = profile or project_config.get("target")

    if not target_env:
        raise ValueError(
            f"No profile provided and no 'target' defined in {project_name}"
        )

    if target_env not in outputs:
        raise KeyError(f"Profile '{target_env}' not found under {project_name}.outputs")

    variables = outputs[target_env].copy()
    logger.info(f"Loaded profile '{target_env}' for project '{project_name}'")

    return variables


def _render_jinja_template(
    config_path_str: str, subfolder: str, template_file: str, variables: dict
) -> dict:
    template_path = Path(config_path_str) / subfolder
    env = Environment(
        loader=FileSystemLoader(str(template_path)),
        keep_trailing_newline=True,
        autoescape=select_autoescape(
            enabled_extensions=("yml", "yaml"),
            default_for_string=False,
            default=False,
        ),
    )
    template = env.get_template(template_file)
    rendered = template.render(**variables)
    return yaml.safe_load(rendered)


def load_configs(
    config_file_name: str,
    config_path_str: str,
    profile: str | None = None,
    subfolder: str = "envs",
) -> dict:
    if not config_path_str:
        raise ValueError("No configuration path provided")

    if profile:
        template_file = f"{config_file_name}.yml.j2"
        config_path = Path(f"{config_path_str}/{subfolder}/{template_file}")
        if not config_path.exists():
            message = f"Configuration template not found at: {config_path}"
            logger.error(message)
            raise FileNotFoundError(message)

        logger.info(f"Loading config from: {config_path} with profile: {profile}")
        variables = _load_profile_variables(config_path_str, profile, subfolder)
        output = _render_jinja_template(
            config_path_str, subfolder, template_file, variables
        )
        return output
    else:
        config_path = Path(f"{config_path_str}/{subfolder}/{config_file_name}.yml")
        if not config_path.exists():
            message = f"Configuration file not found at: {config_path}"
            logger.error(message)
            raise FileNotFoundError(message)

        logger.info(f"Loading config from: {config_path}")
        with open(config_path, "r") as file:
            return yaml.safe_load(file)


def resolve_silver_schema_config_path(
    config_path_str: str, schema_config_file_name: str, schema_path_str: str = "schemas"
) -> Path:
    config_path = Path(config_path_str)
    schema_path = config_path / schema_path_str
    list_of_schema_files = list(schema_path.rglob("*.yml"))
    matched_schema_files = [
        file
        for file in list_of_schema_files
        if file.name == schema_config_file_name or file.stem == schema_config_file_name
    ]
    if not matched_schema_files:
        available = sorted({p.name for p in list_of_schema_files})
        raise FileNotFoundError(
            f"Schema config '{schema_config_file_name}' not found under '{schema_path}'. "
            f"Available schema files: {available}"
        )
    if len(matched_schema_files) > 1:
        candidates = sorted(str(p) for p in matched_schema_files)
        raise ValueError(
            f"Schema config '{schema_config_file_name}' is ambiguous. Matches: {candidates}"
        )
    schema_config_path = matched_schema_files[0]
    logger.info(f"Using schema config file: {schema_config_path}")
    return schema_config_path


def load_silver_schema_config(
    config_path_str: str, schema_config_file_name: str, schema_path_str: str = "schemas"
) -> SilverSchemaConfig:
    config_path = Path(config_path_str)
    schema_config_path = resolve_silver_schema_config_path(
        config_path_str, schema_config_file_name, schema_path_str
    )
    schema_configs = load_configs(
        config_file_name=schema_config_path.stem,
        config_path_str=str(config_path),
        profile=None,
        subfolder=str(schema_config_path.parent.relative_to(config_path)),
    )
    return SilverSchemaConfig(**schema_configs)
