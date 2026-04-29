"""Run Snowflake raw-layer setup and sample loads."""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from manufacturing_pipeline.utils.datasets import DATASETS, dataset_names


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DDL_DIR = DEFAULT_PROJECT_ROOT / "warehouse" / "snowflake" / "ddl"
REQUIRED_ENV_KEYS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value)


def load_config(env_path: Path | None = None) -> SnowflakeConfig:
    if env_path is not None:
        load_dotenv(env_path)

    missing_keys = [key for key in REQUIRED_ENV_KEYS if not os.environ.get(key)]
    if missing_keys:
        missing = ", ".join(missing_keys)
        raise RuntimeError(f"Missing required Snowflake environment values: {missing}")

    return SnowflakeConfig(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def validate_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid Snowflake {label}: {value}")
    return value


def qualified_name(*parts: str) -> str:
    return ".".join(validate_identifier(part, "identifier") for part in parts)


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    quote_char: str | None = None
    index = 0

    while index < len(sql_text):
        char = sql_text[index]
        next_char = sql_text[index + 1] if index + 1 < len(sql_text) else ""

        if quote_char is None and char == "-" and next_char == "-":
            while index < len(sql_text) and sql_text[index] != "\n":
                current.append(sql_text[index])
                index += 1
            continue

        if char in ("'", '"'):
            if quote_char is None:
                quote_char = char
            elif quote_char == char:
                if next_char == char:
                    current.append(char)
                    current.append(next_char)
                    index += 2
                    continue
                quote_char = None

        if char == ";" and quote_char is None:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)

        index += 1

    statement = "".join(current).strip()
    if statement:
        statements.append(statement)

    return statements


def read_sql_file(path: Path) -> list[str]:
    return split_sql_statements(path.read_text(encoding="utf-8"))


def connect(config: SnowflakeConfig):
    try:
        import snowflake.connector
    except ImportError as exc:
        raise RuntimeError(
            "Snowflake connector is not installed. Run: "
            "python -m pip install -r requirements.txt"
        ) from exc

    return snowflake.connector.connect(
        account=config.account,
        user=config.user,
        password=config.password,
        role=config.role,
        warehouse=config.warehouse,
    )


def execute_statements(cursor, statements: Iterable[str]) -> None:
    for statement in statements:
        cursor.execute(statement)


def use_project_context(cursor, config: SnowflakeConfig) -> None:
    cursor.execute(f"USE DATABASE {validate_identifier(config.database, 'database')}")
    cursor.execute(f"USE SCHEMA {validate_identifier(config.schema, 'schema')}")


def put_sample_files(cursor, project_root: Path, config: SnowflakeConfig) -> None:
    stage_name = qualified_name(config.database, config.schema, "BOSCH_SAMPLE_STAGE")

    for dataset_name in dataset_names():
        sample_path = project_root / DATASETS[dataset_name]["sample_path"]
        if not sample_path.exists():
            raise FileNotFoundError(
                f"Missing sample file: {sample_path}. Run scripts/run_profile.sh first."
            )

        put_sql = (
            f"PUT file://{sample_path.resolve().as_posix()} "
            f"@{stage_name} "
            "AUTO_COMPRESS = TRUE "
            "OVERWRITE = TRUE"
        )
        cursor.execute(put_sql)
        print(f"Staged {sample_path.name}")


def print_query_results(cursor, query: str) -> None:
    cursor.execute(query)
    columns = [column[0] for column in cursor.description or []]
    if columns:
        print(" | ".join(columns))
    for row in cursor.fetchall():
        print(" | ".join(str(value) for value in row))


def run_phase4(project_root: Path, ddl_dir: Path, env_path: Path) -> None:
    config = load_config(env_path)

    with connect(config) as connection:
        cursor = connection.cursor()
        try:
            print("Creating Snowflake raw objects")
            execute_statements(
                cursor, read_sql_file(ddl_dir / "001_create_raw_objects.sql")
            )
            use_project_context(cursor, config)

            print("Staging sample files")
            put_sample_files(cursor, project_root, config)

            print("Creating raw tables")
            execute_statements(
                cursor, read_sql_file(ddl_dir / "002_create_raw_tables.sql")
            )

            print("Loading sample data")
            execute_statements(
                cursor, read_sql_file(ddl_dir / "003_copy_sample_data.sql")
            )

            print("Validating raw load")
            for query in read_sql_file(ddl_dir / "004_validate_raw_load.sql"):
                print_query_results(cursor, query)
        finally:
            cursor.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create and validate the Snowflake raw sample load."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
        help="Project root containing .env and data/sample files.",
    )
    parser.add_argument(
        "--ddl-dir",
        type=Path,
        default=DEFAULT_DDL_DIR,
        help="Directory containing Snowflake DDL SQL files.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Path to local Snowflake .env file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = args.project_root.resolve()
    ddl_dir = args.ddl_dir.resolve()
    env_path = args.env_file.resolve() if args.env_file else project_root / ".env"

    run_phase4(project_root=project_root, ddl_dir=ddl_dir, env_path=env_path)


if __name__ == "__main__":
    main()
