"""Run dbt commands with project-local configuration."""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

from manufacturing_pipeline.warehouse.snowflake_runner import DEFAULT_PROJECT_ROOT, load_dotenv


DEFAULT_DBT_PROJECT_DIR = DEFAULT_PROJECT_ROOT / "warehouse" / "dbt"


def run_dbt_build(project_root: Path, dbt_project_dir: Path, env_path: Path) -> None:
    load_dotenv(env_path)

    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", str(dbt_project_dir))

    command = [
        "dbt",
        "build",
        "--project-dir",
        str(dbt_project_dir),
        "--profiles-dir",
        env["DBT_PROFILES_DIR"],
    ]
    subprocess.run(command, cwd=project_root, env=env, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run dbt build for the manufacturing defect project."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
        help="Project root containing .env and warehouse/dbt.",
    )
    parser.add_argument(
        "--dbt-project-dir",
        type=Path,
        default=DEFAULT_DBT_PROJECT_DIR,
        help="Path to the dbt project directory.",
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
    dbt_project_dir = args.dbt_project_dir.resolve()
    env_path = args.env_file.resolve() if args.env_file else project_root / ".env"

    run_dbt_build(
        project_root=project_root,
        dbt_project_dir=dbt_project_dir,
        env_path=env_path,
    )


if __name__ == "__main__":
    main()
