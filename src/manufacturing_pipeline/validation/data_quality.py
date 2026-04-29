"""Data quality checks across local samples, Snowflake raw tables, and dbt marts."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from manufacturing_pipeline.utils.datasets import DATASETS, dataset_names
from manufacturing_pipeline.warehouse.snowflake_runner import (
    DEFAULT_PROJECT_ROOT,
    connect,
    load_config,
    validate_identifier,
)


RAW_TABLES = {
    "numeric": "TRAIN_NUMERIC",
    "date": "TRAIN_DATE",
    "categorical": "TRAIN_CATEGORICAL",
}
MART_TABLE = "MARTS.MART_DEFECT_FEATURES"


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    details: dict[str, Any]


def load_profile_summary(project_root: Path) -> dict[str, Any]:
    profile_path = project_root / "data" / "profiling" / "profile_summary.json"
    if not profile_path.exists():
        raise FileNotFoundError(
            f"Missing profile summary: {profile_path}. Run scripts/run_profile.sh first."
        )
    return json.loads(profile_path.read_text(encoding="utf-8"))


def sample_shape(sample_path: Path) -> tuple[int, int]:
    with sample_path.open("r", newline="", encoding="utf-8") as file_obj:
        reader = csv.reader(file_obj)
        header = next(reader)
        row_count = sum(1 for _ in reader)
    return row_count, len(header)


def sample_response_distribution(sample_path: Path) -> dict[str, int]:
    with sample_path.open("r", newline="", encoding="utf-8") as file_obj:
        reader = csv.DictReader(file_obj)
        counts: dict[str, int] = {}
        for row in reader:
            value = row.get("Response", "")
            counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def local_sample_checks(project_root: Path, summary: dict[str, Any]) -> list[CheckResult]:
    expected_sample_size = int(summary["sample_size"])
    results: list[CheckResult] = []

    for dataset_name in dataset_names():
        sample_path = project_root / DATASETS[dataset_name]["sample_path"]
        row_count, sample_columns = sample_shape(sample_path)
        expected_columns = DATASETS[dataset_name]["expected_field_count"]
        profile_columns = summary["files"][dataset_name]["columns"]

        results.append(
            CheckResult(
                name=f"local_{dataset_name}_sample_shape",
                passed=row_count == expected_sample_size
                and sample_columns == expected_columns
                and profile_columns == expected_columns,
                details={
                    "rows": row_count,
                    "expected_rows": expected_sample_size,
                    "sample_columns": sample_columns,
                    "profile_columns": profile_columns,
                    "expected_columns": expected_columns,
                    "sample_path": str(sample_path),
                },
            )
        )

    response_distribution = sample_response_distribution(
        project_root / DATASETS["numeric"]["sample_path"]
    )
    results.append(
        CheckResult(
            name="local_response_distribution",
            passed=response_distribution == summary["response_distribution"],
            details={
                "sample_response_distribution": response_distribution,
                "profile_response_distribution": summary["response_distribution"],
            },
        )
    )
    results.append(
        CheckResult(
            name="local_sample_id_alignment",
            passed=bool(summary["id_alignment"]["numeric_date_match"])
            and bool(summary["id_alignment"]["numeric_categorical_match"]),
            details=summary["id_alignment"],
        )
    )

    return results


def fetch_one(cursor, query: str) -> tuple[Any, ...]:
    cursor.execute(query)
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"Query returned no rows: {query}")
    return row


def snowflake_checks(cursor, summary: dict[str, Any]) -> list[CheckResult]:
    expected_sample_size = int(summary["sample_size"])
    results: list[CheckResult] = []

    for dataset_name, table_name in RAW_TABLES.items():
        row_count, distinct_ids, null_ids = fetch_one(
            cursor,
            f"""
            SELECT COUNT(*), COUNT(DISTINCT ID), COUNT_IF(ID IS NULL)
            FROM RAW.{table_name}
            """,
        )
        column_count = fetch_one(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'RAW'
              AND TABLE_NAME = '{table_name}'
            """,
        )[0]
        expected_columns = DATASETS[dataset_name]["expected_field_count"]

        results.append(
            CheckResult(
                name=f"snowflake_raw_{dataset_name}_reconciliation",
                passed=row_count == expected_sample_size
                and distinct_ids == expected_sample_size
                and null_ids == 0
                and column_count == expected_columns,
                details={
                    "rows": row_count,
                    "distinct_ids": distinct_ids,
                    "null_ids": null_ids,
                    "columns": column_count,
                    "expected_rows": expected_sample_size,
                    "expected_columns": expected_columns,
                },
            )
        )

    numeric_rows, date_rows, categorical_rows, aligned_ids = fetch_one(
        cursor,
        """
        SELECT
          (SELECT COUNT(*) FROM RAW.TRAIN_NUMERIC),
          (SELECT COUNT(*) FROM RAW.TRAIN_DATE),
          (SELECT COUNT(*) FROM RAW.TRAIN_CATEGORICAL),
          (SELECT COUNT(*)
           FROM RAW.TRAIN_NUMERIC n
           INNER JOIN RAW.TRAIN_DATE d USING (ID)
           INNER JOIN RAW.TRAIN_CATEGORICAL c USING (ID))
        """,
    )
    results.append(
        CheckResult(
            name="snowflake_raw_id_alignment",
            passed=numeric_rows == date_rows == categorical_rows == aligned_ids,
            details={
                "numeric_rows": numeric_rows,
                "date_rows": date_rows,
                "categorical_rows": categorical_rows,
                "aligned_ids": aligned_ids,
            },
        )
    )

    response_zero, response_one, response_other = fetch_one(
        cursor,
        """
        SELECT
          COUNT_IF(defect_response = 0),
          COUNT_IF(defect_response = 1),
          COUNT_IF(defect_response NOT IN (0, 1) OR defect_response IS NULL)
        FROM MARTS.MART_DEFECT_FEATURES
        """,
    )
    expected_distribution = summary["response_distribution"]
    results.append(
        CheckResult(
            name="snowflake_mart_response_distribution",
            passed=response_zero == expected_distribution.get("0", 0)
            and response_one == expected_distribution.get("1", 0)
            and response_other == 0,
            details={
                "response_0": response_zero,
                "response_1": response_one,
                "response_other_or_null": response_other,
                "expected_distribution": expected_distribution,
            },
        )
    )

    mart_rows, mart_distinct_ids, mart_null_ids = fetch_one(
        cursor,
        f"""
        SELECT COUNT(*), COUNT(DISTINCT defect_id), COUNT_IF(defect_id IS NULL)
        FROM {MART_TABLE}
        """,
    )
    results.append(
        CheckResult(
            name="snowflake_mart_key_integrity",
            passed=mart_rows == expected_sample_size
            and mart_distinct_ids == expected_sample_size
            and mart_null_ids == 0,
            details={
                "rows": mart_rows,
                "distinct_ids": mart_distinct_ids,
                "null_ids": mart_null_ids,
                "expected_rows": expected_sample_size,
            },
        )
    )

    return results


def run_checks(project_root: Path, env_path: Path) -> list[CheckResult]:
    summary = load_profile_summary(project_root)
    results = local_sample_checks(project_root, summary)

    config = load_config(env_path)
    with connect(config) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                f"USE DATABASE {validate_identifier(config.database, 'database')}"
            )
            results.extend(snowflake_checks(cursor, summary))
        finally:
            cursor.close()

    return results


def print_results(results: list[CheckResult]) -> None:
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"{status} {result.name}: {json.dumps(result.details, sort_keys=True)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local and Snowflake data quality checks."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
        help="Project root containing samples, profile summary, and .env.",
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
    env_path = args.env_file.resolve() if args.env_file else project_root / ".env"

    results = run_checks(project_root=project_root, env_path=env_path)
    print_results(results)

    failed = [result.name for result in results if not result.passed]
    if failed:
        raise SystemExit(f"Data quality checks failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
