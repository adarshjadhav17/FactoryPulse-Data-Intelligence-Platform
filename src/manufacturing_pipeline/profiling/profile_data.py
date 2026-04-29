"""Profile local Bosch manufacturing CSV files and create small samples."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[3]

SOURCE_FILE_NAMES = {
    "numeric": "train_numeric.csv",
    "date": "train_date.csv",
    "categorical": "train_categorical.csv",
}


@dataclass
class FileProfile:
    name: str
    path: str
    exists: bool
    size_bytes: int | None
    columns: int | None
    sample_rows: int
    total_rows: int | None = None


def read_header(path: Path) -> list[str]:
    with path.open("r", newline="", encoding="utf-8") as file_obj:
        reader = csv.reader(file_obj)
        return next(reader)


def count_data_rows(path: Path) -> int:
    with path.open("rb") as file_obj:
        line_count = sum(1 for _ in file_obj)
    return max(line_count - 1, 0)


def iter_rows(path: Path, limit: int | None = None) -> Iterable[list[str]]:
    with path.open("r", newline="", encoding="utf-8") as file_obj:
        reader = csv.reader(file_obj)
        next(reader)
        for index, row in enumerate(reader):
            if limit is not None and index >= limit:
                break
            yield row


def write_sample(source_path: Path, output_path: Path, sample_size: int) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0

    with source_path.open("r", newline="", encoding="utf-8") as source, output_path.open(
        "w", newline="", encoding="utf-8"
    ) as target:
        reader = csv.reader(source)
        writer = csv.writer(target)

        header = next(reader)
        writer.writerow(header)

        for row in reader:
            if rows_written >= sample_size:
                break
            writer.writerow(row)
            rows_written += 1

    return rows_written


def profile_file(name: str, path: Path, sample_size: int, full_scan: bool) -> FileProfile:
    if not path.exists():
        return FileProfile(
            name=name,
            path=str(path),
            exists=False,
            size_bytes=None,
            columns=None,
            sample_rows=0,
        )

    header = read_header(path)
    profile = FileProfile(
        name=name,
        path=str(path),
        exists=True,
        size_bytes=path.stat().st_size,
        columns=len(header),
        sample_rows=sample_size,
    )

    if full_scan:
        profile.total_rows = count_data_rows(path)

    return profile


def response_distribution(path: Path, limit: int | None = None) -> dict[str, int]:
    header = read_header(path)
    response_index = header.index("Response")
    counts: Counter[str] = Counter()

    for row in iter_rows(path, limit=limit):
        response_value = row[response_index] if response_index < len(row) else ""
        counts[response_value] += 1

    return dict(sorted(counts.items()))


def sample_ids(path: Path, sample_size: int) -> list[str]:
    ids: list[str] = []
    for row in iter_rows(path, limit=sample_size):
        ids.append(row[0])
    return ids


def build_source_files(project_root: Path) -> dict[str, Path]:
    return {
        name: project_root / file_name for name, file_name in SOURCE_FILE_NAMES.items()
    }


def validate_source_files(source_files: dict[str, Path]) -> None:
    missing_files = [str(path) for path in source_files.values() if not path.exists()]
    if missing_files:
        missing_list = "\n".join(f"- {path}" for path in missing_files)
        raise FileNotFoundError(f"Missing required source files:\n{missing_list}")

    for name, path in source_files.items():
        header = read_header(path)
        if "Id" not in header:
            raise ValueError(f"{path} is missing required Id column")
        if name == "numeric" and "Response" not in header:
            raise ValueError(f"{path} is missing required Response column")


def id_alignment(source_files: dict[str, Path], sample_size: int) -> dict[str, object]:
    id_sets = {
        name: sample_ids(path, sample_size) for name, path in source_files.items()
    }
    numeric_ids = id_sets["numeric"]

    return {
        "sample_size": sample_size,
        "numeric_date_match": numeric_ids == id_sets["date"],
        "numeric_categorical_match": numeric_ids == id_sets["categorical"],
        "first_ids": numeric_ids[:10],
    }


def sampled_null_summary(
    source_files: dict[str, Path], sample_size: int
) -> dict[str, dict[str, object]]:
    summary = {}

    for name, path in source_files.items():
        header = read_header(path)
        empty_counts = [0] * len(header)
        rows_scanned = 0

        for row in iter_rows(path, limit=sample_size):
            rows_scanned += 1
            for index in range(len(header)):
                value = row[index] if index < len(row) else ""
                if value == "":
                    empty_counts[index] += 1

        columns_with_nulls = sum(1 for count in empty_counts if count > 0)
        total_cells = rows_scanned * len(header)
        total_nulls = sum(empty_counts)

        summary[name] = {
            "rows_scanned": rows_scanned,
            "columns_scanned": len(header),
            "columns_with_nulls": columns_with_nulls,
            "total_nulls": total_nulls,
            "null_cell_ratio": round(total_nulls / total_cells, 6)
            if total_cells
            else None,
        }

    return summary


def build_summary(
    source_files: dict[str, Path], sample_size: int, full_scan: bool
) -> dict[str, object]:
    profiles = {
        name: asdict(profile_file(name, path, sample_size, full_scan))
        for name, path in source_files.items()
    }

    response_limit = None if full_scan else sample_size

    return {
        "sample_size": sample_size,
        "full_scan": full_scan,
        "files": profiles,
        "id_alignment": id_alignment(source_files, sample_size),
        "sampled_null_summary": sampled_null_summary(source_files, sample_size),
        "response_distribution": response_distribution(
            source_files["numeric"], limit=response_limit
        ),
    }


def create_samples(
    project_root: Path, source_files: dict[str, Path], sample_size: int
) -> dict[str, dict[str, object]]:
    outputs = {}
    sample_dir = project_root / "data" / "sample"

    for name, source_path in source_files.items():
        output_path = sample_dir / f"train_{name}_sample.csv"
        rows_written = write_sample(source_path, output_path, sample_size)
        outputs[name] = {"path": str(output_path), "rows_written": rows_written}
        if rows_written != sample_size:
            raise RuntimeError(
                f"Expected {sample_size} rows for {name}, wrote {rows_written}"
            )

    return outputs


def write_summary(project_root: Path, summary: dict[str, object]) -> Path:
    output_path = project_root / "data" / "profiling" / "profile_summary.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile local manufacturing CSVs and generate sample files."
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of data rows to write into each sample file.",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Scan full files for exact row counts and full Response distribution.",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
        help="Directory containing the raw train CSV files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be greater than zero")

    project_root = args.project_root.resolve()
    source_files = build_source_files(project_root)
    validate_source_files(source_files)

    sample_outputs = create_samples(project_root, source_files, args.sample_size)
    summary = build_summary(source_files, args.sample_size, args.full_scan)
    summary["sample_outputs"] = sample_outputs
    summary_path = write_summary(project_root, summary)

    print(f"Wrote profile summary: {summary_path}")
    for name, output in sample_outputs.items():
        print(f"Wrote {name} sample: {output['path']}")


if __name__ == "__main__":
    main()
