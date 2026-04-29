"""Stream CSV rows as JSON messages to Kafka, with a local dry-run mode."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from manufacturing_pipeline.utils.datasets import DATASETS, dataset_names


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class CsvMessage:
    dataset: str
    topic: str
    key: str
    value: dict[str, str]


def read_csv_messages(
    dataset: str, csv_path: Path, topic: str, limit: int | None = None
) -> Iterable[CsvMessage]:
    with csv_path.open("r", newline="", encoding="utf-8") as file_obj:
        reader = csv.DictReader(file_obj)
        if "Id" not in reader.fieldnames:
            raise ValueError(f"{csv_path} is missing required Id column")

        for index, row in enumerate(reader):
            if limit is not None and index >= limit:
                break
            yield CsvMessage(dataset=dataset, topic=topic, key=row["Id"], value=row)


def serialize_message(message: CsvMessage) -> bytes:
    payload = {
        "dataset": message.dataset,
        "id": message.key,
        "payload": message.value,
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def message_preview(message: CsvMessage, preview_fields: int = 5) -> dict[str, object]:
    return {
        "dataset": message.dataset,
        "id": message.key,
        "field_count": len(message.value),
        "preview": dict(list(message.value.items())[:preview_fields]),
    }


def dry_run(messages: Iterable[CsvMessage]) -> int:
    count = 0
    for message in messages:
        count += 1
        preview = json.dumps(message_preview(message), separators=(",", ":"))
        print(f"topic={message.topic} key={message.key} value={preview}")
    return count


def publish_to_kafka(messages: Iterable[CsvMessage], bootstrap_servers: str) -> int:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "Kafka publish requires kafka-python. Install dependencies from "
            "requirements.txt before running without --dry-run."
        ) from exc

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: value,
    )

    count = 0
    for message in messages:
        producer.send(
            message.topic,
            key=message.key,
            value=serialize_message(message),
        )
        count += 1

    producer.flush()
    producer.close()
    return count


def resolve_input_path(project_root: Path, dataset: str) -> Path:
    try:
        relative_path = DATASETS[dataset]["sample_path"]
    except KeyError as exc:
        valid = ", ".join(dataset_names())
        raise ValueError(f"Unknown dataset '{dataset}'. Expected one of: {valid}") from exc

    path = project_root / relative_path
    if not path.exists():
        raise FileNotFoundError(
            f"Missing sample file: {path}. Run scripts/run_profile.sh first."
        )
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce sample manufacturing CSV rows to Kafka topics."
    )
    parser.add_argument(
        "--dataset",
        choices=dataset_names(),
        default="numeric",
        help="Sample dataset to stream.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of rows to produce.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print messages instead of sending them to Kafka.",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
        help="Project root containing data/sample files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.limit <= 0:
        raise ValueError("--limit must be greater than zero")

    project_root = args.project_root.resolve()
    csv_path = resolve_input_path(project_root, args.dataset)
    topic = DATASETS[args.dataset]["topic"]
    messages = read_csv_messages(args.dataset, csv_path, topic, limit=args.limit)

    if args.dry_run:
        count = dry_run(messages)
    else:
        count = publish_to_kafka(messages, args.bootstrap_servers)

    print(f"Produced {count} {args.dataset} messages")


if __name__ == "__main__":
    main()
