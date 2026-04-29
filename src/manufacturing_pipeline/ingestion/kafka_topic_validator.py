"""Validate Kafka topic messages without printing full wide payloads."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from manufacturing_pipeline.utils.datasets import DATASETS, dataset_names


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class ValidationResult:
    topic: str
    messages_checked: int
    invalid_messages: int
    sample_previews: list[dict[str, object]]


def validate_payload(
    raw_value: bytes | str, expected_dataset: str, preview_fields: int = 5
) -> tuple[bool, dict[str, object]]:
    if isinstance(raw_value, bytes):
        raw_text = raw_value.decode("utf-8")
    else:
        raw_text = raw_value

    try:
        message = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        return False, {"error": f"invalid_json: {exc.msg}"}

    payload = message.get("payload")
    if not isinstance(payload, dict):
        return False, {"error": "payload must be an object"}

    dataset = message.get("dataset")
    message_id = message.get("id")
    field_count = len(payload)
    expected_field_count = DATASETS[expected_dataset]["expected_field_count"]

    preview = {
        "dataset": dataset,
        "id": message_id,
        "field_count": field_count,
        "preview": dict(list(payload.items())[:preview_fields]),
    }

    is_valid = (
        dataset == expected_dataset
        and isinstance(message_id, str)
        and bool(message_id)
        and payload.get("Id") == message_id
        and field_count == expected_field_count
    )

    if not is_valid:
        preview["error"] = "schema_mismatch"

    return is_valid, preview


def validate_messages(
    raw_messages: Iterable[bytes | str],
    topic: str,
    expected_dataset: str,
    preview_count: int = 3,
) -> ValidationResult:
    messages_checked = 0
    invalid_messages = 0
    sample_previews: list[dict[str, object]] = []

    for raw_message in raw_messages:
        messages_checked += 1
        is_valid, preview = validate_payload(raw_message, expected_dataset)
        if not is_valid:
            invalid_messages += 1
        if len(sample_previews) < preview_count:
            sample_previews.append(preview)

    return ValidationResult(
        topic=topic,
        messages_checked=messages_checked,
        invalid_messages=invalid_messages,
        sample_previews=sample_previews,
    )


def consume_topic(
    topic: str,
    bootstrap_servers: str,
    limit: int,
    timeout_ms: int,
) -> list[bytes]:
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError(
            "Kafka consume requires kafka-python. Install dependencies from "
            "requirements.txt before validating Kafka topics."
        ) from exc

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        group_id=None,
    )

    messages: list[bytes] = []
    try:
        for message in consumer:
            messages.append(message.value)
            if len(messages) >= limit:
                break
    finally:
        consumer.close()

    return messages


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume and validate compact previews from Kafka topics."
    )
    parser.add_argument(
        "--dataset",
        choices=dataset_names(),
        default="numeric",
        help="Dataset topic to validate.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of Kafka messages to validate.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=5000,
        help="Kafka consumer timeout in milliseconds.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.limit <= 0:
        raise ValueError("--limit must be greater than zero")

    topic = DATASETS[args.dataset]["topic"]
    raw_messages = consume_topic(
        topic=topic,
        bootstrap_servers=args.bootstrap_servers,
        limit=args.limit,
        timeout_ms=args.timeout_ms,
    )
    result = validate_messages(raw_messages, topic=topic, expected_dataset=args.dataset)

    print(json.dumps(result.__dict__, indent=2))
    if result.messages_checked == 0:
        raise SystemExit("No messages found to validate")
    if result.invalid_messages:
        raise SystemExit(f"Found {result.invalid_messages} invalid messages")


if __name__ == "__main__":
    main()
