import json
import unittest
from io import StringIO
from unittest.mock import patch

from manufacturing_pipeline.ingestion.kafka_topic_validator import (
    main,
    validate_messages,
    validate_payload,
)
from manufacturing_pipeline.utils.datasets import DATASETS


class KafkaTopicValidatorTest(unittest.TestCase):
    def test_validate_payload_accepts_matching_numeric_message(self):
        payload = {"Id": "10"}
        for index in range(DATASETS["numeric"]["expected_field_count"] - 1):
            payload[f"f_{index}"] = str(index)

        raw_message = json.dumps(
            {"dataset": "numeric", "id": "10", "payload": payload}
        )

        is_valid, preview = validate_payload(raw_message, "numeric")

        self.assertTrue(is_valid)
        self.assertEqual(preview["id"], "10")
        self.assertEqual(
            preview["field_count"], DATASETS["numeric"]["expected_field_count"]
        )

    def test_validate_payload_rejects_schema_mismatch(self):
        raw_message = json.dumps(
            {"dataset": "numeric", "id": "10", "payload": {"Id": "11"}}
        )

        is_valid, preview = validate_payload(raw_message, "numeric")

        self.assertFalse(is_valid)
        self.assertEqual(preview["error"], "schema_mismatch")

    def test_validate_messages_counts_invalid_messages(self):
        result = validate_messages(
            raw_messages=["not-json"],
            topic="bosch.train.numeric",
            expected_dataset="numeric",
        )

        self.assertEqual(result.messages_checked, 1)
        self.assertEqual(result.invalid_messages, 1)

    def test_main_fails_when_expected_message_count_is_not_available(self):
        argv = [
            "kafka_topic_validator",
            "--dataset",
            "numeric",
            "--limit",
            "2",
        ]

        with patch("sys.argv", argv), patch("sys.stdout", new_callable=StringIO), patch(
            "manufacturing_pipeline.ingestion.kafka_topic_validator.consume_topic",
            return_value=[],
        ):
            with self.assertRaises(SystemExit) as context:
                main()

        self.assertEqual(str(context.exception), "Expected 2 messages, found 0")


if __name__ == "__main__":
    unittest.main()
