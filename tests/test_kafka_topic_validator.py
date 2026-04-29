import json
import unittest

from manufacturing_pipeline.ingestion.kafka_topic_validator import (
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


if __name__ == "__main__":
    unittest.main()
