import csv
import json
import tempfile
import unittest
from pathlib import Path

from manufacturing_pipeline.ingestion.kafka_csv_producer import (
    message_preview,
    read_csv_messages,
    serialize_message,
)


class KafkaCsvProducerTest(unittest.TestCase):
    def test_read_csv_messages_serializes_rows_with_id_key(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "sample.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as file_obj:
                writer = csv.writer(file_obj)
                writer.writerow(["Id", "feature", "Response"])
                writer.writerow(["10", "0.25", "0"])
                writer.writerow(["11", "", "1"])

            messages = list(
                read_csv_messages(
                    dataset="numeric",
                    csv_path=csv_path,
                    topic="bosch.train.numeric",
                    limit=1,
                )
            )

            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].key, "10")
            self.assertEqual(messages[0].topic, "bosch.train.numeric")

            payload = json.loads(serialize_message(messages[0]).decode("utf-8"))
            self.assertEqual(payload["dataset"], "numeric")
            self.assertEqual(payload["id"], "10")
            self.assertEqual(payload["payload"]["feature"], "0.25")

            preview = message_preview(messages[0], preview_fields=2)
            self.assertEqual(preview["field_count"], 3)
            self.assertEqual(preview["preview"], {"Id": "10", "feature": "0.25"})


if __name__ == "__main__":
    unittest.main()
