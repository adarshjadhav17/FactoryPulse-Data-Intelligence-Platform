import csv
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from manufacturing_pipeline.validation import data_quality
from manufacturing_pipeline.validation.data_quality import (
    CheckResult,
    local_sample_checks,
    print_results,
    sample_shape,
    snowflake_checks,
)


class FakeCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.queries = []

    def execute(self, query):
        self.queries.append(query)

    def fetchone(self):
        return self.rows.pop(0)


class DataQualityTest(unittest.TestCase):
    def test_local_sample_checks_pass_for_matching_profile_summary(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir)
            self._write_csv(
                project_root / "data/sample/train_numeric_sample.csv",
                ["Id", "feature", "Response"],
                [["1", "0.1", "0"], ["2", "0.2", "1"]],
            )
            self._write_csv(
                project_root / "data/sample/train_date_sample.csv",
                ["Id", "date_feature"],
                [["1", "10.0"], ["2", "11.0"]],
            )
            self._write_csv(
                project_root / "data/sample/train_categorical_sample.csv",
                ["Id", "category_feature"],
                [["1", "A"], ["2", "B"]],
            )
            with self._small_dataset_metadata():
                summary = {
                    "sample_size": 2,
                    "files": {
                        "numeric": {"columns": 3},
                        "date": {"columns": 2},
                        "categorical": {"columns": 2},
                    },
                    "response_distribution": {"0": 1, "1": 1},
                    "id_alignment": {
                        "numeric_date_match": True,
                        "numeric_categorical_match": True,
                    },
                }

                results = local_sample_checks(project_root, summary)

        self.assertTrue(all(result.passed for result in results))

    def test_sample_shape_counts_header_columns_and_data_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            sample_path = Path(tmp_dir) / "sample.csv"
            self._write_csv(
                sample_path,
                ["Id", "feature", "Response"],
                [["1", "0.1", "0"], ["2", "0.2", "1"]],
            )

            rows, columns = sample_shape(sample_path)

        self.assertEqual(rows, 2)
        self.assertEqual(columns, 3)

    def test_local_sample_checks_fail_on_sample_column_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir)
            self._write_csv(
                project_root / "data/sample/train_numeric_sample.csv",
                ["Id", "Response"],
                [["1", "0"]],
            )
            self._write_csv(
                project_root / "data/sample/train_date_sample.csv",
                ["Id", "date_feature"],
                [["1", "10.0"]],
            )
            self._write_csv(
                project_root / "data/sample/train_categorical_sample.csv",
                ["Id", "category_feature"],
                [["1", "A"]],
            )
            with self._small_dataset_metadata():
                summary = {
                    "sample_size": 1,
                    "files": {
                        "numeric": {"columns": 3},
                        "date": {"columns": 2},
                        "categorical": {"columns": 2},
                    },
                    "response_distribution": {"0": 1},
                    "id_alignment": {
                        "numeric_date_match": True,
                        "numeric_categorical_match": True,
                    },
                }

                results = local_sample_checks(project_root, summary)

        numeric_shape = next(
            result for result in results if result.name == "local_numeric_sample_shape"
        )
        self.assertFalse(numeric_shape.passed)
        self.assertEqual(numeric_shape.details["sample_columns"], 2)

    def test_local_sample_checks_fail_on_response_distribution_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir)
            for name in ["numeric", "date", "categorical"]:
                file_name = f"train_{name}_sample.csv"
                header = ["Id", "feature", "Response"] if name == "numeric" else ["Id", "feature"]
                rows = [["1", "feature", "0"]] if name == "numeric" else [["1", "feature"]]
                self._write_csv(project_root / "data/sample" / file_name, header, rows)
            with self._small_dataset_metadata():
                summary = {
                    "sample_size": 1,
                    "files": {
                        "numeric": {"columns": 3},
                        "date": {"columns": 2},
                        "categorical": {"columns": 2},
                    },
                    "response_distribution": {"1": 1},
                    "id_alignment": {
                        "numeric_date_match": True,
                        "numeric_categorical_match": True,
                    },
                }

                results = local_sample_checks(project_root, summary)

        response_check = next(
            result for result in results if result.name == "local_response_distribution"
        )
        self.assertFalse(response_check.passed)

    def _small_dataset_metadata(self):
        small_metadata = {
            "numeric": {
                "sample_path": "data/sample/train_numeric_sample.csv",
                "topic": "bosch.train.numeric",
                "expected_field_count": 3,
            },
            "date": {
                "sample_path": "data/sample/train_date_sample.csv",
                "topic": "bosch.train.date",
                "expected_field_count": 2,
            },
            "categorical": {
                "sample_path": "data/sample/train_categorical_sample.csv",
                "topic": "bosch.train.categorical",
                "expected_field_count": 2,
            },
        }
        return patch.dict(data_quality.DATASETS, small_metadata, clear=True)

    @staticmethod
    def _write_csv(path, header, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as file_obj:
            writer = csv.writer(file_obj)
            writer.writerow(header)
            writer.writerows(rows)


class SnowflakeDataQualityTest(unittest.TestCase):
    def test_snowflake_checks_pass_for_matching_counts(self):
        rows = [
            (2, 2, 0),
            (970,),
            (2, 2, 0),
            (1157,),
            (2, 2, 0),
            (2141,),
            (2, 2, 2, 2),
            (1, 1, 0),
            (2, 2, 0),
        ]
        summary = {
            "sample_size": 2,
            "response_distribution": {"0": 1, "1": 1},
        }

        results = snowflake_checks(FakeCursor(rows), summary)

        self.assertTrue(all(result.passed for result in results))

    def test_print_results_marks_failures(self):
        result = CheckResult(
            name="example_check",
            passed=False,
            details={"rows": 0, "expected_rows": 1},
        )

        with patch("sys.stdout", new_callable=StringIO) as stdout:
            print_results([result])

        self.assertIn("FAIL example_check", stdout.getvalue())
        self.assertIn('"expected_rows": 1', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
