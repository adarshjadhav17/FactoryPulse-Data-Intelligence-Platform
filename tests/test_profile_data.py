import csv
import tempfile
import unittest
from pathlib import Path

from manufacturing_pipeline.profiling.profile_data import (
    build_source_files,
    build_summary,
    create_samples,
    response_distribution,
    validate_source_files,
)


class ProfileDataTest(unittest.TestCase):
    def test_build_summary_and_samples_from_small_csvs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir)
            self._write_csv(
                project_root / "train_numeric.csv",
                ["Id", "L0_S0_F0", "Response"],
                [["1", "0.1", "0"], ["2", "", "1"]],
            )
            self._write_csv(
                project_root / "train_date.csv",
                ["Id", "L0_S0_D1"],
                [["1", "10.0"], ["2", ""]],
            )
            self._write_csv(
                project_root / "train_categorical.csv",
                ["Id", "L0_S1_F25"],
                [["1", ""], ["2", "A"]],
            )

            source_files = build_source_files(project_root)
            validate_source_files(source_files)

            samples = create_samples(project_root, source_files, sample_size=2)
            summary = build_summary(source_files, sample_size=2, full_scan=True)

            self.assertEqual(samples["numeric"]["rows_written"], 2)
            self.assertEqual(summary["files"]["numeric"]["columns"], 3)
            self.assertEqual(summary["files"]["numeric"]["total_rows"], 2)
            self.assertTrue(summary["id_alignment"]["numeric_date_match"])
            self.assertTrue(summary["id_alignment"]["numeric_categorical_match"])
            self.assertEqual(summary["response_distribution"], {"0": 1, "1": 1})
            self.assertEqual(
                summary["sampled_null_summary"]["numeric"]["total_nulls"], 1
            )

    def test_validate_source_files_requires_id_and_response(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_root = Path(tmp_dir)
            self._write_csv(
                project_root / "train_numeric.csv",
                ["Id", "L0_S0_F0"],
                [["1", "0.1"]],
            )
            self._write_csv(project_root / "train_date.csv", ["Id"], [["1"]])
            self._write_csv(project_root / "train_categorical.csv", ["Id"], [["1"]])

            with self.assertRaisesRegex(ValueError, "Response"):
                validate_source_files(build_source_files(project_root))

    def test_response_distribution_handles_short_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "train_numeric.csv"
            with csv_path.open("w", encoding="utf-8") as file_obj:
                file_obj.write("Id,feature,Response\n")
                file_obj.write("1,0.1,0\n")
                file_obj.write("2,0.2\n")

            self.assertEqual(response_distribution(csv_path), {"": 1, "0": 1})

    @staticmethod
    def _write_csv(path, header, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as file_obj:
            writer = csv.writer(file_obj)
            writer.writerow(header)
            writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
