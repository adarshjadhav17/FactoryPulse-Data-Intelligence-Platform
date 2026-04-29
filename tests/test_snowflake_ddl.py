import unittest
from pathlib import Path

from manufacturing_pipeline.utils.datasets import DATASETS


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DDL_DIR = PROJECT_ROOT / "warehouse" / "snowflake" / "ddl"


class SnowflakeDdlTest(unittest.TestCase):
    def test_phase4_ddl_files_are_present(self):
        expected_files = [
            "001_create_raw_objects.sql",
            "002_put_sample_files.sql",
            "003_create_raw_tables.sql",
            "004_copy_sample_data.sql",
            "005_validate_raw_load.sql",
        ]

        for file_name in expected_files:
            self.assertTrue((DDL_DIR / file_name).exists(), file_name)

    def test_raw_table_ddl_covers_all_datasets(self):
        raw_tables_sql = (DDL_DIR / "003_create_raw_tables.sql").read_text(
            encoding="utf-8"
        )
        copy_sql = (DDL_DIR / "004_copy_sample_data.sql").read_text(encoding="utf-8")

        for dataset_name in DATASETS:
            table_name = f"TRAIN_{dataset_name.upper()}"
            sample_file = f"train_{dataset_name}_sample.csv.gz"
            self.assertIn(table_name, raw_tables_sql)
            self.assertIn(sample_file, raw_tables_sql)
            self.assertIn(table_name, copy_sql)
            self.assertIn(sample_file, copy_sql)

    def test_raw_table_ddl_uses_schema_inference(self):
        raw_tables_sql = (DDL_DIR / "003_create_raw_tables.sql").read_text(
            encoding="utf-8"
        )

        self.assertIn("USING TEMPLATE", raw_tables_sql)
        self.assertIn("INFER_SCHEMA", raw_tables_sql)
        self.assertIn("WITHIN GROUP (ORDER BY ORDER_ID)", raw_tables_sql)


if __name__ == "__main__":
    unittest.main()
