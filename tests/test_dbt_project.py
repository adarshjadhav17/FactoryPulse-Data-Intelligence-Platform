import unittest
from pathlib import Path
from unittest.mock import patch

from manufacturing_pipeline.warehouse.dbt_runner import run_dbt_build

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "warehouse" / "dbt"


class DbtProjectTest(unittest.TestCase):
    def test_dbt_project_files_are_present(self):
        expected_files = [
            "dbt_project.yml",
            "profiles.yml",
            "macros/generate_schema_name.sql",
            "models/staging/sources.yml",
            "models/staging/schema.yml",
            "models/staging/stg_bosch_numeric.sql",
            "models/staging/stg_bosch_date.sql",
            "models/staging/stg_bosch_categorical.sql",
            "models/marts/schema.yml",
            "models/marts/mart_defect_features.sql",
        ]

        for relative_path in expected_files:
            self.assertTrue((DBT_DIR / relative_path).exists(), relative_path)

    def test_staging_models_reference_raw_sources(self):
        expected_sources = {
            "stg_bosch_numeric.sql": "source('raw_bosch', 'TRAIN_NUMERIC')",
            "stg_bosch_date.sql": "source('raw_bosch', 'TRAIN_DATE')",
            "stg_bosch_categorical.sql": "source('raw_bosch', 'TRAIN_CATEGORICAL')",
        }

        for file_name, source_reference in expected_sources.items():
            sql = (DBT_DIR / "models" / "staging" / file_name).read_text(
                encoding="utf-8"
            )
            self.assertIn(source_reference, sql)
            self.assertIn("ID as defect_id", sql)

    def test_mart_joins_all_staging_models(self):
        sql = (DBT_DIR / "models" / "marts" / "mart_defect_features.sql").read_text(
            encoding="utf-8"
        )

        expected_refs = [
            "ref('stg_bosch_numeric')",
            "ref('stg_bosch_date')",
            "ref('stg_bosch_categorical')",
        ]
        for ref in expected_refs:
            self.assertIn(ref, sql)

        self.assertIn("inner join date_features", sql)
        self.assertIn("inner join categorical_features", sql)
        self.assertIn("defect_response", sql)

    def test_custom_schema_macro_uses_exact_schema_names(self):
        macro = (DBT_DIR / "macros" / "generate_schema_name.sql").read_text(
            encoding="utf-8"
        )

        self.assertIn("macro generate_schema_name", macro)
        self.assertIn("custom_schema_name | trim", macro)

    def test_dbt_runner_uses_project_local_profiles_dir(self):
        with patch(
            "manufacturing_pipeline.warehouse.dbt_runner.load_dotenv"
        ) as load_dotenv, patch(
            "manufacturing_pipeline.warehouse.dbt_runner.subprocess.run"
        ) as run:
            run_dbt_build(
                project_root=PROJECT_ROOT,
                dbt_project_dir=DBT_DIR,
                env_path=PROJECT_ROOT / ".env",
            )

        load_dotenv.assert_called_once_with(PROJECT_ROOT / ".env")
        command = run.call_args.args[0]
        self.assertEqual(command[:2], ["dbt", "build"])
        self.assertIn("--project-dir", command)
        self.assertIn(str(DBT_DIR), command)
        self.assertIn("--profiles-dir", command)


if __name__ == "__main__":
    unittest.main()
