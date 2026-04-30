import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ProjectPackagingTest(unittest.TestCase):
    def test_local_pipeline_script_runs_expected_steps(self):
        script = (PROJECT_ROOT / "scripts" / "run_local_pipeline.sh").read_text(
            encoding="utf-8"
        )

        expected_commands = [
            'scripts/run_profile.sh "${SAMPLE_SIZE}"',
            "PYTHONPATH=src python -m unittest discover -s tests",
            "scripts/run_phase3_kafka_check.sh",
            "scripts/run_snowflake_raw_load.sh",
            "scripts/run_dbt_build.sh",
            "scripts/run_data_quality_checks.sh",
        ]
        for command in expected_commands:
            self.assertIn(command, script)

        self.assertIn('RUN_KAFKA="${RUN_KAFKA:-0}"', script)

    def test_final_docs_are_present(self):
        expected_docs = [
            "docs/architecture.md",
            "docs/project_summary.md",
            "docs/phase_plan.md",
            "docs/folder_structure.md",
        ]

        for relative_path in expected_docs:
            self.assertTrue((PROJECT_ROOT / relative_path).exists(), relative_path)

    def test_readme_points_to_architecture_and_summary(self):
        readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")

        self.assertIn("docs/architecture.md", readme)
        self.assertIn("docs/project_summary.md", readme)
        self.assertIn("scripts/run_local_pipeline.sh", readme)


if __name__ == "__main__":
    unittest.main()
