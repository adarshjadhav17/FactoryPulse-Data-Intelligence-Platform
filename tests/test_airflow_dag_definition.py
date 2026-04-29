import ast
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = PROJECT_ROOT / "pipelines" / "airflow" / "dags" / "manufacturing_defect_pipeline.py"


def read_dag_tree() -> ast.Module:
    return ast.parse(DAG_PATH.read_text(encoding="utf-8"))


def task_assignments(tree: ast.Module) -> dict[str, ast.Call]:
    assignments = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        assignments[node.targets[0].id] = node.value
    return assignments


def keyword_value(call: ast.Call, keyword_name: str):
    for keyword in call.keywords:
        if keyword.arg == keyword_name and isinstance(keyword.value, ast.Constant):
            return keyword.value.value
    return None


class AirflowDagDefinitionTest(unittest.TestCase):
    def test_dag_file_is_valid_python(self):
        read_dag_tree()

    def test_dag_contains_expected_tasks_and_commands(self):
        assignments = task_assignments(read_dag_tree())
        expected_commands = {
            "profile_samples": "PYTHONPATH=src scripts/run_profile.sh 1000",
            "run_unit_tests": "PYTHONPATH=src python -m unittest discover -s tests",
            "kafka_smoke_check": "PYTHONPATH=src scripts/run_phase3_kafka_check.sh",
            "load_snowflake_raw": "PYTHONPATH=src scripts/run_snowflake_raw_load.sh",
            "build_dbt_models": "PYTHONPATH=src scripts/run_dbt_build.sh",
            "run_data_quality_checks": "PYTHONPATH=src scripts/run_data_quality_checks.sh",
        }

        for variable_name, command in expected_commands.items():
            with self.subTest(variable_name=variable_name):
                call = assignments[variable_name]
                self.assertEqual(keyword_value(call, "task_id"), variable_name)
                self.assertEqual(keyword_value(call, "bash_command"), command)

    def test_dag_is_manual_and_does_not_catch_up(self):
        source = DAG_PATH.read_text(encoding="utf-8")

        self.assertIn('dag_id="manufacturing_defect_sample_pipeline"', source)
        self.assertIn("schedule=None", source)
        self.assertIn("catchup=False", source)

    def test_dag_dependencies_are_linear_after_tests(self):
        source = DAG_PATH.read_text(encoding="utf-8")

        self.assertIn("start >> profile_samples >> run_unit_tests", source)
        self.assertIn(
            "run_unit_tests >> kafka_smoke_check >> load_snowflake_raw",
            source,
        )
        self.assertIn(
            "load_snowflake_raw >> build_dbt_models >> run_data_quality_checks >> finish",
            source,
        )


if __name__ == "__main__":
    unittest.main()
