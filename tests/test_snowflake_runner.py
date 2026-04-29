import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from manufacturing_pipeline.warehouse.snowflake_runner import (
    load_config,
    qualified_name,
    split_sql_statements,
)


class SnowflakeRunnerTest(unittest.TestCase):
    def test_split_sql_statements_ignores_semicolons_inside_strings(self):
        sql_text = """
        -- comment with a semicolon;
        SELECT 'a;b' AS value;
        SELECT "quoted;name" FROM table_name;
        """

        statements = split_sql_statements(sql_text)

        self.assertEqual(len(statements), 2)
        self.assertIn("SELECT 'a;b' AS value", statements[0])
        self.assertIn('SELECT "quoted;name" FROM table_name', statements[1])

    def test_load_config_reads_env_file_without_overwriting_existing_env(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            env_path = Path(tmp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "SNOWFLAKE_ACCOUNT=file_account",
                        "SNOWFLAKE_USER=file_user",
                        "SNOWFLAKE_PASSWORD=file_password",
                        "SNOWFLAKE_ROLE=file_role",
                        "SNOWFLAKE_WAREHOUSE=file_warehouse",
                        "SNOWFLAKE_DATABASE=file_database",
                        "SNOWFLAKE_SCHEMA=file_schema",
                    ]
                ),
                encoding="utf-8",
            )

            clear_keys = [
                "SNOWFLAKE_ACCOUNT",
                "SNOWFLAKE_USER",
                "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_ROLE",
                "SNOWFLAKE_WAREHOUSE",
                "SNOWFLAKE_DATABASE",
                "SNOWFLAKE_SCHEMA",
            ]
            with patch.dict(os.environ, {"SNOWFLAKE_USER": "env_user"}, clear=False):
                for key in clear_keys:
                    if key != "SNOWFLAKE_USER":
                        os.environ.pop(key, None)

                config = load_config(env_path)

        self.assertEqual(config.account, "file_account")
        self.assertEqual(config.user, "env_user")
        self.assertEqual(config.database, "file_database")

    def test_qualified_name_rejects_unsafe_identifiers(self):
        self.assertEqual(
            qualified_name("MANUFACTURING_DEFECTS", "RAW", "BOSCH_SAMPLE_STAGE"),
            "MANUFACTURING_DEFECTS.RAW.BOSCH_SAMPLE_STAGE",
        )

        with self.assertRaisesRegex(ValueError, "Invalid Snowflake"):
            qualified_name("MANUFACTURING_DEFECTS", "RAW;DROP", "STAGE")


if __name__ == "__main__":
    unittest.main()
