import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import dean_utils


class DuckLakeConnectionTests(unittest.TestCase):
    def setUp(self):
        self.duckdb = types.ModuleType("duckdb")
        self.duckdb.connect = MagicMock()
        self.con = self.duckdb.connect.return_value
        self.addCleanup(self.remove_module)

    @staticmethod
    def remove_module():
        sys.modules.pop("dean_utils.ducklake_con", None)
        if hasattr(dean_utils, "ducklake_con"):
            delattr(dean_utils, "ducklake_con")

    def load_module(self, env):
        self.remove_module()
        with patch.dict(os.environ, env, clear=True):
            with patch.dict(sys.modules, {"duckdb": self.duckdb}):
                from dean_utils import ducklake_con

                return ducklake_con

    def test_defaults_and_setup(self):
        module = self.load_module({"Synblob": "azure-secret"})
        self.assertIs(module.con, self.con)
        self.duckdb.connect.assert_called_once_with()
        calls = self.con.execute.call_args_list
        self.assertEqual(
            [call.args[0] for call in calls[:6]],
            [
                "INSTALL ducklake",
                "LOAD ducklake",
                "INSTALL postgres",
                "LOAD postgres",
                "INSTALL azure",
                "LOAD azure",
            ],
        )
        self.assertEqual(calls[6].args[1], ["azure-secret"])
        self.assertIn("CREATE OR REPLACE SECRET az_store", calls[6].args[0])
        self.assertIn("sslmode=require", calls[7].args[0])
        self.assertIn('search_path="ducklake"', calls[7].args[0])
        self.assertIn(' AS "lake"', calls[7].args[0])
        self.assertIn("META_SCHEMA 'ducklake'", calls[7].args[0])
        self.assertNotIn("host=", calls[7].args[0])
        self.assertNotIn("password=", calls[7].args[0])
        self.assertEqual(calls[8].args[0], "SET azure_transport_option_type = 'curl'")

    def test_configured_values_are_escaped(self):
        module = self.load_module(
            {
                "Synblob": "azure-secret",
                "DUPG_HOST": "db host",
                "DUPG_PORT": "5432",
                "DUPG_DB": "data",
                "DUPG_USER": "dean",
                "DUPG_PW": "pa's\\word",
                "DUPG_SCHEMA": 'odd schema"\'',
                "DUPG_DDB_SCHEMA": 'lake"name',
            }
        )
        self.assertEqual(
            module.catalog_parts[:5],
            [
                "host='db host'",
                "port='5432'",
                "dbname='data'",
                "user='dean'",
                "password='pa\\'s\\\\word'",
            ],
        )
        sql = self.con.execute.call_args_list[7].args[0]
        self.assertIn(' AS "lake""name"', sql)
        self.assertIn("META_SCHEMA 'odd schema\"'''", sql)
        self.assertIn("password=''pa\\''s\\\\word''", sql)
        self.assertIn('search_path="odd\\\\ schema""', sql)

    def test_partial_configuration_omits_missing_fields(self):
        module = self.load_module({"Synblob": "secret", "DUPG_DB": "data"})
        self.assertEqual(module.catalog_parts[0], "dbname='data'")
        for key in ("host", "port", "user", "password"):
            self.assertFalse(
                any(part.startswith(f"{key}=") for part in module.catalog_parts)
            )

    def test_missing_azure_secret_fails_before_connecting(self):
        with self.assertRaisesRegex(KeyError, "Synblob"):
            self.load_module({})
        self.duckdb.connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
