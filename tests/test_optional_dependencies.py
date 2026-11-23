import os
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch


class OptionalDependencyTests(unittest.TestCase):
    def test_base_import_without_optional_dependencies(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
        result = subprocess.run(
            [
                sys.executable,
                "-S",
                "-B",
                "-c",
                "import dean_utils; assert callable(dean_utils.to_db)",
            ],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


class PostgreSQLDependencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_table_definition_imports_query_when_called(self):
        from dean_utils.utils.pl_to_db import _get_table_def

        query = object()
        info_sql = types.ModuleType("dean_utils.utils.info_sql")
        info_sql.info_sql = query
        polars = types.ModuleType("polars")
        polars.DataFrame = MagicMock()
        cursor = AsyncMock()
        cursor.fetchall.return_value = [("id", "NO", "integer", False, None)]
        cursor_context = MagicMock()
        cursor_context.__aenter__ = AsyncMock(return_value=cursor)
        cursor_context.__aexit__ = AsyncMock(return_value=False)
        conn = MagicMock()
        conn.cursor.return_value = cursor_context

        with patch.dict(
            sys.modules,
            {"dean_utils.utils.info_sql": info_sql, "polars": polars},
        ):
            result = await _get_table_def(conn, "example.items")

        cursor.execute.assert_awaited_once_with(query, ("example", "items"))
        polars.DataFrame.assert_called_once_with(
            cursor.fetchall.return_value,
            schema=["column_name", "is_nullable", "data_type", "is_array", "user_type"],
        )
        self.assertIs(result, polars.DataFrame.return_value)
