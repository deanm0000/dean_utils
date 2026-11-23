"""Create a DuckLake connection using PostgreSQL and Azure environment variables."""

import os

import duckdb


def _libpq_value(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


schema = os.environ.get("DUPG_SCHEMA", "ducklake")
alias = os.environ.get("DUPG_DDB_SCHEMA", "lake")
azure_connection_string = os.environ["Synblob"]

catalog_parts = [
    f"{key}={_libpq_value(os.environ[env])}"
    for env, key in (
        ("DUPG_HOST", "host"),
        ("DUPG_PORT", "port"),
        ("DUPG_DB", "dbname"),
        ("DUPG_USER", "user"),
        ("DUPG_PW", "password"),
    )
    if env in os.environ
]
search_path = _sql_identifier(schema)
search_path_option = "".join(
    "\\" + char if char.isspace() or char == "\\" else char for char in search_path
)
catalog_parts.extend(
    ("sslmode=require", f"options={_libpq_value('-c search_path=' + search_path_option)}")
)
catalog_dsn_sql = " ".join(catalog_parts).replace("'", "''")
meta_schema_sql = schema.replace("'", "''")

con = duckdb.connect()
for ext in ("ducklake", "postgres", "azure"):
    con.execute(f"INSTALL {ext}")
    con.execute(f"LOAD {ext}")

con.execute(
    """
    CREATE OR REPLACE SECRET az_store (
        TYPE azure,
        CONNECTION_STRING ?
    )
    """,
    [azure_connection_string],
)
con.execute(
    f"""
    ATTACH 'ducklake:postgres:{catalog_dsn_sql}' AS {_sql_identifier(alias)} (
        META_SCHEMA '{meta_schema_sql}'
    )
    """
)
con.execute("SET azure_transport_option_type = 'curl'")
