# Dean utils

This is a collection of funcs I use often and don't want to copy paste everywhere. They aren't necessarily functionally related and have hard coded env names that might probably don't make sense for others. It's public so that I can import it from various Azure Functions.

## Optional dependencies

Install only the features you need, for example
`pip install 'dean_utils[blobs,pyarrow]'`, or use `dean_utils[all]`.

- `blobs`: Azure blob storage, including async transport.
- `queues`: Azure queues, including async transport and managed identity.
- `email`: Azure Communication Services email.
- `postgres`: `to_db` with Polars and Psycopg (Psycopg also requires libpq).
- `httpx`: `global_async_client`.
- `pydantic`: Pydantic 2 model validation in `read_json`.
- `polars` and `pyarrow`: their respective libraries. Parquet operations on
  Polars data require both; blob Parquet operations also require `blobs`.
- `ducklake`: the DuckLake connection below.

## DuckLake connection

Install with `pip install dean_utils[ducklake]`, then:

```python
from dean_utils import ducklake_con

con = ducklake_con.con
```

Importing the module creates the connection, installs and loads the DuckLake,
Postgres and Azure extensions, and attaches the lake. `Synblob` is required for
the Azure secret. `DUPG_HOST`, `DUPG_PORT`, `DUPG_DB`, `DUPG_USER`, and
`DUPG_PW` are included in the PostgreSQL DSN only when present. The PostgreSQL
search path and DuckLake metadata schema use `DUPG_SCHEMA`, defaulting to
`ducklake`; the attached database name uses `DUPG_DDB_SCHEMA`, defaulting to
`lake`. TLS is required for the PostgreSQL connection.