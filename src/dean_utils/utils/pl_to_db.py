from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
    from psycopg import AsyncConnection
    from psycopg.sql import Composed
from dean_utils.utils.info_sql import info_sql


async def _get_table_def(conn: AsyncConnection, table_schema_name: str):
    import polars as pl

    if "." in table_schema_name:
        table_schema, table_name = table_schema_name.split(".", maxsplit=1)
    else:
        table_schema = "public"
        table_name = table_schema_name

    # table_def_sql = await deparameter_qry(
    #     conn, table_def_sql, (table_schema, table_name)
    # )
    async with conn.cursor() as cur:
        await cur.execute(info_sql, (table_schema, table_name))
        rows = await cur.fetchall()
    table_def = pl.DataFrame(
        rows,
        schema=["column_name", "is_nullable", "data_type", "is_array", "user_type"],
    )
    return table_def


async def _insert_via_temp_table(
    conn: AsyncConnection,
    df: pl.DataFrame,
    table_def: pl.DataFrame,
    target_table: str,
    upsert: Sequence[str] | None = None,
):
    import polars as pl
    from polars import col as c
    from polars import lit
    from psycopg.sql import SQL, Identifier

    if upsert is None:
        upsert = ()
    if "." in target_table:
        table_schema, table_name = target_table.split(".", maxsplit=1)
    else:
        table_schema = "public"
        table_name = target_table
    same_cols = [x for x in df.columns if x in table_def["column_name"]]
    table_def = table_def.filter(c.column_name.is_in(same_cols))
    df_schema = pl.DataFrame(
        [{"column_name": x, "pltype": str(y)} for x, y in df.schema.items()]
    )
    # print(df_schema)
    df = df.select(same_cols)
    create_table_lines = []
    copy_qry_elements = []
    select_qry_elements = []
    combined_schema = df_schema.join(table_def, on="column_name")
    combined_schema = combined_schema.with_columns(
        temp_type=pl.when(
            (c.pltype == "String") & (c.data_type.is_in(["character varying", "text"]))
        )
        .then(pl.struct(temp_type=c.data_type, sel_entry=lit(None)))
        .when((c.pltype == "String") & (c.data_type == "uuid"))
        .then(pl.struct(temp_type=lit("text"), sel_entry=c.data_type))
        .when((c.pltype == "String") & (c.data_type == "timestamp with time zone"))
        .then(
            pl.struct(temp_type=lit("text"), sel_entry=lit("timestamp with time zone"))
        )
        .when((c.pltype == "Float64") & (c.data_type == "double precision"))
        .then(pl.struct(temp_type=c.data_type, sel_entry=lit(None)))
        .when(c.pltype.str.contains("Int") & (c.data_type == lit("integer")))
        .then(pl.struct(temp_type=c.data_type, sel_entry=lit(None)))
        .when((c.pltype == "String") & (c.data_type == "USER-DEFINED"))
        .then(pl.struct(temp_type=lit("text"), sel_entry=c.user_type))
        .when((c.pltype == "Date") & (c.data_type == "date"))
        .then(pl.struct(temp_type=lit("date"), sel_entry=lit(None)))
        .when(
            (c.pltype.str.starts_with("Datetime"))
            & (c.data_type == "timestamp with time zone")
        )
        .then(pl.struct(temp_type=c.data_type, sel_entry=lit(None)))
        .otherwise(pl.struct(temp_type=c.data_type, sel_entry=lit(None))),
        nullability=pl.when(c.is_nullable == "NO")
        .then(lit("NOT NULL"))
        .otherwise(lit("")),
    ).unnest("temp_type")
    # print(combined_schema)
    df = df.select(combined_schema["column_name"].to_list())
    update_excluded: list[Composed] = []
    # assert_series_equal(combined_schema["column_name"], pl.Series("column_name", df.columns))
    for col_name, temp_type, nullability, sel_entry in combined_schema.select(
        "column_name", "temp_type", "nullability", "sel_entry"
    ).iter_rows():
        if col_name not in upsert and len(upsert) > 0:
            update_excluded.append(
                SQL("").join(
                    [Identifier(col_name), SQL("=EXCLUDED."), Identifier(col_name)]
                )
            )
        create_table_lines.append(
            SQL(" ").join([Identifier(col_name), SQL(temp_type), SQL(nullability)])
        )
        copy_qry_elements.append(Identifier(col_name))
        if sel_entry is None:
            select_qry_elements.append(Identifier(col_name))
        else:
            select_qry_elements.append(
                SQL("::").join([Identifier(col_name), SQL(sel_entry)])
            )
    col_list = SQL("").join([SQL("("), SQL(", ").join(copy_qry_elements), SQL(")")])
    copy_qry = SQL("").join([SQL("COPY abcd "), col_list, SQL(" FROM STDIN")])
    # print(copy_qry.as_string())
    create_table_sql = SQL("\n").join(
        [
            SQL("CREATE TEMPORARY TABLE abcd ("),
            SQL(",\n").join(create_table_lines),
            SQL(")"),
        ]
    )
    # print(create_table_sql.as_string())
    async with conn.transaction():
        await conn.execute(create_table_sql)
        async with conn.cursor() as cur, cur.copy(copy_qry) as cp:
            for row in df.iter_rows():
                await cp.write_row(row)

        ins_qry = [
            SQL("INSERT INTO {}.{}").format(
                Identifier(table_schema), Identifier(table_name)
            ),
            col_list,
            SQL("SELECT "),
            SQL(", ").join(select_qry_elements),
            SQL("FROM abcd"),
        ]

        if len(upsert) > 0:
            upsert_sql = SQL(",").join([Identifier(x) for x in upsert])
            update_excluded_sql = SQL(",\n").join(update_excluded)
            ins_qry.extend(
                [
                    SQL("").join([SQL("ON CONFLICT ("), upsert_sql, SQL(")")]),
                    SQL("DO UPDATE"),
                    SQL("SET"),
                ]
            )
            ins_qry.append(update_excluded_sql)
        ins_qry = SQL("\n").join(ins_qry)
        try:
            await conn.execute(ins_qry)
        except Exception:
            print(ins_qry.strings)
            raise


async def to_db(
    conn: AsyncConnection,
    df: pl.DataFrame,
    target_table: str,
    upsert: tuple[str] | None = None,
) -> None:
    """
    Sends a polars DataFrame to a PostgreSQL table.

    Args:
        conn (AsyncConnection): Async connection to the PostgreSQL database.
        df (pl.DataFrame): DataFrame to insert into the database.
        target_table (str): Target table name (format: 'schema.table' or 'table').
        upsert (tuple[str] | None, optional): Column names to use as conflict keys for upsert
            operations. Defaults to None.
    """
    table_def = await _get_table_def(conn, target_table)
    await _insert_via_temp_table(conn, df, table_def, target_table, upsert)
