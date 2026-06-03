from psycopg.sql import SQL

info_sql = SQL("""
               SELECT c.column_name,
    c.is_nullable,
    coalesce(e.data_type, c.data_type) AS data_type,
    c.data_type = 'ARRAY' as is_array,
    case
        when c.data_type = 'USER-DEFINED' then concat(c.udt_schema, '.', c.udt_name)
        else NULL
    end as user_type
FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN information_schema.element_types e ON (
        (
            c.table_catalog,
            c.table_schema,
            c.table_name,
            'TABLE',
            c.dtd_identifier
        ) = (
            e.object_catalog,
            e.object_schema,
            e.object_name,
            e.object_type,
            e.collection_type_identifier
        )
    )
WHERE c.table_schema = %s
    AND c.table_name = %s""")
