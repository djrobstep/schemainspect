select
        schemaname || '.' || indexname as fullname,
        schemaname as schema,
        tablename as table_name,
        indexname as name,
        indexdef as definition
    FROM
        pg_indexes
    order by
        schemaname, tablename, indexname
