with indexes as (
    select
        schemaname as schema,
        tablename as table_name,
        indexname as name,
        indexdef as definition,
        indexdef as create_statement
    FROM
        pg_indexes
        -- SKIP_INTERNAL where schemaname not in ('pg_catalog', 'information_schema', 'pg_toast')
        -- SKIP_INTERNAL and schemaname not like 'pg_temp_%' and schemaname not like 'pg_toast_temp_%'
    order by
        schemaname, tablename, indexname
)
select
    nspname as schema,
    conname as name,
    relname as table_name,
    pg_get_constraintdef(pg_constraint.oid) as definition,
    tc.constraint_type as constraint_type,
    i.name as index
from
    pg_constraint
    INNER JOIN pg_class
        ON conrelid=pg_class.oid
    INNER JOIN pg_namespace
        ON pg_namespace.oid=pg_class.relnamespace
    inner join information_schema.table_constraints tc
        on nspname = tc.constraint_schema
        and conname = tc.constraint_name
        and relname = tc.table_name
    left outer join indexes i
        on nspname = i.schema
        and conname = i.name
        and relname = i.table_name
    where true
  -- SKIP_INTERNAL and nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
order by 1, 3, 2;