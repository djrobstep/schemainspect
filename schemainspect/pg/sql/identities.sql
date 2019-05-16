select
    table_schema,
    table_name,
    column_name,
    identity_generation,
from
    information_schema.columns
where
    is_identity = 'YES'
    -- SKIP_INTERNAL and table_schema not in ('pg_catalog', 'information_schema', 'pg_toast')
    -- SKIP_INTERNAL and table_schema not like 'pg_temp_%' and schemaname not like 'pg_toast_temp_%'
