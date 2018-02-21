select
    nspname as schema
from
    pg_catalog.pg_namespace
-- SKIP_INTERNAL where nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
order by 1;
