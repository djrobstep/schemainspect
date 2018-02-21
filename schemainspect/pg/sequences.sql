select
    sequence_schema as schema,
    sequence_name as name
from information_schema.sequences
-- SKIP_INTERNAL where sequence_schema not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
order by 1, 2;
