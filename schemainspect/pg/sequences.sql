select
    sequence_schema as schema,
    sequence_name as name
from information_schema.sequences
-- SKIP_INTERNAL where sequence_schema not in ('pg_catalog', 'information_schema')
order by 1, 2;
