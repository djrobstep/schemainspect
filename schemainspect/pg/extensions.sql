select
  nspname as schema,
  extname as name,
  extversion as version
from
    pg_extension
    INNER JOIN pg_namespace
        ON pg_namespace.oid=pg_extension.extnamespace
order by 1, 2
