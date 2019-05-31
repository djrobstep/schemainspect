SELECT
    n.nspname AS schema,
    c.relname AS table_name,
    i.relname AS name,
    i.oid as oid,
    pg_get_indexdef(i.oid) AS definition
  FROM pg_index x
    JOIN pg_class c ON c.oid = x.indrelid
    JOIN pg_class i ON i.oid = x.indexrelid
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind in ('r', 'm') AND i.relkind in ('i')
      -- SKIP_INTERNAL and nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
      -- SKIP_INTERNAL and nspname not like 'pg_temp_%' and nspname not like 'pg_toast_temp_%'
order by 1, 2, 3;