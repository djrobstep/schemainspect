with extension_oids as (
  select
      objid
  from
      pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass
) SELECT n.nspname AS schema,
   c.relname AS table_name,
   i.relname AS name,
   i.oid as oid,
   e.objid as extension_oid,
   pg_get_indexdef(i.oid) AS definition
  FROM pg_index x
    JOIN pg_class c ON c.oid = x.indrelid
    JOIN pg_class i ON i.oid = x.indexrelid
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    left join extension_oids e
      on c.oid = e.objid or i.oid = e.objid
 WHERE c.relkind in ('r', 'm') AND i.relkind in ('i')
      -- SKIP_INTERNAL and nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
      -- SKIP_INTERNAL and e.objid is null
order by 1, 2, 3;
