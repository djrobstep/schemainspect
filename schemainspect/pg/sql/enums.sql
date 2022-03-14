with extension_oids as (
  select
      objid
  from
      pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass and
      d.classid = 'pg_type'::regclass
)
SELECT
  n.nspname as "schema",
  t.typname as "name",
  ARRAY(
     SELECT e.enumlabel
      FROM pg_catalog.pg_enum e
      WHERE e.enumtypid = t.oid
      ORDER BY e.enumsortorder
  ) as elements,
  e.objid is not null as is_extension
FROM pg_catalog.pg_type t
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
     left outer join extension_oids e
       on t.oid = e.objid
WHERE
  t.typcategory = 'E'
  -- SKIP_INTERNAL and n.nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
  -- SKIP_INTERNAL and n.nspname not like 'pg_temp_%' and n.nspname not like 'pg_toast_temp_%'
ORDER BY 1, 2;
