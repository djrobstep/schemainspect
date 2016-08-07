with extension_oids as (
  select
      objid
  from
      pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass
)
SELECT n.nspname as "schema",
  pg_catalog.format_type(t.oid, NULL) AS "name",
  ARRAY(
     SELECT e.enumlabel
      FROM pg_catalog.pg_enum e
      WHERE e.enumtypid = t.oid
      ORDER BY e.enumsortorder
  ) as elements
FROM pg_catalog.pg_type t
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
     left outer join extension_oids e
       on t.oid = e.objid
WHERE
  t.typcategory = 'E'
  and e.objid is null
  -- SKIP_INTERNAL and n.nspname not in ('pg_catalog', 'information_schema')
  AND pg_catalog.pg_type_is_visible(t.oid)
ORDER BY 1, 2;
