with extension_oids as (
  select
      objid
  from
      pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass
), enums as (

  SELECT
    t.oid as enum_oid,
    n.nspname as "schema",
    pg_catalog.format_type(t.oid, NULL) AS "name"
  FROM pg_catalog.pg_type t
       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
       left outer join extension_oids e
         on t.oid = e.objid
  WHERE
    t.typcategory = 'E'
    and e.objid is null
    -- SKIP_INTERNAL and n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    AND pg_catalog.pg_type_is_visible(t.oid)
  ORDER BY 1, 2
),
r as (
    select
        c.relname as name,
        n.nspname as schema,
        c.relkind as relationtype,
        c.oid as oid,
        case when c.relkind in ('m', 'v') then
          pg_get_viewdef(c.oid)
        else null end
          as definition
    from
        pg_catalog.pg_class c
        inner join pg_catalog.pg_namespace n
          ON n.oid = c.relnamespace
        left outer join extension_oids e
          on c.oid = e.objid
    where c.relkind in ('r', 'v', 'm', 'c')
    -- SKIP_INTERNAL and e.objid is null
    -- SKIP_INTERNAL and n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
)
select
    r.relationtype,
    r.schema,
    r.name,
    r.definition as definition,
    a.attnum as position_number,
    a.attname as attname,
    a.attnotnull as not_null,
    a.atttypid::regtype AS datatype,
    pg_get_expr(ad.adbin, ad.adrelid) as defaultdef,
    r.oid as oid,
    format_type(atttypid, atttypmod) AS datatypestring,
    e.enum_oid is not null as is_enum,
    e.name as enum_name,
    e.schema as enum_schema
FROM
    r
    inner join pg_catalog.pg_attribute a
        on r.oid = a.attrelid
    left join pg_catalog.pg_attrdef ad
        on a.attrelid = ad.adrelid
        and a.attnum = ad.adnum
    left join enums e
      on a.atttypid = e.enum_oid
where a.attnum > 0
-- SKIP_INTERNAL and r.schema not in ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
AND    NOT a.attisdropped  -- no dead columns
order by relationtype, r.schema, r.name, position_number;
