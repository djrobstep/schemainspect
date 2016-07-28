with extension_oids as (
  select
      objid
  from
      pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass
), r as (
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
    where c.relkind in ('r', 'v', 'm')
    -- SKIP_INTERNAL and e.objid is null
    -- SKIP_INTERNAL and n.nspname not in ('pg_catalog', 'information_schema')
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
    ad.adsrc as defaultdef,
    r.oid as oid,
    format_type(atttypid, atttypmod) AS datatypestring
FROM
    r
    inner join pg_catalog.pg_attribute a
        on r.oid = a.attrelid
    left join pg_catalog.pg_attrdef ad
        on a.attrelid = ad.adrelid
        and a.attnum = ad.adnum
where a.attnum > 0
-- SKIP_INTERNAL and schema not in ('pg_catalog', 'information_schema')
AND    NOT a.attisdropped  -- no dead columns
order by relationtype, r.schema, r.name, position_number;
