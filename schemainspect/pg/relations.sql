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
    -- SKIP_INTERNAL and n.nspname not in ('pg_catalog', 'information_schema')
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
    -- SKIP_INTERNAL and n.nspname not in ('pg_catalog', 'information_schema')
), viewdeps as (
    with predeps as (
        SELECT
            distinct
            dependent_ns.nspname as schema,
            dependent_view.relname as name,
            source_ns.nspname as schema_dependent_on,
            source_table.relname as name_dependent_on
        FROM
            pg_depend
            JOIN pg_rewrite
                ON pg_depend.objid = pg_rewrite.oid
            JOIN pg_class as dependent_view
                ON pg_rewrite.ev_class = dependent_view.oid
            JOIN pg_class as source_table
                ON pg_depend.refobjid = source_table.oid
            JOIN pg_attribute
                ON pg_depend.refobjid = pg_attribute.attrelid
                AND pg_depend.refobjsubid = pg_attribute.attnum
            JOIN pg_namespace dependent_ns
                ON dependent_ns.oid = dependent_view.relnamespace
            JOIN pg_namespace source_ns
                ON source_ns.oid = source_table.relnamespace
        where
          -- SKIP_INTERNAL dependent_ns.nspname not in ('pg_catalog',
          -- SKIP_INTERNAL 'information_schema') and source_ns.nspname not in ('pg_catalog', 'information_schema')
        ORDER BY 1,2
    )

    select
        schema,
        name,
        array_agg(array[schema_dependent_on, name_dependent_on]) as dep_on
    from
        predeps
    group by schema, name
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
    format_type(atttypid, atttypmod) AS datatypestring,
    e.enum_oid is not null as is_enum,
    e.name as enum_name,
    e.schema as enum_schema,
    coalesce(v.dep_on, array[]::text[]) as dependent_on
FROM
    r
    inner join pg_catalog.pg_attribute a
        on r.oid = a.attrelid
    left join pg_catalog.pg_attrdef ad
        on a.attrelid = ad.adrelid
        and a.attnum = ad.adnum
    left join enums e
      on a.atttypid = e.enum_oid
    left join viewdeps v
      on r.relationtype = 'v' and r.schema = v.schema and r.name = v.name
where a.attnum > 0
-- SKIP_INTERNAL and r.schema not in ('pg_catalog', 'information_schema')
AND    NOT a.attisdropped  -- no dead columns
order by relationtype, r.schema, r.name, position_number;
