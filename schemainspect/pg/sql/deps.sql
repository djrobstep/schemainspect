with things1 as (
  select
    oid as objid,
    pronamespace as namespace,
    proname as name,
    pg_get_function_identity_arguments(oid) as identity_arguments,
    'f' as kind,
    null::oid as composite_type_oid
  from pg_proc
  -- 11_AND_LATER where pg_proc.prokind != 'a'
  -- 10_AND_EARLIER where pg_proc.proisagg is False
  union
  select
    oid,
    relnamespace as namespace,
    relname as name,
    null as identity_arguments,
    relkind as kind,
    null::oid as composite_type_oid
  from pg_class
  where oid not in (
    select ftrelid from pg_foreign_table
  )
    union
    select
        oid,
        typnamespace as namespace,
        typname as name,
        null as identity_arguments,
        'c' as kind,
        typrelid::oid as composite_type_oid
    from pg_type
    where typrelid != 0
),
extension_objids as (
  select
      objid as extension_objid
  from
      pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass
    union
    select
        t.typrelid as extension_objid
    from
        pg_depend d
        join pg_type t on t.oid = d.objid
    where
        d.refclassid = 'pg_extension'::regclass
),
things as (
    select
      objid,
      kind,
      n.nspname as schema,
      name,
      identity_arguments,
      t.composite_type_oid
    from things1 t
    inner join pg_namespace n
      on t.namespace = n.oid
    left outer join extension_objids
      on t.objid = extension_objids.extension_objid
    where
      kind in ('r', 'v', 'm', 'c', 'f') and
      nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
      and nspname not like 'pg_temp_%' and nspname not like 'pg_toast_temp_%'
      and extension_objids.extension_objid is null
),
array_dependencies as (
  select
    att.attrelid as objid,
    att.attname as column_name,
    tbl.typelem as composite_type_oid,
    comp_tbl.typrelid as objid_dependent_on
  from pg_attribute att
  join pg_type tbl on tbl.oid = att.atttypid
  join pg_type comp_tbl on tbl.typelem = comp_tbl.oid
  where tbl.typcategory = 'A'
),
combined as (
  select distinct
    coalesce(t.composite_type_oid, t.objid),
    t.schema,
    t.name,
    t.identity_arguments,
    case when t.composite_type_oid is not null then 'r' ELSE t.kind end,
    things_dependent_on.objid as objid_dependent_on,
    things_dependent_on.schema as schema_dependent_on,
    things_dependent_on.name as name_dependent_on,
    things_dependent_on.identity_arguments as identity_arguments_dependent_on,
    things_dependent_on.kind as kind_dependent_on
  FROM
      pg_depend d
      inner join things things_dependent_on
        on d.refobjid = things_dependent_on.objid
      inner join pg_rewrite rw
        on d.objid = rw.oid
        and things_dependent_on.objid != rw.ev_class
      inner join things t
        on rw.ev_class = t.objid
  where
    d.deptype in ('n')
    and
    rw.rulename = '_RETURN'
  union all
  select distinct
    coalesce(t.composite_type_oid, t.objid),
    t.schema,
    t.name,
    t.identity_arguments,
    case when t.composite_type_oid is not null then 'r' ELSE t.kind end,
    things_dependent_on.objid as objid_dependent_on,
    things_dependent_on.schema as schema_dependent_on,
    things_dependent_on.name as name_dependent_on,
    things_dependent_on.identity_arguments as identity_arguments_dependent_on,
    things_dependent_on.kind as kind_dependent_on
  FROM
      pg_depend d
      inner join things things_dependent_on
        on d.refobjid = things_dependent_on.objid
      inner join things t
        on d.objid = t.objid
  where
    d.deptype in ('n')
  union all
  select
    coalesce(t.composite_type_oid, t.objid),
    t.schema,
    t.name,
    t.identity_arguments,
    case when t.composite_type_oid is not null then 'r' ELSE t.kind end,
    things_dependent_on.objid as objid_dependent_on,
    things_dependent_on.schema as schema_dependent_on,
    things_dependent_on.name as name_dependent_on,
    things_dependent_on.identity_arguments as identity_arguments_dependent_on,
    things_dependent_on.kind as kind_dependent_on
  FROM
    array_dependencies ad
    inner join things things_dependent_on
    on ad.objid_dependent_on = things_dependent_on.objid
    inner join things t
    on ad.objid = t.objid
)
select * from combined
order by
schema, name, identity_arguments, kind_dependent_on,
schema_dependent_on, name_dependent_on, identity_arguments_dependent_on
