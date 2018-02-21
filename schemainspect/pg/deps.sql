
with things1 as (
  select
    oid as objid,
    pronamespace as namespace,
    proname || '(' || pg_get_function_identity_arguments(oid) || ')' as name,
    'f' as kind
  from pg_proc
  union
  select
    oid,
    relnamespace as namespace,
    relname as name,
    relkind as kind
  from pg_class
),
extension_objids as (
  select
      objid as extension_objid
  from
      pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass
),
things as (
    select
      objid,
      kind,
      n.nspname as schema,
      name
    from things1 t
    inner join pg_namespace n
      on t.namespace = n.oid
    left outer join extension_objids
      on t.objid = extension_objids.extension_objid
    where
      kind in ('r', 'v', 'm', 'c', 'f') and
      nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1') and extension_objids.extension_objid is null
),
combined as (
  select distinct
    t.objid,
    t.schema,
    t.name,
    things_dependent_on.objid as objid_dependent_on,
    things_dependent_on.schema as schema_dependent_on,
    things_dependent_on.name as name_dependent_on
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
    and rw.rulename = '_RETURN'
)
select * from combined;
