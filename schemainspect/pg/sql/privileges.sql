WITH partition_child_tables as (
  select
      inhrelid
  from
      pg_inherits
)
select
    n.nspname as schema,
    c.relname as name,
    case
        when c.relkind in ('r', 'v', 'm', 'f', 'p') then 'table'
        when c.relkind = 'S' then 'sequence'
        else null end as object_type,
    pg_get_userbyid(acl.grantee) as "user",
    acl.privilege,
    NULL as postfix
from
    pg_catalog.pg_class c
    join pg_catalog.pg_namespace n
         on n.oid = c.relnamespace,
    lateral (select aclx.*, privilege_type as privilege
             from aclexplode(c.relacl) aclx
             union
             select aclx.*, privilege_type || '(' || a.attname || ')' as privilege
             from
                 pg_catalog.pg_attribute a
                 cross join aclexplode(a.attacl) aclx
             where attrelid = c.oid and not attisdropped and attacl is not null ) acl
where
    acl.grantee != acl.grantor
    and acl.grantee != 0
    and c.relkind in ('r', 'v', 'm', 'S', 'f', 'p')
    -- and table is not a partition child table
    and c.oid not in (select inhrelid from partition_child_tables)
-- SKIP_INTERNAL    and nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
-- SKIP_INTERNAL    and nspname not like 'pg_temp_%' and nspname not like 'pg_toast_temp_%'
union
select
    routine_schema as schema,
    routine_name   as name,
    'function'     as object_type,
    grantee        as "user",
    privilege_type as privilege,
    FORMAT('(%s)', PG_GET_FUNCTION_IDENTITY_ARGUMENTS(oid)) as postfix
  FROM information_schema.role_routine_grants g
  JOIN pg_catalog.pg_proc p ON g.specific_schema::REGNAMESPACE = p.pronamespace::REGNAMESPACE AND g.specific_name = FORMAT('%s_%s', p.proname, p.oid)
where
    grantor != grantee
    and grantee != 'PUBLIC'
-- SKIP_INTERNAL    and routine_schema not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
-- SKIP_INTERNAL    and routine_schema not like 'pg_temp_%' and routine_schema not like 'pg_toast_temp_%'
union
select
    '' as schema,
    n.nspname as name,
    'schema' as object_type,
    pg_get_userbyid(acl.grantee) as "user",
    privilege,
    NULL as postfix
from pg_catalog.pg_namespace n,
    lateral (select aclx.*, privilege_type as privilege
             from aclexplode(n.nspacl) aclx
             union
             select aclx.*, privilege_type || '(' || a.attname || ')' as privilege
             from
                 pg_catalog.pg_attribute a
                 cross join aclexplode(a.attacl) aclx
             where attrelid = n.oid and not attisdropped and attacl is not null ) acl
where
    privilege != 'CREATE'
    and acl.grantor != acl.grantee
    and acl.grantee != 0
-- SKIP_INTERNAL    and n.nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
-- SKIP_INTERNAL    and n.nspname not like 'pg_temp_%' and n.nspname not like 'pg_toast_temp_%'
order by schema, name, "user";
