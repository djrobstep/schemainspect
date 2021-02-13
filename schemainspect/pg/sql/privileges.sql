select
  table_schema as schema,
  table_name as name,
  'table' as object_type,
  grantee as user,
  privilege_type as privilege
from information_schema.role_table_grants
where grantee != (
    select tableowner
    from pg_tables
    where schemaname = table_schema
    and tablename = table_name
)
-- SKIP_INTERNAL and table_schema not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
-- SKIP_INTERNAL and table_schema not like 'pg_temp_%' and table_schema not like 'pg_toast_temp_%'
union
select
  routine_schema as schema,
  routine_name as name,
  'function' as object_type,
  grantee as user,
  privilege_type as privilege
from information_schema.role_routine_grants
where grantee != (
    select datdba::regrole::text
    from pg_database
    where datname = current_database()
)
-- SKIP_INTERNAL and routine_schema not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
-- SKIP_INTERNAL and routine_schema not like 'pg_temp_%' and routine_schema not like 'pg_toast_temp_%'
order by schema, name, "user";
