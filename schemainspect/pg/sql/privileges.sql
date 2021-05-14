select
    table_schema   as schema,
    table_name     as name,
    'table'        as object_type,
    grantee        as "user",
    privilege_type as privilege
from
    information_schema.role_table_grants r
    left join pg_tables t
        on t.schemaname = r.table_schema and t.tablename = r.table_name
    left join pg_views v
        on v.schemaname = r.table_schema and v.viewname = r.table_name
where
    r.grantee is distinct from t.tableowner
    and r.grantee is distinct from v.viewowner
-- SKIP_INTERNAL and table_schema not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
-- SKIP_INTERNAL and table_schema not like 'pg_temp_%' and table_schema not like 'pg_toast_temp_%'
order by schema, name, user;

