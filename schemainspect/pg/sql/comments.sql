select
    'function' object_type,
    n.nspname "schema",
    NULL "table",
    p.proname "name",
    pg_catalog.pg_get_function_identity_arguments(p.oid) args,
    pg_catalog.obj_description(p.oid, 'pg_proc') "comment"
from
    pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
where
    n.nspname <> 'pg_catalog'
    and n.nspname <> 'information_schema'
    and pg_catalog.obj_description(p.oid, 'pg_proc') is not null
union all
select
    case c.relkind
        when 'I' then 'index'
        when 'c' then 'type'
        when 'i' then 'index'
        when 'm' then 'materialized view'
        when 'p' then 'table'
        when 'r' then 'table'
        when 's' then 'sequence'
        when 'v' then 'view'
    end,
    n.nspname,
    NULL,
    c.relname,
    NULL,
    pg_catalog.obj_description(c.oid, 'pg_class')
from
    pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
where
    n.nspname <> 'pg_catalog'
    and n.nspname <> 'information_schema'
    and pg_catalog.obj_description(c.oid, 'pg_class') is not null
union all
select
    'column',
    n.nspname,
    c.relname,
    a.attname,
    NULL,
    pg_catalog.col_description(c.oid, a.attnum)
from
    pg_catalog.pg_attribute a
    join pg_catalog.pg_class c on c.oid = a.attrelid
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
where
    n.nspname <> 'pg_catalog'
    and n.nspname <> 'information_schema'
    and pg_catalog.col_description(c.oid, a.attnum) is not null;
