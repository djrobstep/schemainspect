with extension_oids as (
  select
      objid
  from
      pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass
), indexes as (
    select
        schemaname as schema,
        tablename as table_name,
        indexname as name,
        indexdef as definition,
        indexdef as create_statement
    FROM
        pg_indexes
        -- SKIP_INTERNAL where schemaname not in ('pg_catalog', 'information_schema', 'pg_toast')
				-- SKIP_INTERNAL and schemaname not like 'pg_temp_%' and schemaname not like 'pg_toast_temp_%'
    order by
        schemaname, tablename, indexname
)
select
    nspname as schema,
    conname as name,
    relname as table_name,
    pg_get_constraintdef(pg_constraint.oid) as definition,
    tc.constraint_type as constraint_type,
    i.name as index,
    e.objid as extension_oid,
    case when tc.constraint_type = 'FOREIGN KEY' then
        (
            SELECT nspname
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS ns
            ON c.relnamespace = ns.oid
            WHERE c.oid = confrelid::regclass
        )

    end as foreign_table_schema,
    case when tc.constraint_type = 'FOREIGN KEY' then
        confrelid::regclass
    end as foreign_table_name,
    case when tc.constraint_type = 'FOREIGN KEY' then
        (
            select
                array_agg(ta.attname order by ta.attnum)
            from
            pg_attribute ta where ta.attrelid = conrelid and ta.attnum = any(conkey)
        )
    else null end as fk_columns_local,
    case when tc.constraint_type = 'FOREIGN KEY' then
        (
            select
                array_agg(fa.attname order by fa.attnum)
            from
            pg_attribute fa where fa.attrelid = confrelid and fa.attnum = any(confkey)
        )
    else null end as fk_columns_foreign,
    tc.constraint_type = 'FOREIGN KEY' as is_fk,
    tc.is_deferrable = 'YES' as is_deferrable,
    tc.initially_deferred = 'YES' as initially_deferred
from
    pg_constraint
    INNER JOIN pg_class
        ON conrelid=pg_class.oid
    INNER JOIN pg_namespace
        ON pg_namespace.oid=pg_class.relnamespace
    inner join information_schema.table_constraints tc
        on nspname = tc.constraint_schema
        and conname = tc.constraint_name
        and relname = tc.table_name
    left outer join indexes i
        on nspname = i.schema
        and conname = i.name
        and relname = i.table_name
    left outer join extension_oids e
      on pg_class.oid = e.objid
    where true
  -- SKIP_INTERNAL and nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
  -- SKIP_INTERNAL and e.objid is null
order by 1, 3, 2;
