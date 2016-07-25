with indexes as (
        select
            schemaname || '.' || indexname as fullname,
            schemaname as schema,
            tablename as table_name,
            indexname as name,
            indexdef as definition,
            indexdef as create_statement
        FROM
            pg_indexes
        order by
            schemaname, tablename, indexname
    )

    select
        nspname as schema,
        conname as name,
        relname as table_name,
        pg_get_constraintdef(pg_constraint.oid) as definition,
        tc.constraint_type as constraint_type,
        i.fullname is not null as is_index
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
            and relname = i.table_name;
