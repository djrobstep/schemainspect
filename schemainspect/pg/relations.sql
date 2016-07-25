with relations as (
        select
            schemaname || '.' || tablename as fullname,
            schemaname as schema,
            tablename as name,
            'r' as relationtype,
            null as definition
            from pg_tables
        union
        select
            schemaname || '.' || viewname as fullname,
            schemaname as schema,
            viewname as name,
            'v' as relationtype,
            definition as definition
            from pg_views
        union
        select
            schemaname || '.' || matviewname as fullname,
            schemaname as schema,
            matviewname as name,
            'm' as relationtype,
            definition as definition
            from pg_matviews
    )
    select
        r.relationtype,
        r.name,
        r.schema,
        r.fullname,
        r.definition as definition,
        a.attnum as position_number,
        a.attname as attname,
        a.attnotnull as not_null,
        a.atttypid::regtype AS datatype,
        ad.adsrc as defaultdef,
        format_type(atttypid, atttypmod) AS datatypestring
    FROM
        relations r
        inner join pg_attribute a
            on r.fullname::regclass = a.attrelid
        left join pg_attrdef ad
            on a.attrelid = ad.adrelid
            and a.attnum = ad.adnum
    where  attnum > 0
    AND    NOT attisdropped  -- no dead columns
    order by relationtype, r.name, position_number;
