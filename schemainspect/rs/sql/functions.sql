with r1 as
(
    SELECT
        r.routine_name as name,
        r.routine_schema as schema,
        r.specific_name as specific_name,
        r.data_type,
        r.external_language,
        r.routine_definition as definition
    FROM information_schema.routines r
    -- SKIP_INTERNAL where r.external_language not in ('C', 'INTERNAL')
    -- SKIP_INTERNAL and r.routine_schema not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
    -- SKIP_INTERNAL and r.routine_schema not like 'pg_temp_%' and r.routine_schema not like 'pg_toast_temp_%'
    order by
        r.specific_name
),
pgproc as
(
  select
    nspname as "schema",
    proname as "name",
    p.oid as oid,
    case proisstrict when true then
      'RETURNS NULL ON NULL INPUT'
    else
      'CALLED ON NULL INPUT'
    end as strictness,
    case prosecdef when true then
      'SECURITY DEFINER'
    else
      'SECURITY INVOKER'
    end as security_type,
    case provolatile
      when 'i' then
        'IMMUTABLE'
      when 's' then
        'STABLE'
      when 'v' then
        'VOLATILE'
      else
        null
    end as volatility
  from
      (SELECT oid, * FROM pg_proc p WHERE NOT p.proisagg) p
      INNER JOIN pg_namespace n
          ON n.oid=p.pronamespace
  -- SKIP_INTERNAL where nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
  -- SKIP_INTERNAL and nspname not like 'pg_temp_%' and nspname not like 'pg_toast_temp_%'
),
r as
(
    select
        r1.*,
        pp.volatility,
        pp.strictness,
        pp.security_type,
        pp.oid
    from r1
    left outer join pgproc pp
      on r1.schema = pp.schema
      and r1.specific_name = pp.name || '_' || pp.oid
),
pre as
(
    SELECT
        r.schema as schema,
        r.name as name,
        r.data_type as returntype,
        p.parameter_name as parameter_name,
        p.data_type as data_type,
        p.parameter_mode as parameter_mode,
        p.ordinal_position as position_number,
        r.definition as definition,
        pg_get_functiondef(oid) as full_definition,
        r.external_language as language,
        r.strictness as strictness,
        r.security_type as security_type,
        r.volatility as volatility,
        r.oid as oid,
        pg_catalog.obj_description(r.oid) as comment
    FROM r
        LEFT JOIN information_schema.parameters p ON
            r.specific_name=p.specific_name
    order by
        name, parameter_mode, ordinal_position, parameter_name
)
select
  *
from pre
order by
  schema, name, parameter_mode, position_number, parameter_name;