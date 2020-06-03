with r1 as (
        SELECT
            r.routine_name as name,
            r.routine_schema as schema,
            r.specific_name as specific_name,
            r.data_type,
            r.type_udt_schema,
            r.type_udt_name,
            r.external_language,
            r.routine_definition as definition
        FROM information_schema.routines r
        -- SKIP_INTERNAL where r.external_language not in ('C', 'INTERNAL')
        -- SKIP_INTERNAL and r.routine_schema not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
        -- SKIP_INTERNAL and r.routine_schema not like 'pg_temp_%' and r.routine_schema not like 'pg_toast_temp_%'
        order by
            r.specific_name
    ),
    pgproc as (
      select
        nspname as schema,
        proname as name,
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
        end as volatility,
        p.proargtypes,
        p.proallargtypes,
        p.proargnames,
        p.proargdefaults,
        p.proargmodes,
        p.proowner,
        COALESCE(p.proallargtypes, p.proargtypes::oid[]) as procombinedargtypes,
        -- 11_AND_LATER p.prokind as kind
        -- 10_AND_EARLIER case when p.proisagg then 'a' else 'f' end as kind
      from
          pg_proc p
          INNER JOIN pg_namespace n
              ON n.oid=p.pronamespace
      where true
      -- 11_AND_LATER and p.prokind != 'a'
      -- SKIP_INTERNAL and nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
      -- SKIP_INTERNAL and nspname not like 'pg_temp_%' and nspname not like 'pg_toast_temp_%'
    ),
    extension_oids as (
      select
          objid
      from
          pg_depend d
      WHERE
          d.refclassid = 'pg_extension'::regclass
          and d.classid = 'pg_proc'::regclass
    ),
    r as (
        select
            r1.*,
            pp.volatility,
            pp.strictness,
            pp.security_type,
            pp.oid,
            pp.kind,
            e.objid as extension_oid
        from r1
        left outer join pgproc pp
          on r1.schema = pp.schema
          and r1.specific_name = pp.name || '_' || pp.oid
        left outer join extension_oids e
          on pp.oid = e.objid
        -- SKIP_INTERNAL where e.objid is null
    ),
unnested as (
    select
        p.oid as p_oid,
        --schema,
        --name,
        pname as parameter_name,
        --pdatatype,
        --pargtype as data_type,
        pnum as position_number,
        CASE
            WHEN pargmode IS NULL THEN null
            WHEN pargmode = 'i'::"char" THEN 'IN'::text
            WHEN pargmode = 'o'::"char" THEN 'OUT'::text
            WHEN pargmode = 'b'::"char" THEN 'INOUT'::text
            WHEN pargmode = 'v'::"char" THEN 'IN'::text
            WHEN pargmode = 't'::"char" THEN 'OUT'::text
            ELSE NULL::text
            END::information_schema.character_data AS parameter_mode,
      CASE
        WHEN t.typelem <> 0::oid AND t.typlen = '-1'::integer THEN 'ARRAY'::text
        else format_type(t.oid, NULL::integer)

    END::information_schema.character_data AS data_type,
    CASE
            WHEN pg_has_role(p.proowner, 'USAGE'::text) THEN pg_get_function_arg_default(p.oid, pnum::int)
            ELSE NULL::text
        END::varchar AS parameter_default
    from pgproc p
    left join lateral
    unnest(
        p.proargnames,
        p.proallargtypes,
        p.procombinedargtypes,
        p.proargmodes)
    WITH ORDINALITY AS uu(pname, pdatatype, pargtype, pargmode, pnum) ON TRUE
    left join pg_type t
        on t.oid = uu.pargtype
),
    pre as (
        SELECT
            r.schema as schema,
            r.name as name,
            case when r.data_type = 'USER-DEFINED' then
              '"' || r.type_udt_schema || '"."' || r.type_udt_name || '"'
            else
              r.data_type
            end as returntype,
            r.data_type = 'USER-DEFINED' as has_user_defined_returntype,
            p.parameter_name as parameter_name,
            p.data_type as data_type,
            p.parameter_mode as parameter_mode,
            p.parameter_default as parameter_default,
            p.position_number as position_number,
            r.definition as definition,
            pg_get_functiondef(r.oid) as full_definition,
            r.external_language as language,
            r.strictness as strictness,
            r.security_type as security_type,
            r.volatility as volatility,
            r.kind as kind,
            r.oid as oid,
            r.extension_oid as extension_oid,
            pg_get_function_result(r.oid) as result_string,
            pg_get_function_identity_arguments(r.oid) as identity_arguments,
            pg_catalog.obj_description(r.oid) as comment
        FROM r
        left join unnested p
          on r.oid = p.p_oid
        --    LEFT JOIN information_schema.parameters p ON
        --        r.specific_name=p.specific_name


        order by
            name, parameter_mode, position_number, parameter_name
    )
select
*
from pre
order by
    schema, name, parameter_mode, position_number, parameter_name;
