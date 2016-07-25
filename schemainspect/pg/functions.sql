with r1 as (
        SELECT
            r.routine_name,
            r.specific_catalog as db,
            r.routine_schema as schema,
            r.routine_schema || '.' || r.routine_name as fullname,
            r.specific_name,
            r.data_type,
            r.external_language,
            r.routine_definition as definition,
            r.is_null_call,
            r.is_deterministic,
            string_to_array(r.specific_name, '_') as specific_name_tokens,
            array_length(string_to_array(r.specific_name, '_'), 1) as i
        FROM information_schema.routines r
        order by
            r.specific_name
    ),
    r as (
        select
            r1.*,
            specific_name_tokens[i]::bigint as oid
        from r1
    ),
    pre as (
        SELECT
            r.db as db,
            r.schema as schema,
            r.routine_name as name,
            r.fullname as fullname,
            r.data_type as returntype,
            p.parameter_name as parameter_name,
            p.data_type as data_type,
            p.parameter_mode as parameter_mode,
            p.parameter_default as parameter_default,
            p.ordinal_position as position_number,
            r.definition as definition,
            r.external_language as lang,
            pg_get_function_result(oid) as resultstring,
            pg_get_function_identity_arguments(oid) as args,
            r.fullname
            || '('
            || pg_get_function_identity_arguments(oid)
            || ')' as sig

        FROM r
            JOIN information_schema.parameters p ON
                r.specific_name=p.specific_name
        order by
            db, name, parameter_mode, ordinal_position, parameter_name
    )
select
*,
'drop function if exists ' || sig || ' cascade;'
    as drop_statement,
'create or replace function ' || sig || '
returns ' || resultstring || ' as
$$' || definition || '$$
LANGUAGE ' || lang || ';
'
    as create_statement
from pre
where
  lang not in ('C', 'INTERNAL')
  and schema != 'pg_catalog'
order by
    db, name, parameter_mode, position_number, parameter_name
