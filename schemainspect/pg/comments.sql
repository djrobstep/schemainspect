SELECT n.nspname as "schema",
  'function' as object_type,
  p.proname || '(' || pg_catalog.pg_get_function_arguments(p.oid) || ')' as "ident",
  pg_catalog.obj_description(p.oid, 'pg_proc') as "comment"
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
     LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang
WHERE pg_catalog.pg_function_is_visible(p.oid)
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND pg_catalog.obj_description(p.oid, 'pg_proc') is not null
ORDER BY 1, 2, 3;
