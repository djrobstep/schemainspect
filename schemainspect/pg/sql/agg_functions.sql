SELECT obj_address.type                                     AS object_type
     , obj_address.object_names                             AS object_addr
     , obj_address.object_args                              AS object_args
     , nsp.nspname                                          AS schema
     , p.proname                                            AS name
     , pg_catalog.PG_GET_FUNCTION_ARGUMENTS(p.oid)          AS function_arguments
     , pg_catalog.pg_get_function_identity_arguments(p.oid) AS function_identity_arguments
     , pg_catalog.PG_GET_FUNCTION_RESULT(p.oid)             AS result_type
     , NULLIF(a.aggtransfn::TEXT, '-')::REGPROC             AS aggtransfn
     , NULLIF(a.aggfinalfn::TEXT, '-')::REGPROC             AS aggfinalfn
     , NULLIF(a.aggmtransfn::TEXT, '-')::REGPROC            AS aggmtransfn
     , NULLIF(a.aggmfinalfn::TEXT, '-')::REGPROC            AS aggmfinalfn
     , a.aggtransspace                                      AS aggtransspace
     , a.aggmtransspace                                     AS aggmtransspace
     , a.agginitval                                         AS agginitval
     , a.aggminitval                                        AS aggminitval
     , a.aggkind                                            AS aggkind
     , a.aggnumdirectargs                                   AS aggnumdirectargs
     , tt.oid::REGTYPE                                      AS state_type
     , ttf.oid::REGTYPE                                     AS final_type
FROM pg_catalog.pg_proc                                                           p
JOIN      LATERAL PG_IDENTIFY_OBJECT_AS_ADDRESS('pg_proc'::REGCLASS, p.oid, 0) AS obj_address ON TRUE
JOIN      pg_catalog.pg_namespace                                                 nsp ON p.pronamespace = nsp.oid
JOIN      pg_catalog.pg_aggregate                                                 a ON a.aggfnoid = p.oid
LEFT JOIN pg_catalog.pg_type                                                      tt ON tt.oid = a.aggtranstype
LEFT JOIN pg_catalog.pg_type                                                      ttf ON ttf.oid = p.prorettype
WHERE nsp.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schema, name;
