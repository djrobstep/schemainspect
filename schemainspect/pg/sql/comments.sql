SELECT obj_address.type                                                      AS object_type
     , obj_address.object_names                                              AS object_addr
     , obj_address.object_args                                               AS object_args
     , d.description                                                         AS comment
     , 'comment on ' || PG_DESCRIBE_OBJECT(d.classoid, d.objoid, d.objsubid) AS object_description
     , CASE
           WHEN obj_address.type = 'function'
               THEN 'COMMENT ON FUNCTION ' || ARRAY_TO_STRING(
                   (SELECT ARRAY_AGG(QUOTE_IDENT(o)) FROM UNNEST(obj_address.object_names) o), '.') || '(' ||
                    ARRAY_TO_STRING(object_args, ', ') || ') IS ' || QUOTE_LITERAL(d.description) || ';'
           WHEN obj_address.type LIKE '% column'
               THEN 'COMMENT ON COLUMN ' ||
                    ARRAY_TO_STRING((SELECT ARRAY_AGG(QUOTE_IDENT(o)) FROM UNNEST(obj_address.object_names) o), '.') ||
                    ' IS ' || QUOTE_LITERAL(d.description) || ';'
           WHEN obj_address.type = 'type'
               THEN 'COMMENT ON TYPE ' ||
                    (SELECT QUOTE_IDENT(typnamespace::REGNAMESPACE::TEXT) || '.' || QUOTE_IDENT(typname)
                     FROM pg_type
                     WHERE oid = obj_address.object_names[1]::REGTYPE) || ' IS ' || QUOTE_LITERAL(d.description) || ';'
           ELSE 'COMMENT ON ' || UPPER(obj_address.type) || ' ' ||
                ARRAY_TO_STRING((SELECT ARRAY_AGG(QUOTE_IDENT(o)) FROM UNNEST(obj_address.object_names) o), '.') ||
                ' IS ' || QUOTE_LITERAL(d.description) || ';'
       END                                                                   AS create_statement
     , CASE
           WHEN obj_address.type = 'function'
               THEN 'COMMENT ON FUNCTION ' ||
                    ARRAY_TO_STRING((SELECT ARRAY_AGG(QUOTE_IDENT(o)) FROM UNNEST(obj_address.object_names) o), '.') ||
                    '(' || ARRAY_TO_STRING(object_args, ', ') || ') IS NULL;'
           WHEN obj_address.type LIKE '% column'
               THEN 'COMMENT ON COLUMN ' ||
                    ARRAY_TO_STRING((SELECT ARRAY_AGG(QUOTE_IDENT(o)) FROM UNNEST(obj_address.object_names) o), '.') ||
                    ' IS NULL;'
           WHEN obj_address.type = 'type'
               THEN 'COMMENT ON TYPE ' ||
                    (SELECT QUOTE_IDENT(typnamespace::REGNAMESPACE::TEXT) || '.' || QUOTE_IDENT(typname)
                     FROM pg_type
                     WHERE oid = obj_address.object_names[1]::REGTYPE) || ' IS NULL;'
           ELSE 'COMMENT ON ' || UPPER(obj_address.type) || ' ' ||
                ARRAY_TO_STRING((SELECT ARRAY_AGG(QUOTE_IDENT(o)) FROM UNNEST(obj_address.object_names) o), '.') ||
                ' IS NULL;'
       END                                                                   AS drop_statement
FROM pg_description                                                             d
JOIN LATERAL PG_IDENTIFY_OBJECT_AS_ADDRESS(d.classoid, d.objoid, d.objsubid) AS obj_address ON TRUE
WHERE obj_address.object_names[1] NOT LIKE 'pg_%'
  AND obj_address.object_names[1] != 'information_schema';
