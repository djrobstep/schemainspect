SELECT
  CASE (d.iden).type
    WHEN 'domain constraint' THEN 'constraint'
    WHEN 'table column' THEN 'column'
    WHEN 'table constraint' THEN 'constraint'
    ELSE (d.iden).type::TEXT
  END AS object_type,
  (d.iden).identity AS identifier,
  d.description AS comment
FROM (
  SELECT
    pg_identify_object(classoid, objoid, objsubid) AS iden,
    DESCRIPTION
  FROM pg_description
) d
WHERE
    (
        (d.iden).schema IS NULL
        AND (d.iden).type = 'trigger'
    ) OR (
        (d.iden).schema <> 'pg_catalog'
        AND (d.iden).schema <> 'information_schema'
    );
