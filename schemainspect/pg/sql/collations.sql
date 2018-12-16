select
  collname as name,
  n.nspname as schema,
-- PG_10  case collprovider
-- PG_10    when 'i' then 'icu'
-- PG_10    when 'c' then 'libc'
-- PG_10  end 
-- PG_!10 'db'  
  as provider,
  collencoding as encoding,
  collcollate as lc_collate,
  collctype as lc_ctype,
-- PG_10  collversion
-- PG_!10 null 
  as version
from
pg_collation c
INNER JOIN pg_namespace n
    ON n.oid=c.collnamespace
    -- SKIP_INTERNAL where nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
    -- SKIP_INTERNAL and nspname not like 'pg_temp_%' and nspname not like 'pg_toast_temp_%'
order by 2, 1
