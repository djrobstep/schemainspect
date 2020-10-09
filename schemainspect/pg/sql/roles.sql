select
  oid,
  rolname as name,
  case rolsuper when true then
    'SUPERUSER'
  else
    'NOSUPERUSER'
  end as superuser,
  case rolinherit when true then
    'INHERIT'
  else
    'NOINHERIT'
  end as inherit,
  case rolcreaterole when true then
    'CREATEROLE'
  else
    'NOCREATEROLE'
  end as createrole,
  case rolcreatedb when true then
    'CREATEDB'
  else
    'NOCREATEDB'
  end as createdb,
  case rolcanlogin when true then
    'LOGIN'
  else
    'NOLOGIN'
  end as login,
  case rolreplication when true then
    'REPLICATION'
  else
    'NOREPLICATION'
  end as replication,
  case rolbypassrls when true then
    'BYPASSRLS'
  else
    'NOBYPASSRLS'
  end as bypassrls,
  rolconnlimit as connection_limit,
  rolpassword as password,
  rolvaliduntil as valid_until
from pg_authid
where
  rolsuper = false
  and rolname not like 'pg_%'
order by rolname;
