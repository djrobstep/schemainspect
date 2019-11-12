select
  ur.rolname as roleid,
  um.rolname as member,
  a.admin_option,
  ug.rolname as grantor
from pg_auth_members a
left join pg_authid ur on ur.oid = a.roleid
left join pg_authid ug on ug.oid = a.grantor
left join pg_authid um on um.oid = a.member
where
  not (ur.rolname ~ '^pg_' and um.rolname ~ '^pg_')
order by 1, 2, 3;
