select tg.tgname "name", nsp.nspname "schema", cls.relname table_name, pg_get_triggerdef(tg.oid) full_definition
from pg_trigger tg
join pg_class cls on cls.oid = tg.tgrelid
join pg_namespace nsp on nsp.oid = cls.relnamespace
where not tg.tgisinternal
order by schema, name;
