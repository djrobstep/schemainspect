select tg.tgname "name", nspt.nspname "schema", pro.proname proc_name, nspp.nspname proc_schema, pg_get_triggerdef(tg.oid) full_definition
from pg_trigger tg
join pg_class cls on cls.oid = tg.tgrelid
join pg_proc pro on pro.oid = tg.tgfoid
join pg_namespace nspt on nspt.oid = cls.relnamespace
join pg_namespace nspp on nspp.oid = pro.pronamespace
where not tg.tgisinternal;