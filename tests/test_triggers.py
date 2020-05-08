from sqlbag import S

from schemainspect import get_inspector

BASE = """
CREATE TABLE "my_table" (
    "some_text" text,
    "some_count" int
);

CREATE VIEW "view_on_table" AS
SELECT some_text, some_count FROM my_table;

CREATE OR REPLACE FUNCTION my_function()
    RETURNS trigger
    LANGUAGE plpgsql
AS $function$
    BEGIN
        INSERT INTO my_table (some_text)
        VALUES (NEW.some_text);
        RETURN NEW;
    END;
$function$
;

CREATE TRIGGER trigger_on_view INSTEAD OF
INSERT ON view_on_table
FOR EACH ROW EXECUTE PROCEDURE my_function();
;

"""


def test_triggers(db):
    with S(db) as s:
        s.execute(BASE)
        i = get_inspector(s)

        print(list(i.triggers))

        t = i.triggers['"public"."view_on_table"."trigger_on_view"']

        print(dir(t))
        print(t.table_name)
        # f = i.functions['"public"."ordinary_f"(t text)']

        # assert f.definition == "select\r\n1"
        # assert f.kind == 'f'

        # p = i.functions['"public"."proc"(a integer, b integer)']

        # assert p.definition == "\nselect a, b;\n"
        # assert p.kind == 'p'

        # assert p.drop_statement == 'drop procedure if exists "public"."proc"(a integer, b integer);'
