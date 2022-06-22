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


def test_view_trigger(db):
    with S(db) as s:
        s.execute(BASE)
        i = get_inspector(s)

        trigger = i.triggers['"public"."view_on_table"."trigger_on_view"']

        # Triggers on views should not include the ALTER TABLE part of the create statement
        assert "ALTER TABLE" not in trigger.create_statement


def test_replica_trigger(db):
    with S(db) as s:
        s.execute(BASE)
        function = """
        CREATE OR REPLACE FUNCTION table_trigger_function()
            RETURNS trigger
            LANGUAGE plpgsql
        AS $function$
            BEGIN
                RETURN NEW;
            END;
        $function$
        ;
        """
        s.execute(function)
        s.execute(
            "CREATE TRIGGER table_trigger AFTER INSERT ON my_table FOR EACH ROW EXECUTE PROCEDURE table_trigger_function();"
        )
        s.execute("ALTER TABLE my_table ENABLE REPLICA TRIGGER table_trigger;")

        i = get_inspector(s)

        trigger = i.triggers['"public"."my_table"."table_trigger"']

        # Replica trigger needs the ALTER TABLE statement as well as the trigger definition
        assert "ALTER TABLE" in trigger.create_statement
