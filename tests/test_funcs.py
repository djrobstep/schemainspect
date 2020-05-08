from sqlbag import S

from schemainspect import get_inspector

FUNC = """

create or replace function "public"."ordinary_f"(t text)
    returns integer as
    $$select\r\n1$$
    language SQL VOLATILE CALLED ON NULL INPUT SECURITY INVOKER;

"""

PROC = """
CREATE PROCEDURE proc(a integer, b integer)
LANGUAGE SQL
AS $$
select a, b;
$$;


"""


def test_kinds(db):
    with S(db) as s:
        s.execute(FUNC)
        s.execute(PROC)
        i = get_inspector(s)
        f = i.functions['"public"."ordinary_f"(t text)']

        assert f.definition == "select\r\n1"
        assert f.kind == "f"

        p = i.functions['"public"."proc"(a integer, b integer)']

        assert p.definition == "\nselect a, b;\n"
        assert p.kind == "p"

        assert (
            p.drop_statement
            == 'drop procedure if exists "public"."proc"(a integer, b integer);'
        )
