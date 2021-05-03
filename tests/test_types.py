from sqlbag import S

from schemainspect import get_inspector

CHECK = r"CHECK (VALUE ~ '^\d{5}$'::text OR VALUE ~ '^\d{5}-\d{4}$'::text)"

CREATE = """
CREATE TYPE compfoo AS (f1 int, f2 text);

CREATE DOMAIN us_postal_code AS TEXT
{};
""".format(
    CHECK
)


FUNC_N = """
create or replace function "public"."depends_on_vvv"(t text)
    returns integer as
    $$select\r\n1$$
    language SQL VOLATILE CALLED ON NULL INPUT SECURITY INVOKER;

"""


def test_lineendings(db):
    with S(db) as s:
        s.execute(FUNC_N)
        i = get_inspector(s)
        f = i.functions['"public"."depends_on_vvv"(t text)']

        assert f.definition == "select\r\n1"


def test_types_and_domains(db):
    with S(db) as s:
        s.execute(CREATE)
        i = get_inspector(s)

        compfoo = i.types['"public"."compfoo"']

        assert len(compfoo.columns) == 2

        assert (
            compfoo.create_statement
            == """\
create type "public"."compfoo" as (
    "f1" int4,
    "f2" text
);"""
        )

        compfoo.name = "compfoo2"

        s.execute(compfoo.create_statement)

        i = get_inspector(s)
        c1 = i.types['"public"."compfoo"']
        c2 = i.types['"public"."compfoo2"']

        c2.name = "compfoo"
        assert c1 == c2

        postal = i.domains['"public"."us_postal_code"']

        assert postal.data_type == "text"
        assert postal.not_null is False
        assert postal.constraint_name == "us_postal_code_check"

        assert postal.check == CHECK

        assert (
            postal.create_statement
            == """\
create domain "public"."us_postal_code"
as text
null
{}
;
""".format(
                CHECK
            )
        )
        assert postal.drop_statement == """drop domain if exists "public"."us_postal_code";"""

        postal.name = "postal2"
        s.execute(postal.create_statement)
