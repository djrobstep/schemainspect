from sqlbag import S

from schemainspect import get_inspector

from .test_all import setup_pg_schema


def asserts_pg_excludedschemas(i, schema_names, excludedschema_names):
    schemas = set()
    for (
        prop
    ) in "schemas relations tables views functions selectables sequences enums constraints".split():
        att = getattr(i, prop)
        for k, v in att.items():
            assert v.schema not in excludedschema_names
            schemas.add(v.schema)
    assert schemas == set(schema_names)


def test_postgres_inspect_excludeschemas(db):
    with S(db) as s:
        setup_pg_schema(s)
        s.execute("create schema thirdschema;")
        s.execute("create schema forthschema;")
        s.execute("create schema fifthschema;")
        # all: public other third forth fifth
        i = get_inspector(s, exclude_schema=["otherschema", "thirdschema"])
        asserts_pg_excludedschemas(
            i, ["public", "forthschema", "fifthschema"], ["otherschema", "thirdschema"]
        )
        i = get_inspector(s, exclude_schema=["forthschema", "thirdschema"])
        asserts_pg_excludedschemas(
            i, ["public", "otherschema", "fifthschema"], ["forthschema", "thirdschema"]
        )
