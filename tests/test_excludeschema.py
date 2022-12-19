from sqlbag import S

from schemainspect import get_inspector

from .test_all import setup_pg_schema


def asserts_pg_excludedschema(i, schema_names, excludedschemas):
    schemas = set()
    for (
        prop
    ) in "schemas relations tables views functions selectables sequences enums constraints".split():
        att = getattr(i, prop)
        for k, v in att.items():
            assert v.schema not in excludedschemas
            schemas.add(v.schema)
    assert schemas == set(schema_names)


def test_postgres_inspect_excludeschema(db):
    with S(db) as s:
        setup_pg_schema(s)
        s.execute("create schema thirdschema;")
        s.execute("create schema forthschema;")
        i = get_inspector(s, exclude_schema="otherschema")
        asserts_pg_excludedschema(
            i, ["public", "forthschema", "thirdschema"], ["otherschema"]
        )
        i = get_inspector(s, exclude_schema="forthschema")
        asserts_pg_excludedschema(
            i, ["public", "otherschema", "thirdschema"], ["forthschema"]
        )
        i = get_inspector(s, exclude_schema="forthschema,otherschema")
        asserts_pg_excludedschema(
            i, ["public", "thirdschema"], ["forthschema", "otherschema"]
        )
        i = get_inspector(s, exclude_schema=["forthschema", "otherschema"])
        asserts_pg_excludedschema(
            i, ["public", "thirdschema"], ["forthschema", "otherschema"]
        )
