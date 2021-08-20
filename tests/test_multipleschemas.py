from sqlbag import S

from schemainspect import get_inspector

from .test_all import setup_pg_schema


def asserts_pg_multipleschemas(i, schema_names):
    for (
        prop
    ) in "schemas relations tables views functions selectables sequences enums constraints".split():
        att = getattr(i, prop)
        for k, v in att.items():
            assert v.schema in schema_names


def test_postgres_inspect_multipleschemas(db):
    with S(db) as s:
        setup_pg_schema(s)
        i = get_inspector(s, schema=["schema1", "schema2"])
        asserts_pg_multipleschemas(i, ["schema1", "schema2"])
        i = get_inspector(s, schema=["public", "schema"])
        asserts_pg_multipleschemas(i, "public")
