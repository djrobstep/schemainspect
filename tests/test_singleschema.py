from sqlbag import S

from schemainspect import get_inspector

from .test_all import setup_pg_schema


def asserts_pg_singleschema(i, schema_name):
    for (
        prop
    ) in "schemas relations tables views functions selectables sequences enums constraints rlspolicies".split():
        att = getattr(i, prop)
        for k, v in att.items():
            assert v.schema == schema_name


def test_postgres_inspect_singleschema(db):
    with S(db) as s:
        setup_pg_schema(s)
        i = get_inspector(s, schema="otherschema")
        asserts_pg_singleschema(i, "otherschema")
        i = get_inspector(s, schema="public")
        asserts_pg_singleschema(i, "public")
