from sqlbag import S

from schemainspect import get_inspector

CREATE = """
DROP SCHEMA IF EXISTS it CASCADE;
CREATE SCHEMA it;

CREATE FUNCTION it.key_func(jsonb) RETURNS int AS $$
SELECT jsonb_array_length($1);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION it.part_func(jsonb) RETURNS boolean AS $$
SELECT jsonb_typeof($1) = 'array';
$$ LANGUAGE SQL IMMUTABLE;

CREATE TABLE it.foo(a bigserial, b jsonb);

CREATE UNIQUE INDEX fun_partial_index ON it.foo (it.key_func(b))
 WHERE it.part_func(b);

CREATE INDEX brin_index ON it.foo USING BRIN (a);
"""


def test_indexes(db):
    with S(db) as s:
        s.execute(CREATE)
        i1 = get_inspector(s, schema="it")

        # Recreate schema.
        # Functions oids will be changed
        s.execute(CREATE)
        i2 = get_inspector(s, schema="it")

        assert i1.indexes == i2.indexes


CREATE_CONST = """
create table t(id uuid primary key, x bigint);

"""


def test_constraints(db):
    with S(db) as s:
        s.execute(CREATE_CONST)

        i = get_inspector(s)
        constraints_keys = list(i.constraints.keys())
        assert constraints_keys == ['"public"."t"."t_pkey"']

        indexes_keys = list(i.indexes.keys())

        assert indexes_keys == ['"public"."t_pkey"']


INDEX_DEFS = """

create schema s;

CREATE TABLE s.t (
    id uuid NOT NULL,
    a int4 NULL,
    b int4 NULL,
    CONSTRAINT pk PRIMARY KEY (id)
);

CREATE UNIQUE INDEX i ON s.t USING btree (a);

CREATE UNIQUE INDEX iii ON s.t USING btree (b, a) include (id);

CREATE UNIQUE INDEX iii_exp ON s.t((lower(id::text)));

"""

COLLATIONS = """
CREATE COLLATION french (locale = 'fr_FR');

CREATE COLLATION german (locale = 'de_DE');

CREATE TABLE test1c (
    id integer,
    content varchar COLLATE "french"
);

CREATE INDEX i ON test1c (id);

CREATE INDEX i_fr ON test1c (content);

CREATE INDEX i_de ON test1c (content COLLATE "german");


"""


def test_index_defs(db):
    with S(db) as s:
        ii = get_inspector(s)

        if ii.pg_version <= 10:
            return
        s.execute(INDEX_DEFS)

        ii = get_inspector(s)

        indexes_keys = list(ii.indexes.keys())

        EXPECTED = ['"s"."i"', '"s"."iii"', '"s"."iii_exp"', '"s"."pk"']
        assert indexes_keys == EXPECTED

        i = ii.indexes['"s"."i"']

        assert i.index_columns == ["a"]
        assert i.key_columns == ["a"]
        assert i.included_columns == []
        assert i.key_expressions is None

        i = ii.indexes['"s"."iii"']

        assert i.index_columns == ["b", "a", "id"]
        assert i.key_columns == ["b", "a"]
        assert i.included_columns == ["id"]
        assert i.key_expressions is None

        i = ii.indexes['"s"."iii_exp"']

        assert i.index_columns is None
        assert i.key_columns is None
        assert i.included_columns is None
        assert i.key_expressions == "lower((id)::text)"


# def test_collation_names(db):
#     with S(db) as s:
#         ii = get_inspector(s)

#         if ii.pg_version <= 10:
#             return
#         s.execute(COLLATIONS)

#         ii = get_inspector(s)

#         i = ii.indexes['"public"."i"']  # noqa

#         i_fr = ii.indexes['"public"."i_fr"']  # noqa
#         i_de = ii.indexes['"public"."i_de"']  # noqa

#         # TODO: Check for collation info once we add it.
