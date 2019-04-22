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
