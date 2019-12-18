from sqlbag import S

from schemainspect import get_inspector


def test_partitions(db):
    with S(db) as s:
        i = get_inspector(s)

        if i.pg_version <= 9:
            return
        s.execute(
            """
CREATE TABLE measurement (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
)
PARTITION BY RANGE (logdate);

CREATE TABLE measurement_y2006 PARTITION OF measurement
    FOR VALUES FROM ('2006-01-01') TO ('2007-01-01');
        """
        )

        i = get_inspector(s)
    assert list(i.tables) == ['"public"."measurement"', '"public"."measurement_y2006"']

    m2006 = i.tables['"public"."measurement_y2006"']
    m = i.tables['"public"."measurement"']

    assert m.parent_table is None
    assert m2006.parent_table == '"public"."measurement"'

    assert m2006.partition_def == "FOR VALUES FROM ('2006-01-01') TO ('2007-01-01')"

    assert m.partition_def == "RANGE (logdate)"

    assert (
        m.create_statement
        == """create table "public"."measurement" (
    "city_id" integer not null,
    "logdate" date not null,
    "peaktemp" integer,
    "unitsales" integer
) partition by RANGE (logdate);
"""
    )

    assert (
        m2006.create_statement
        == """create table "public"."measurement_y2006" partition of "public"."measurement" FOR VALUES FROM ('2006-01-01') TO ('2007-01-01');
"""
    )

    assert m.is_partitioning_child_table is False
    assert m.is_inheritance_child_table is False
    assert m.contains_data is False
    assert m.is_alterable is True
    assert m.is_partitioned is True
    assert m.uses_partitioning is True

    assert m2006.is_partitioning_child_table is True
    assert m2006.is_inheritance_child_table is False
    assert m2006.contains_data is True
    assert m2006.is_alterable is False
    assert m2006.is_partitioned is False
    assert m2006.uses_partitioning is True

    with S(db) as s:
        s.execute(
            """
CREATE TABLE plain (id int);
        """
        )

    i = get_inspector(s)

    p = i.tables['"public"."plain"']
    assert p.is_partitioning_child_table is False
    assert p.is_inheritance_child_table is False
    assert p.contains_data is True
    assert p.is_alterable is True
    assert p.is_partitioned is False
    assert p.uses_partitioning is False


def test_inherit(db):
    with S(db) as s:
        i = get_inspector(s)

        if i.pg_version <= 9:
            return
        s.execute(
            """
CREATE TABLE entity_bindings (
    id BIGSERIAL,
    entity_type TEXT NOT NULL,
    entity_id BIGINT NOT NULL
);
CREATE TABLE entity_bindings_A (
    CONSTRAINT "entity_type must be A" CHECK("entity_type" = 'A'),
    UNIQUE("entity_id", "entity_type")
) INHERITS (entity_bindings)
;
CREATE TABLE entity_bindings_B (
    CONSTRAINT "entity_type must be B" CHECK("entity_type" = 'B'),
    UNIQUE("entity_id", "entity_type")
) INHERITS (entity_bindings)
;
CREATE TABLE entity_bindings_C (
    CONSTRAINT "entity_type must be C" CHECK("entity_type" = 'C'),
    UNIQUE("entity_id", "entity_type")
) INHERITS (entity_bindings)
;
        """
        )

        i = get_inspector(s)

        t = i.tables['"public"."entity_bindings"']
        tb = i.tables['"public"."entity_bindings_b"']

        assert tb.parent_table == '"public"."entity_bindings"'

        assert t.parent_table is None

        assert tb.is_inheritance_child_table is True
        assert tb.uses_partitioning is False
