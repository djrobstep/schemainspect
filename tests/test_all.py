import datetime
from collections import OrderedDict as od
from contextlib import contextmanager
from copy import deepcopy

import psycopg2
import pytest
import sqlalchemy.dialects.postgresql
import sqlalchemy.exc
from psycopg2.extras import NamedTupleCursor
from pytest import raises
from sqlbag import S, temporary_database

import schemainspect
from schemainspect import NullInspector, get_inspector, to_pytype
from schemainspect.inspected import ColumnInfo
from schemainspect.misc import quoted_identifier
from schemainspect.pg.obj import (
    InspectedConstraint,
    InspectedEnum,
    InspectedExtension,
    InspectedIndex,
    InspectedPrivilege,
    InspectedSequence,
)

T_CREATE = """create table "public"."films" (
    "code" character(5) not null,
    "title" character varying not null,
    "did" bigint not null,
    "date_prod" date,
    "kind" character varying(10),
    "len" interval hour to minute,
    "drange" daterange
);
"""
CV = "character varying"
CV10 = "character varying(10)"
INT = "interval"
INTHM = "interval hour to minute"
PGRANGE = sqlalchemy.dialects.postgresql.ranges.DATERANGE
TD = datetime.timedelta
FILMS_COLUMNS = od(
    [
        ("code", ColumnInfo("code", "character", str, dbtypestr="character(5)")),
        ("title", ColumnInfo("title", "character varying", str)),
        ("did", ColumnInfo("did", "bigint", int)),
        ("date_prod", ColumnInfo("date_prod", "date", datetime.date)),
        ("kind", ColumnInfo("kind", CV, str, dbtypestr=CV10)),
        ("len", ColumnInfo("len", INT, TD, dbtypestr=INTHM)),
        ("drange", ColumnInfo("drange", "daterange", PGRANGE)),
    ]
)
FILMSF_COLUMNS = od(
    [
        ("title", ColumnInfo("title", "character varying", str)),
        ("release_date", ColumnInfo("release_date", "date", datetime.date)),
    ]
)
d1 = ColumnInfo("d", "date", datetime.date)
d2 = ColumnInfo("def_t", "text", str, default="NULL::text")
d3 = ColumnInfo("def_d", "date", datetime.date, default="'2014-01-01'::date")
FILMSF_INPUTS = [d1, d2, d3]
FDEF = """CREATE OR REPLACE FUNCTION public.films_f(d date, def_t text DEFAULT NULL::text, def_d date DEFAULT '2014-01-01'::date)
 RETURNS TABLE(title character varying, release_date date)
 LANGUAGE sql
AS $function$select 'a'::varchar, '2014-01-01'::date$function$
;"""
VDEF = """create or replace view "public"."v_films" as  SELECT films.code,
    films.title,
    films.did,
    films.date_prod,
    films.kind,
    films.len,
    films.drange
   FROM films;
"""
MVDEF = """create materialized view "public"."mv_films" as  SELECT films.code,
    films.title,
    films.did,
    films.date_prod,
    films.kind,
    films.len,
    films.drange
   FROM films;
"""


def test_basic_schemainspect():
    a = ColumnInfo("a", "text", str)
    a2 = ColumnInfo("a", "text", str)
    b = ColumnInfo("b", "varchar", str, dbtypestr="varchar(10)")
    b2 = ColumnInfo(
        "b", "text", str, dbtypestr="text", default="'d'::text", not_null=True
    )
    assert a == a2
    assert a == a
    assert a != b
    assert b != b2
    alter = b2.alter_table_statements(b, "t")
    assert alter == [
        "alter table t alter column \"b\" set default 'd'::text;",
        'alter table t alter column "b" set not null;',
        'alter table t alter column "b" set data type text using "b"::text;',
    ]
    alter = b.alter_table_statements(b2, "t")
    assert alter == [
        'alter table t alter column "b" drop default;',
        'alter table t alter column "b" drop not null;',
        'alter table t alter column "b" set data type varchar(10) using "b"::varchar(10);',
    ]
    b.add_column_clause == 'add column "b"'
    b.drop_column_clause == 'drop column "b"'
    with temporary_database("sqlite") as dburl:
        with raises(NotImplementedError):
            with S(dburl) as s:
                get_inspector(s)


def test_inspected():
    x = schemainspect.Inspected()
    x.name = "b"
    x.schema = "a"
    assert x.quoted_full_name == '"a"."b"'
    assert x.unquoted_full_name == "a.b"
    x = schemainspect.ColumnInfo(name="a", dbtype="integer", pytype=int)
    assert x.creation_clause == '"a" integer'
    x.default = "5"
    x.not_null = True
    assert x.creation_clause == '"a" integer not null default 5'


def test_postgres_objects():
    ex = InspectedExtension("name", "schema", "1.2")
    assert ex.drop_statement == 'drop extension if exists "name";'
    assert (
        ex.create_statement
        == 'create extension if not exists "name" with schema "schema" version \'1.2\';'
    )
    assert ex.update_statement == "alter extension \"name\" update to '1.2';"
    ex2 = deepcopy(ex)
    assert ex == ex2
    ex2.version = "2.1"
    assert ex != ex2

    ex3 = ex2.unversioned_copy()
    assert ex2 != ex3

    assert ex3.update_statement is None

    assert ex3.drop_statement == 'drop extension if exists "name";'
    assert (
        ex3.create_statement
        == 'create extension if not exists "name" with schema "schema";'
    )

    ix = InspectedIndex(
        name="name",
        schema="schema",
        table_name="table",
        key_columns=["y"],
        index_columns=["y"],
        included_columns=[],
        key_options="0",
        num_att="1",
        is_unique=False,
        is_pk=True,
        is_exclusion=False,
        is_immediate=True,
        is_clustered=False,
        key_collations="0",
        key_expressions=None,
        partial_predicate=None,
        algorithm="BRIN",
        definition="create index name on t(x)",
    )
    assert ix.drop_statement == 'drop index if exists "schema"."name";'
    assert ix.create_statement == "create index name on t(x);"
    ix2 = deepcopy(ix)
    assert ix == ix2
    ix2.table_name = "table2"
    assert ix != ix2
    i = InspectedSequence("name", "schema")
    assert i.create_statement == 'create sequence "schema"."name";'
    assert i.drop_statement == 'drop sequence if exists "schema"."name";'
    i2 = deepcopy(i)
    assert i == i2
    i2.schema = "schema2"
    assert i != i2
    i = InspectedEnum("name", "schema", ["a", "b", "c"])
    assert (
        i.create_statement == "create type \"schema\".\"name\" as enum ('a', 'b', 'c');"
    )
    assert i.drop_statement == 'drop type "schema"."name";'
    i2 = InspectedEnum("name", "schema", ["a", "a1", "c", "d"])
    assert i.can_be_changed_to(i)
    assert i != i2
    assert not i.can_be_changed_to(i2)
    i2.elements = ["a", "b"]
    assert i2.can_be_changed_to(i)
    i2.elements = ["b", "a"]
    assert not i2.can_be_changed_to(i)
    i2.elements = ["a", "b", "c"]
    assert i2.can_be_changed_to(i)
    assert i.can_be_changed_to(i2)
    i2.elements = ["a", "a1", "c", "d", "b"]
    assert not i.can_be_changed_to(i2)
    with raises(ValueError):
        i.change_statements(i2)
    i2.elements = ["a0", "a", "a1", "b", "c", "d"]
    assert i.can_be_changed_to(i2)
    assert i.change_statements(i2) == [
        "alter type \"schema\".\"name\" add value 'a0' before 'a';",
        "alter type \"schema\".\"name\" add value 'a1' after 'a';",
        "alter type \"schema\".\"name\" add value 'd' after 'c';",
    ]
    c = InspectedConstraint(
        constraint_type="PRIMARY KEY",
        definition="PRIMARY KEY (code)",
        index="firstkey",
        name="firstkey",
        schema="public",
        table_name="films",
    )
    assert (
        c.create_statement
        == 'alter table "public"."films" add constraint "firstkey" PRIMARY KEY using index "firstkey";'
    )
    c2 = deepcopy(c)
    assert c == c2
    c.index = None
    assert c != c2
    assert (
        c.create_statement
        == 'alter table "public"."films" add constraint "firstkey" PRIMARY KEY (code);'
    )
    assert (
        c.drop_statement == 'alter table "public"."films" drop constraint "firstkey";'
    )


def setup_pg_schema(s):
    s.execute("create table emptytable()")
    s.execute("comment on table emptytable is 'emptytable comment'")
    s.execute("create extension pg_trgm")
    s.execute("create schema otherschema")
    s.execute(
        """
        CREATE TABLE films (
            code        char(5) CONSTRAINT firstkey PRIMARY KEY,
            title       varchar NOT NULL,
            did         bigint NOT NULL,
            date_prod   date,
            kind        varchar(10),
            len         interval hour to minute,
            drange      daterange
        );
        grant select, update, delete, insert on table films to postgres;
    """
    )
    s.execute("""CREATE VIEW v_films AS (select * from films)""")
    s.execute("""CREATE VIEW v_films2 AS (select * from v_films)""")
    s.execute(
        """
            CREATE MATERIALIZED VIEW mv_films
            AS (select * from films)
        """
    )
    s.execute(
        """
            CREATE or replace FUNCTION films_f(d date,
            def_t text default null,
            def_d date default '2014-01-01'::date)
            RETURNS TABLE(
                title character varying,
                release_date date
            )
            as $$select 'a'::varchar, '2014-01-01'::date$$
            language sql;
        """
    )
    s.execute("comment on function films_f(date, text, date) is 'films_f comment'")
    s.execute(
        """
        CREATE OR REPLACE FUNCTION inc_f(integer) RETURNS integer AS $$
        BEGIN
                RETURN $1 + 1;
        END;
        $$ LANGUAGE plpgsql stable;
    """
    )
    s.execute(
        """
        CREATE OR REPLACE FUNCTION inc_f_out(integer, out outparam integer) returns integer AS $$
                select 1;
        $$ LANGUAGE sql;
    """
    )
    s.execute(
        """
        CREATE OR REPLACE FUNCTION inc_f_noargs() RETURNS void AS $$
        begin
            perform 1;
        end;
        $$ LANGUAGE plpgsql stable;
    """
    )
    s.execute(
        """
            create index on films(title);
    """
    )
    s.execute(
        """
            create index on mv_films(title);
    """
    )
    s.execute(
        """
            create type ttt as (a int, b text);
    """
    )
    s.execute(
        """
            create type abc as enum ('a', 'b', 'c');
    """
    )
    s.execute(
        """
            create table t_abc (id serial, x abc);
    """
    )


def n(name, schema="public"):
    return quoted_identifier(name, schema=schema)


def asserts_pg(i, has_timescale=False):
    # schemas
    assert list(i.schemas.keys()) == ["otherschema", "public"]
    otherschema = i.schemas["otherschema"]
    assert i.schemas["public"] != i.schemas["otherschema"]
    assert otherschema.create_statement == 'create schema if not exists "otherschema";'
    assert otherschema.drop_statement == 'drop schema if exists "otherschema";'

    # to_pytype
    assert to_pytype(i.dialect, "integer") == int
    assert to_pytype(i.dialect, "nonexistent") == type(None)  # noqa

    # dialect
    assert i.dialect.name == "postgresql"

    # tables and views
    films = n("films")
    v_films = n("v_films")
    v_films2 = n("v_films2")
    v = i.views[v_films]
    public_views = od((k, v) for k, v in i.views.items() if v.schema == "public")
    assert list(public_views.keys()) == [v_films, v_films2]
    assert v.columns == FILMS_COLUMNS
    assert v.create_statement == VDEF
    assert v == v
    assert v == deepcopy(v)
    assert v.drop_statement == "drop view if exists {};".format(v_films)
    v = i.views[v_films]

    # dependencies
    assert v.dependent_on == [films]
    v = i.views[v_films2]
    assert v.dependent_on == [v_films]

    for k, r in i.relations.items():
        for dependent in r.dependents:
            assert k in i.get_dependency_by_signature(dependent).dependent_on
        for dependency in r.dependent_on:
            assert k in i.get_dependency_by_signature(dependency).dependents

    # materialized views
    mv_films = n("mv_films")
    mv = i.materialized_views[mv_films]
    assert list(i.materialized_views.keys()) == [mv_films]
    assert mv.columns == FILMS_COLUMNS
    assert mv.create_statement == MVDEF
    assert mv.drop_statement == "drop materialized view if exists {};".format(mv_films)

    # materialized view indexes
    assert n("mv_films_title_idx") in mv.indexes

    # functions
    films_f = n("films_f") + "(d date, def_t text, def_d date)"
    inc_f = n("inc_f") + "(integer)"
    inc_f_noargs = n("inc_f_noargs") + "()"
    inc_f_out = n("inc_f_out") + "(integer, OUT outparam integer)"
    public_funcs = [k for k, v in i.functions.items() if v.schema == "public"]
    assert public_funcs == [films_f, inc_f, inc_f_noargs, inc_f_out]
    f = i.functions[films_f]
    f2 = i.functions[inc_f]
    f3 = i.functions[inc_f_noargs]
    f4 = i.functions[inc_f_out]
    assert f == f
    assert f != f2
    assert f.columns == FILMSF_COLUMNS
    assert f.inputs == FILMSF_INPUTS
    assert f3.inputs == []
    assert list(f2.columns.values())[0].name == "inc_f"
    assert list(f3.columns.values())[0].name == "inc_f_noargs"
    assert list(f4.columns.values())[0].name == "outparam"
    fdef = i.functions[films_f].definition
    assert fdef == "select 'a'::varchar, '2014-01-01'::date"
    assert f.create_statement == FDEF
    assert (
        f.drop_statement
        == 'drop function if exists "public"."films_f"(d date, def_t text, def_d date);'
    )
    assert f.comment == "films_f comment"
    assert f2.comment is None

    # extensions
    ext = [
        n("plpgsql", schema="pg_catalog"),
        n("pg_trgm"),
    ]
    if has_timescale:
        ext.append(n("timescaledb"))
    assert [e.quoted_full_name for e in i.extensions.values()] == ext

    # constraints
    cons = i.constraints['"public"."films"."firstkey"']
    assert (
        cons.create_statement
        == 'alter table "public"."films" add constraint "firstkey" PRIMARY KEY using index "firstkey";'
    )

    # tables
    t_films = n("films")
    t = i.tables[t_films]
    empty = i.tables[n("emptytable")]
    assert empty.comment == "emptytable comment"

    # empty tables
    assert empty.columns == od()
    assert (
        empty.create_statement
        == """create table "public"."emptytable" (
);
"""
    )

    # create and drop tables
    assert t.create_statement == T_CREATE
    assert t.drop_statement == "drop table {};".format(t_films)
    assert t.alter_table_statement("x") == "alter table {} x;".format(t_films)

    # table indexes
    assert n("films_title_idx") in t.indexes

    # privileges
    g = InspectedPrivilege("table", "public", "films", "select", "postgres")
    g = i.privileges[g.key]
    assert g.create_statement == 'grant select on table {} to "postgres";'.format(
        t_films
    )
    assert g.drop_statement == 'revoke select on table {} from "postgres";'.format(
        t_films
    )

    # composite types
    ct = i.composite_types[n("ttt")]
    assert [(x.name, x.dbtype) for x in ct.columns.values()] == [
        ("a", "integer"),
        ("b", "text"),
    ]
    assert (
        ct.create_statement == 'create type "public"."ttt" as ("a" integer, "b" text);'
    )
    assert ct.drop_statement == 'drop type "public"."ttt";'

    # enums
    assert i.enums[n("abc")].elements == ["a", "b", "c"]
    x = i.tables[n("t_abc")].columns["x"]
    assert x.is_enum
    assert (
        x.change_enum_to_string_statement("t")
        == 'alter table t alter column "x" set data type varchar using "x"::varchar;'
    )
    assert (
        x.change_string_to_enum_statement("t")
        == 'alter table t alter column "x" set data type abc using "x"::abc;'
    )
    tid = i.tables[n("t_abc")].columns["id"]

    with raises(ValueError):
        tid.change_enum_to_string_statement("t")
    with raises(ValueError):
        tid.change_string_to_enum_statement("t")

    # comments
    assert len(i.comments) == 2
    assert (
        i.comments[
            'function "public"."films_f"(d date, def_t text, def_d date)'
        ].create_statement
        == 'comment on function "public"."films_f"(d date, def_t text, def_d date) is \'films_f comment\';'
    )
    assert (
        i.comments['table "public"."emptytable"'].create_statement
        == 'comment on table "public"."emptytable" is \'emptytable comment\';'
    )


def test_weird_names(db):
    with S(db) as s:
        s.execute("""create table "a(abc=3)"(id text)  """)
        i = get_inspector(s)
        assert list(i.tables.keys())[0] == '"public"."a(abc=3)"'


def test_identity_columns(db):
    with S(db) as s:
        i = get_inspector(s)

        if i.pg_version < 10:
            pytest.skip("identity columns not supported in 9")

        s.execute(
            """create table t(
            a int,
            b int default 1,
            --c int generated always as (1) stored,
            d int generated always as identity,
            e int generated by default as identity
        ) """
        )
        i = get_inspector(s)

        t_key = '"public"."t"'
        assert list(i.tables.keys())[0] == t_key

        t = i.tables[t_key]

        assert list(t.columns) == "a b d e".split()

        EXPECTED = [
            (None, False, False, False),
            ("1", False, False, False),
            # ("1", False, False, True),
            (None, True, True, False),
            (None, True, False, False),
        ]

        cols = list(t.columns.values())

        for c, expected in zip(cols, EXPECTED):
            tup = (c.default, c.is_identity, c.is_identity_always, c.is_generated)

            assert tup == expected

        EXPECTED = [
            '"a" integer',
            '"b" integer default 1',
            # "c" integer generated always as (1) stored',
            '"d" integer generated always as identity not null',
            '"e" integer generated by default as identity not null',
        ]

        for c, expected in zip(cols, EXPECTED):
            assert c.creation_clause == expected


def test_generated_columns(db):
    with S(db) as s:
        i = get_inspector(s)

        if i.pg_version < 12:
            pytest.skip("generated columns not supported in < 12")

        s.execute(
            """create table t(
                c int generated always as (1) stored
        ) """
        )

        i = get_inspector(s)

        t_key = '"public"."t"'
        assert list(i.tables.keys())[0] == t_key

        t = i.tables[t_key]

        EXPECTED = ("1", False, False, True)

        c = t.columns["c"]

        tup = (c.default, c.is_identity, c.is_identity_always, c.is_generated)

        assert tup == EXPECTED

        EXPECTED = '"c" integer generated always as (1) stored'

        assert c.creation_clause == EXPECTED


def test_sequences(db):
    with S(db) as s:
        i = get_inspector(s)

        if i.pg_version < 10:
            pytest.skip("identity columns not supported in 9")

        s.execute(
            """
        create table t(id serial);
        """
        )

        s.execute(
            """
        CREATE SEQUENCE serial START 101;
        """
        )

        s.execute(
            """
        create table t2(id integer generated always as identity);
        """
        )

        i = get_inspector(s)

        seqs = list(i.sequences)

        assert seqs == ['"public"."serial"', '"public"."t_id_seq"']

        unowned = i.sequences['"public"."serial"']
        assert unowned.table_name is None

        owned = i.sequences['"public"."t_id_seq"']
        assert owned.table_name == "t"
        assert owned.quoted_full_table_name == '"public"."t"'
        assert owned.quoted_table_and_column_name == '"public"."t"."id"'


def test_postgres_inspect(db, pytestconfig):
    if pytestconfig.getoption("timescale"):
        pytest.skip("--timescale was specified")
    else:
        assert_postgres_inspect(db)


@pytest.mark.timescale
def test_timescale_inspect(db):
    assert_postgres_inspect(db, has_timescale=True)


def assert_postgres_inspect(db, has_timescale=False):
    with S(db) as s:
        if has_timescale:
            s.execute("create extension if not exists timescaledb;")
        setup_pg_schema(s)
        i = get_inspector(s)
        asserts_pg(i, has_timescale)
        assert i == i == get_inspector(s)


def test_empty():
    x = NullInspector()
    assert x.tables == od()
    assert x.relations == od()
    assert type(schemainspect.get_inspector(None)) == NullInspector


@contextmanager
def transaction_cursor(db):
    conn = psycopg2.connect(db)
    try:
        with conn:
            with conn.cursor(cursor_factory=NamedTupleCursor) as curs:
                yield curs
    finally:
        conn.close()


def test_raw_connection(db):
    with S(db) as s:
        setup_pg_schema(s)

    with S(db) as s:
        i1 = get_inspector(s)

    with transaction_cursor(db) as c:
        i2 = get_inspector(c)

    assert i1 == i2
