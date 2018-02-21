from __future__ import (absolute_import, division, print_function, unicode_literals)

from collections import OrderedDict as od

import datetime
from pytest import raises

import sqlalchemy.exc
import sqlalchemy.dialects.postgresql
import six
from copy import deepcopy

from sqlbag import temporary_database, S

from schemainspect.misc import quoted_identifier

import schemainspect
from schemainspect import get_inspector, NullInspector, to_pytype
from schemainspect.inspected import ColumnInfo

from schemainspect.pg import InspectedIndex, InspectedSequence, InspectedConstraint, InspectedExtension, InspectedEnum

if not six.PY2:
    unicode = str

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

CV = 'character varying'
CV10 = 'character varying(10)'
INT = 'interval'
INTHM = 'interval hour to minute'
PGRANGE = sqlalchemy.dialects.postgresql.ranges.DATERANGE
TD = datetime.timedelta


FILMS_COLUMNS = od([
    ('code', ColumnInfo(
        'code', 'character', str, dbtypestr='character(5)')),
    ('title', ColumnInfo('title', 'character varying', str)),
    ('did', ColumnInfo('did', 'bigint', int)),
    ('date_prod', ColumnInfo('date_prod', 'date', datetime.date)),
    ('kind', ColumnInfo('kind', CV, str, dbtypestr=CV10)),
    ('len', ColumnInfo('len', INT, TD, dbtypestr=INTHM)),
    (u'drange', ColumnInfo('drange', 'daterange', PGRANGE))
])

FILMSF_COLUMNS = od([
    ('title', ColumnInfo('title', 'character varying', str)),
    ('release_date', ColumnInfo('release_date', 'date', datetime.date))
])

d1 = ColumnInfo('d', 'date', datetime.date)
d2 = ColumnInfo('def_t', 'text', str, default='NULL::text')
d3 = ColumnInfo('def_d', 'date', datetime.date, default="'2014-01-01'::date")
FILMSF_INPUTS = [d1, d2, d3]

FDEF = """create or replace function "public"."films_f"(d date, def_t text, def_d date)
returns TABLE(title character varying, release_date date) as
$$select 'a'::varchar, '2014-01-01'::date$$
language SQL VOLATILE CALLED ON NULL INPUT SECURITY INVOKER;"""

VDEF = """create view "public"."v_films" as  SELECT films.code,
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
    a = ColumnInfo('a', 'text', str)
    a2 = ColumnInfo('a', 'text', str)

    b = ColumnInfo('b', 'varchar', str, dbtypestr='varchar(10)')
    b2 = ColumnInfo(
        'b',
        'text',
        str,
        dbtypestr='text',
        default="'d'::text",
        not_null=True)

    assert a == a2
    assert a == a
    assert a != b
    assert b != b2

    alter = b2.alter_table_statements(b, 't')

    assert alter == [
        'alter table t alter column "b" set default \'d\'::text;',
        'alter table t alter column "b" set not null;',
        'alter table t alter column "b" set data type text;']

    alter = b.alter_table_statements(b2, 't')

    assert alter == [
        'alter table t alter column "b" drop default;',
        'alter table t alter column "b" drop not null;',
        'alter table t alter column "b" set data type varchar(10);']

    b.add_column_clause == 'add column "b"'
    b.drop_column_clause == 'drop column "b"'

    with temporary_database('sqlite') as dburl:
        with raises(NotImplementedError):
            with S(dburl) as s:
                get_inspector(s)


def test_inspected():
    x = schemainspect.Inspected()
    x.name = 'b'
    x.schema = 'a'
    assert x.quoted_full_name == '"a"."b"'
    assert x.unquoted_full_name == 'a.b'

    x = schemainspect.ColumnInfo(name='a', dbtype='integer', pytype=int)
    assert x.creation_clause == '"a" integer'
    x.default = "5"
    x.not_null = True
    assert x.creation_clause == '"a" integer not null default 5'


def test_postgres_objects():
    ex = InspectedExtension('name', 'schema', '1.2')
    assert ex.drop_statement == 'drop extension if exists "name";'
    assert ex.create_statement == \
        'create extension "name" with schema "schema" version \'1.2\';'
    assert ex.update_statement == \
        'alter extension "schema"."name" update to version \'1.2\';'

    ex2 = deepcopy(ex)
    assert ex == ex2
    ex2.version = '2.1'
    assert ex != ex2

    ix = InspectedIndex('name', 'schema', 'table', 'create index name on t(x)')
    assert ix.drop_statement == 'drop index if exists "schema"."name";'
    assert ix.create_statement == \
        'create index name on t(x);'

    ix2 = deepcopy(ix)
    assert ix == ix2
    ix2.definition = 'create index name on t(y)'
    assert ix != ix2

    i = InspectedSequence('name', 'schema')
    assert i.create_statement == 'create sequence "schema"."name";'
    assert i.drop_statement == 'drop sequence if exists "schema"."name";'
    i2 = deepcopy(i)
    assert i == i2
    i2.schema = 'schema2'
    assert i != i2

    i = InspectedEnum('name', 'schema', ['a', 'b', 'c'])
    assert i.create_statement == "create type \"schema\".\"name\" as enum ('a', 'b', 'c');"
    assert i.drop_statement == 'drop type "schema"."name";'
    i2 = InspectedEnum('name', 'schema', ['a', 'a1', 'c', 'd'])
    assert i.can_be_changed_to(i)
    assert i != i2
    assert not i.can_be_changed_to(i2)
    i2.elements = ['a', 'b']
    assert i2.can_be_changed_to(i)
    i2.elements = ['b', 'a']
    assert not i2.can_be_changed_to(i)
    i2.elements = ['a', 'b', 'c']
    assert i2.can_be_changed_to(i)
    assert i.can_be_changed_to(i2)

    i2.elements = ['a', 'a1', 'c', 'd', 'b']
    assert not i.can_be_changed_to(i2)

    with raises(ValueError):
        i.change_statements(i2)

    i2.elements = ['a0', 'a', 'a1', 'b', 'c', 'd']
    assert i.can_be_changed_to(i2)

    assert i.change_statements(i2) == [
        "alter type \"schema\".\"name\" add value 'a0' before 'a'",
        "alter type \"schema\".\"name\" add value 'a1' after 'a'",
        "alter type \"schema\".\"name\" add value 'd' after 'c'"]

    c = InspectedConstraint(
        constraint_type='PRIMARY KEY',
        definition='PRIMARY KEY (code)',
        index='firstkey',
        name='firstkey',
        schema='public',
        table_name='films')

    assert c.create_statement == \
        'alter table "public"."films" add constraint "firstkey" PRIMARY KEY using index "firstkey";'

    c2 = deepcopy(c)
    assert c == c2
    c.index = None
    assert c != c2
    assert c.create_statement == 'alter table "public"."films" add constraint "firstkey" PRIMARY KEY (code);'
    assert c.drop_statement == 'alter table "public"."films" drop constraint "firstkey";'


def setup_pg_schema(s):
    s.execute('create extension pg_trgm')

    s.execute('create schema otherschema')

    s.execute("""
        CREATE TABLE films (
            code        char(5) CONSTRAINT firstkey PRIMARY KEY,
            title       varchar NOT NULL,
            did         bigint NOT NULL,
            date_prod   date,
            kind        varchar(10),
            len         interval hour to minute,
            drange      daterange
        );
    """)

    s.execute("""CREATE VIEW v_films AS (select * from films)""")

    s.execute("""CREATE VIEW v_films2 AS (select * from v_films)""")

    s.execute("""
            CREATE MATERIALIZED VIEW mv_films
            AS (select * from films)
        """)

    s.execute("""
            CREATE or replace FUNCTION films_f(d date,
            def_t text default null,
            def_d date default '2014-01-01'::date)
            RETURNS TABLE(
                title character varying,
                release_date date
            )
            as $$select 'a'::varchar, '2014-01-01'::date$$
            language sql;
        """)

    s.execute("""
        CREATE OR REPLACE FUNCTION inc_f(integer) RETURNS integer AS $$
        BEGIN
                RETURN $1 + 1;
        END;
        $$ LANGUAGE plpgsql stable;
    """)

    s.execute("""
        CREATE OR REPLACE FUNCTION inc_f_out(integer, out outparam integer) returns integer AS $$
                select 1;
        $$ LANGUAGE sql;
    """)

    s.execute("""
        CREATE OR REPLACE FUNCTION inc_f_noargs() RETURNS void AS $$
        begin
            perform 1;
        end;
        $$ LANGUAGE plpgsql stable;
    """)

    s.execute("""
            create index on films(title);
    """)

    s.execute("""
            create index on mv_films(title);
    """)

    s.execute("""
            create type ttt as (a int, b text);
    """)

    s.execute("""
            create type abc as enum ('a', 'b', 'c');
    """)

    s.execute("""
            create table t_abc (id serial, x abc);
    """)


def asserts_pg(i):
    assert list(i.schemas.keys()) == [
        'public',
        'otherschema'
    ]

    otherschema = i.schemas['otherschema']

    assert i.schemas['public'] != i.schemas['otherschema']

    assert otherschema.create_statement == 'create schema if not exists "otherschema";'

    assert otherschema.drop_statement == 'drop schema if exists "otherschema";'

    assert to_pytype(i.dialect, 'integer') == int
    assert to_pytype(i.dialect, 'nonexistent') == type(None)  # noqa

    def n(name, schema='public'):
        return quoted_identifier(name, schema=schema)

    assert i.dialect.name == 'postgresql'

    films = n('films')
    v_films = n('v_films')
    v_films2 = n('v_films2')

    v = i.views[v_films]

    public_views = od(
        (k, v)
        for k, v
        in i.views.items()
        if v.schema == 'public'
    )

    assert list(public_views.keys()) == [v_films, v_films2]
    assert v.columns == FILMS_COLUMNS
    assert v.create_statement == VDEF
    assert v == v
    assert v == deepcopy(v)
    assert v.drop_statement == \
        'drop view if exists {} cascade;'.format(v_films)

    v = i.views[v_films]
    assert v.dependent_on == [films]

    v = i.views[v_films2]
    assert v.dependent_on == [v_films]

    for k, r in i.relations.items():
        for dependent in r.dependents:
            assert k in i.relations[dependent].dependent_on

        for dependency in r.dependent_on:
            assert k in i.relations[dependency].dependents

    mv_films = n('mv_films')
    mv = i.materialized_views[mv_films]
    assert list(i.materialized_views.keys()) == [mv_films]
    assert mv.columns == FILMS_COLUMNS
    assert mv.create_statement == MVDEF
    assert mv.drop_statement == \
        'drop materialized view if exists {} cascade;'.format(mv_films)

    assert n('mv_films_title_idx') in mv.indexes

    films_f = n('films_f') + '(d date, def_t text, def_d date)'
    inc_f = n('inc_f') + '(integer)'
    inc_f_noargs = n('inc_f_noargs') + '()'
    inc_f_out = n('inc_f_out') + '(integer, OUT outparam integer)'

    public_funcs = \
        [k for k, v in i.functions.items() if v.schema == 'public']
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

    assert list(f2.columns.values())[0].name == 'inc_f'
    assert list(f3.columns.values())[0].name == 'inc_f_noargs'
    assert list(f4.columns.values())[0].name == 'outparam'

    fdef = i.functions[films_f].definition
    assert fdef == "select 'a'::varchar, '2014-01-01'::date"
    assert f.create_statement == FDEF
    assert f.drop_statement == \
        'drop function if exists "public"."films_f"(d date, def_t text, def_d date) cascade;'

    assert [e.quoted_full_name for e in i.extensions.values()] == \
        [n('plpgsql', schema='pg_catalog'), n('pg_trgm')]

    cons = i.constraints[n('firstkey')]
    assert cons.create_statement == 'alter table "public"."films" add constraint "firstkey" PRIMARY KEY using index "firstkey";'

    t_films = n('films')
    t = i.tables[t_films]
    assert t.create_statement == T_CREATE
    assert t.drop_statement == 'drop table {};'.format(t_films)
    assert t.alter_table_statement('x') == 'alter table {} x;'.format(t_films)

    assert n('films_title_idx') in t.indexes

    ct = i.composite_types[n('ttt')]
    assert [(x.name, x.dbtype) for x in ct.columns.values()] == \
        [('a', 'integer'), ('b', 'text')]
    assert ct.create_statement == \
        'create type "public"."ttt" as ("a" integer, "b" text);'
    assert ct.drop_statement == \
        'drop type "public"."ttt";'
    assert i.enums[n('abc')].elements == ['a', 'b', 'c']

    x = i.tables[n('t_abc')].columns['x']
    assert x.is_enum
    assert x.change_enum_to_string_statement('t') == \
        'alter table t alter column "x" set data type varchar;'
    assert x.change_string_to_enum_statement('t') == \
        'alter table t alter column "x" set data type abc using "x"::abc;'

    tid = i.tables[n('t_abc')].columns['id']
    with raises(ValueError):
        tid.change_enum_to_string_statement('t')

    with raises(ValueError):
        tid.change_string_to_enum_statement('t')


def test_postgres_inspect(db):
    with S(db) as s:
        setup_pg_schema(s)
        i = get_inspector(s)
        asserts_pg(i)
        assert i == i == get_inspector(s)


def test_empty():
    x = NullInspector()
    assert x.tables == od()
    assert x.relations == od()

    assert type(schemainspect.get_inspector(None)) == NullInspector
