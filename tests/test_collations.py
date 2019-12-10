from sqlbag import S

from schemainspect import get_inspector


def test_collations(db):
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
);

create schema x;

CREATE COLLATION x.german (provider = icu, locale = 'de-DE-x-icu');
CREATE COLLATION naturalsort (provider = icu, locale = 'en-u-kn-true');
        """
        )

        i = get_inspector(s)
    assert list(i.collations) == ['"public"."naturalsort"', '"x"."german"']

    gc = i.collations['"x"."german"']
    assert (
        gc.create_statement
        == """create collation if not exists "x"."german" (provider = 'icu', locale = 'de-DE-x-icu');"""
    )

    nc = i.collations['"public"."naturalsort"']
    assert (
        nc.create_statement
        == """create collation if not exists "public"."naturalsort" (provider = 'icu', locale = 'en-u-kn-true');"""
    )

    assert gc == gc
    assert gc != nc

    with S(db) as s:
        s.execute(
            """
CREATE TABLE tt (
    id         int,
    t text,
    tde text collate "POSIX"
);
        """
        )

    i = get_inspector(s)
    tt = i.tables['"public"."tt"']
    t = tt.columns["t"]
    tde = tt.columns["tde"]
    assert t.collation is None
    assert tde.collation == "POSIX"

    assert (
        t.alter_data_type_clause
        == 'alter column "t" set data type text using "t"::text'
    )
    assert (
        tde.alter_data_type_clause
        == 'alter column "tde" set data type text collate "POSIX" using "tde"::text'
    )
