# `schemainspect`: SQL Schema Inspection

Schema inspection for PostgreSQL (and potentially others in the future).

Inspects tables, views, materialized views, constraints, indexes, sequences, enums, functions, and extensions. Handles table partitioning and inheritance.

**Limitations:** Function inspection only confirmed to work with SQL/PLPGSQL languages so far.

## Basic Usage

Get an inspection object from an already opened SQLAlchemy session or connection as follows:

    from schemainspect import get_inspector
    from sqlbag import S

    with S('postgresql:///example') as s:
        i = get_inspector(s)

The inspection object has attributes for tables, views, and all the other things it tracks. At each of these attributes you'll find a dictionary (OrderedDict) mapping from fully-qualified-and-quoted-name-of-thing-in-database to information object.

For instance, the information about a table *books* would be accessed as follows:

    >>> books_table = i.tables['"public"."books"']
    >>> books_table.name
    'books'
    >>> books_table.schema
    'public'
    >>> [each.name for each in books_table.columns]
    ['id', 'title', 'isbn']


## Documentation

Documentation is a bit patchy at the moment. Watch this space!


## Author Credits

Initial development, maintenance:

- [djrobstep](https://github.com/djrobstep)

Contributions:

- [BenSjoberg](https://github.com/BenSjoberg)
- [johto](https://github.com/johto)


## Install

Install with [pip](https://pip.pypa.io):

    $ pip install schemainspect

To install psycopg2 (the PostgreSQL driver) at the same time as well:

    $ pip install schemainspect[pg]
