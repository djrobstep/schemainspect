from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from ..inspector import DBInspector
from ..inspected import ColumnInfo, Inspected
from ..inspected import InspectedSelectable as BaseInspectedSelectable
from ..misc import resource_text, quoted_identifier
from collections import OrderedDict as od
from itertools import groupby

CREATE_TABLE = """create table {} (
    {}
);
"""


class InspectedSelectable(BaseInspectedSelectable):
    @property
    def create_statement(self):
        if self._create_statement:
            return self._create_statement

        n = self.quoted_full_name

        if self.relationtype == 'r':
            colspec = ',\n    '.join(c.creation_sql
                                     for c in self.columns.values())
            create_statement = CREATE_TABLE.format(n, colspec)
        elif self.relationtype == 'v':
            create_statement = 'create view {} as {}\n'.format(n,
                                                               self.definition)
        elif self.relationtype == 'm':
            create_statement = 'create materialized view {} as {}\n'.format(
                n, self.definition)
        else:
            raise NotImplementedError  # pragma: no cover
        return create_statement

    @property
    def drop_statement(self):
        if self._drop_statement:
            return self._drop_statement

        n = self.quoted_full_name

        if self.relationtype == 'r':
            drop_statement = \
                'drop table {};'.format(n)
        elif self.relationtype == 'v':
            drop_statement = \
                'drop view if exists {} cascade;'.format(n)
        elif self.relationtype == 'm':
            drop_statement = \
                'drop materialized view if exists {} cascade;'.format(n)
        else:
            raise NotImplementedError  # pragma: no cover
        return drop_statement


class InspectedIndex(Inspected):
    def __init__(self, name, schema, table_name, definition=None):
        self.name = name
        self.schema = schema
        self.definition = definition
        self.table_name = table_name

    @property
    def drop_statement(self):
        return 'drop index {};'.format(self.quoted_full_name)

    @property
    def create_statement(self):
        return '{};'.format(self.definition)

    def __eq__(self, other):
        equalities = \
            self.name == other.name, \
            self.schema == other.schema, \
            self.table_name == other.table_name, \
            self.definition == other.definition
        return all(equalities)


class InspectedSequence(Inspected):
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    @property
    def drop_statement(self):
        return 'drop sequence {};'.format(self.quoted_full_name)

    @property
    def create_statement(self):
        return 'create sequence {};'.format(self.quoted_full_name)

    def __eq__(self, other):
        equalities = \
            self.name == other.name, \
            self.schema == other.schema
        return all(equalities)


class InspectedExtension(Inspected):
    def __init__(self, name, schema, version):

        self.name = name
        self.schema = schema
        self.version = version

    @property
    def drop_statement(self):
        return 'drop extension if exists {};'.format(self.quoted_name)

    @property
    def create_statement(self):
        return \
            "create extension {} with schema {} version '{}';"\
            .format(
                self.quoted_name,
                self.quoted_schema,
                self.version)

    @property
    def update_statement(self):
        return \
            "alter extension {} update to version '{}';"\
            .format(
                self.quoted_full_name,
                self.version)

    def __eq__(self, other):
        equalities = \
            self.name == other.name, \
            self.schema == other.schema, \
            self.version == other.version
        return all(equalities)


class InspectedConstraint(Inspected):
    def __init__(self, name, schema, constraint_type, table_name, definition,
                 is_index):
        self.name = name
        self.schema = schema
        self.constraint_type = constraint_type
        self.table_name = table_name
        self.definition = definition
        self.is_index = is_index

    @property
    def quoted_full_table_name(self):
        return '{}.{}'.format(
            quoted_identifier(self.schema), quoted_identifier(self.table_name))

    @property
    def drop_statement(self):
        return \
            'alter table {} drop constraint {};'.format(
                self.quoted_full_table_name,
                self.quoted_name)

    @property
    def create_statement(self):
        USING = 'alter table {} add constraint {} {} using index {};'
        NOT_USING = 'alter table {} add constraint {} {};'

        if self.is_index:
            return USING.format(self.quoted_full_table_name, self.quoted_name,
                                self.constraint_type, self.quoted_name)
        else:
            return NOT_USING.format(self.quoted_full_table_name,
                                    self.quoted_name, self.definition)

    def __eq__(self, other):
        equalities = \
            self.name == other.name, \
            self.schema == other.schema, \
            self.table_name == other.table_name, \
            self.definition == other.definition, \
            self.is_index == other.is_index
        return all(equalities)


class PostgreSQL(DBInspector):
    def load_all(self):
        self.load_all_relations()
        self.load_functions()
        self.selectables = od()
        self.selectables.update(self.relations)
        self.selectables.update(self.functions)

    def load_all_relations(self):
        self.tables = od()
        self.views = od()
        self.materialized_views = od()

        q = self.c.execute(self.ALL_RELATIONS_QUERY)

        for _, g in groupby(q, lambda x: (x.relationtype, x.fullname)):
            clist = list(g)
            name = clist[0].name
            schema = clist[0].schema
            relationtype = clist[0].relationtype
            definition = clist[0].definition

            columns = [ColumnInfo(
                name=c.attname,
                dbtype=c.datatype,
                dbtypestr=c.datatypestring,
                pytype=self.to_pytype(c.datatype),
                default=c.defaultdef,
                not_null=c.not_null) for c in clist]

            s = InspectedSelectable(
                name=name,
                schema=schema,
                columns=od((c.name, c) for c in columns),
                relationtype=relationtype,
                definition=definition)

            if relationtype == 'r':
                self.tables[s.quoted_full_name] = s
            elif relationtype == 'v':
                self.views[s.quoted_full_name] = s
            elif relationtype == 'm':
                self.materialized_views[s.quoted_full_name] = s

        self.relations = od()

        for x in (self.tables, self.views, self.materialized_views):
            self.relations.update(x)

        q = self.c.execute(self.INDEXES_QUERY)

        indexlist = [
            InspectedIndex(
                name=i.name,
                schema=i.schema,
                definition=i.definition,
                table_name=i.table_name) for i in q
        ]

        self.indexes = od((i.quoted_full_name, i) for i in indexlist)

        q = self.c.execute(self.SEQUENCES_QUERY)

        sequencelist = [
            InspectedSequence(
                name=i.name, schema=i.schema) for i in q
        ]

        self.sequences = od((i.quoted_full_name, i) for i in sequencelist)

        q = self.c.execute(self.CONSTRAINTS_QUERY)

        constraintlist = [
            InspectedConstraint(
                name=i.name,
                schema=i.schema,
                constraint_type=i.constraint_type,
                table_name=i.table_name,
                definition=i.definition,
                is_index=i.is_index) for i in q
        ]

        self.constraints = od((i.quoted_full_name, i) for i in constraintlist)

        q = self.c.execute(self.EXTENSIONS_QUERY)

        extensionlist = [
            InspectedExtension(
                name=i.name, schema=i.schema, version=i.version) for i in q
        ]

        # extension names are unique per-database rather than per-schema like other things (even though extensions are assigned to a particular schema)
        self.extensions = od((i.name, i) for i in extensionlist)

    def load_functions(self):
        self.functions = od()

        q = self.c.execute(self.FUNCTIONS_QUERY)

        for _, g in groupby(q, lambda x: (x.db, x.schema, x.name)):
            clist = list(g)

            name = clist[0].name
            schema = clist[0].schema
            returntype = clist[0].returntype
            drop_statement = clist[0].drop_statement
            create_statement = clist[0].create_statement
            definition = clist[0].definition

            outs = [c for c in clist if c.parameter_mode == 'OUT']

            if returntype == 'record':
                columns = [ColumnInfo(
                    name=c.parameter_name,
                    dbtype=c.data_type,
                    pytype=self.to_pytype(c.data_type)) for c in outs]
            else:
                columns = [ColumnInfo(
                    name=None,
                    dbtype=returntype,
                    pytype=self.to_pytype(returntype))]

            plist = [ColumnInfo(
                name=c.parameter_name,
                dbtype=c.data_type,
                pytype=self.to_pytype(c.data_type),
                default=c.parameter_default)
                for c in clist if c.parameter_mode == 'IN']

            s = InspectedSelectable(
                schema=schema,
                name=name,
                columns=od((c.name, c) for c in columns),
                inputs=plist,
                drop_statement=drop_statement,
                create_statement=create_statement,
                relationtype='function',
                definition=definition)

            self.functions[s.quoted_full_name] = s

    ALL_RELATIONS_QUERY = resource_text('relations.sql')
    INDEXES_QUERY = resource_text('indexes.sql')
    SEQUENCES_QUERY = resource_text('sequences.sql')
    CONSTRAINTS_QUERY = resource_text('constraints.sql')
    FUNCTIONS_QUERY = resource_text('functions.sql')
    EXTENSIONS_QUERY = resource_text('extensions.sql')
