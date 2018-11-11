from __future__ import absolute_import, division, print_function, unicode_literals

from collections import OrderedDict as od
from itertools import groupby

from sqlalchemy import text

from ..inspected import ColumnInfo, Inspected
from ..inspected import InspectedSelectable as BaseInspectedSelectable
from ..inspected import TableRelated
from ..inspector import DBInspector
from ..misc import quoted_identifier, resource_text

CREATE_TABLE = """create table {} ({}
);
"""
CREATE_FUNCTION_FORMAT = """create or replace function {signature}
returns {result_string} as
$${definition}$$
language {language} {volatility} {strictness} {security_type};"""
ALL_RELATIONS_QUERY = resource_text("sql/relations.sql")
SCHEMAS_QUERY = resource_text("sql/schemas.sql")
INDEXES_QUERY = resource_text("sql/indexes.sql")
SEQUENCES_QUERY = resource_text("sql/sequences.sql")
CONSTRAINTS_QUERY = resource_text("sql/constraints.sql")
FUNCTIONS_QUERY = resource_text("sql/functions.sql")
EXTENSIONS_QUERY = resource_text("sql/extensions.sql")
ENUMS_QUERY = resource_text("sql/enums.sql")
DEPS_QUERY = resource_text("sql/deps.sql")
PRIVILEGES_QUERY = resource_text("sql/privileges.sql")
TRIGGERS_QUERY = resource_text("sql/triggers.sql")


class InspectedSelectable(BaseInspectedSelectable):
    @property
    def create_statement(self):
        n = self.quoted_full_name
        if self.relationtype == "r":
            colspec = ",\n".join(
                "    " + c.creation_clause for c in self.columns.values()
            )
            if colspec:
                colspec = "\n" + colspec

            create_statement = CREATE_TABLE.format(n, colspec)
        elif self.relationtype == "v":
            create_statement = "create view {} as {}\n".format(n, self.definition)
        elif self.relationtype == "m":
            create_statement = "create materialized view {} as {}\n".format(
                n, self.definition
            )
        elif self.relationtype == "c":
            colspec = ", ".join(c.creation_clause for c in self.columns.values())
            create_statement = "create type {} as ({});".format(n, colspec)
        else:
            raise NotImplementedError  # pragma: no cover

        return create_statement

    @property
    def drop_statement(self):
        n = self.quoted_full_name
        if self.relationtype == "r":
            drop_statement = "drop table {};".format(n)
        elif self.relationtype == "v":
            drop_statement = "drop view if exists {} cascade;".format(n)
        elif self.relationtype == "m":
            drop_statement = "drop materialized view if exists {} cascade;".format(n)
        elif self.relationtype == "c":
            drop_statement = "drop type {};".format(n)
        else:
            raise NotImplementedError  # pragma: no cover

        return drop_statement

    def alter_table_statement(self, clause):
        if self.relationtype == "r":
            alter = "alter table {} {};".format(self.quoted_full_name, clause)
        else:
            raise NotImplementedError  # pragma: no cover

        return alter


class InspectedFunction(InspectedSelectable):
    def __init__(
        self,
        name,
        schema,
        columns,
        inputs,
        definition,
        volatility,
        strictness,
        security_type,
        identity_arguments,
        result_string,
        language,
        full_definition,
        comment,
    ):
        self.identity_arguments = identity_arguments
        self.result_string = result_string
        self.language = language
        self.volatility = volatility
        self.strictness = strictness
        self.security_type = security_type
        self.full_definition = full_definition

        super(InspectedFunction, self).__init__(
            name=name,
            schema=schema,
            columns=columns,
            inputs=inputs,
            definition=definition,
            relationtype="f",
            comment=comment,
        )

    @property
    def signature(self):
        return "{}({})".format(self.quoted_full_name, self.identity_arguments)

    @property
    def create_statement(self):
        return self.full_definition + ";"
        """
        return CREATE_FUNCTION_FORMAT.format(
            signature=self.signature,
            result_string=self.result_string,
            definition=self.definition,
            language=self.language,
            volatility=self.volatility,
            strictness=self.strictness,
            security_type=self.security_type,
        )
        """

    @property
    def drop_statement(self):
        return "drop function if exists {} cascade;".format(self.signature)

    def __eq__(self, other):
        return (
            self.signature == other.signature
            and self.result_string == other.result_string
            and self.definition == other.definition
            and self.language == other.language
            and self.volatility == other.volatility
            and self.strictness == other.strictness
            and self.security_type == other.security_type
        )


class InspectedTrigger(Inspected):
    def __init__(self, name, schema, full_definition):
        self.name, self.schema, self.full_definition = name, schema, full_definition

    @property
    def drop_statement(self):
        return 'drop trigger if exists {};'.format(self.quoted_full_name)

    @property
    def create_statement(self):
        return self.full_definition + ';'

    def __eq__(self, other):
        """
        :type other: InspectedTrigger
        :rtype: bool
        """
        return self.name == other.name and self.schema == other.schema and self.full_definition == other.full_definition


class InspectedIndex(Inspected, TableRelated):
    def __init__(self, name, schema, table_name, definition=None):
        self.name = name
        self.schema = schema
        self.definition = definition
        self.table_name = table_name

    @property
    def drop_statement(self):
        return "drop index if exists {};".format(self.quoted_full_name)

    @property
    def create_statement(self):
        return "{};".format(self.definition)

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.table_name == other.table_name,
            self.definition == other.definition,
        )
        return all(equalities)


class InspectedSequence(Inspected):
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    @property
    def drop_statement(self):
        return "drop sequence if exists {};".format(self.quoted_full_name)

    @property
    def create_statement(self):
        return "create sequence {};".format(self.quoted_full_name)

    def __eq__(self, other):
        equalities = self.name == other.name, self.schema == other.schema
        return all(equalities)


class InspectedEnum(Inspected):
    def __init__(self, name, schema, elements):
        self.name = name
        self.schema = schema
        self.elements = elements

    @property
    def drop_statement(self):
        return "drop type {};".format(self.quoted_full_name)

    @property
    def create_statement(self):
        return "create type {} as enum ({});".format(
            self.quoted_full_name, self.quoted_elements
        )

    @property
    def quoted_elements(self):
        quoted = ["'{}'".format(e) for e in self.elements]
        return ", ".join(quoted)

    def change_statements(self, new):
        if not self.can_be_changed_to(new):
            raise ValueError

        new = new.elements
        old = self.elements
        statements = []
        previous = None
        for c in new:
            if c not in old:
                if not previous:
                    s = "alter type {} add value '{}' before '{}'".format(
                        self.quoted_full_name, c, old[0]
                    )
                else:
                    s = "alter type {} add value '{}' after '{}'".format(
                        self.quoted_full_name, c, previous
                    )
                statements.append(s)
            previous = c
        return statements

    def can_be_changed_to(self, new):
        old = self.elements
        # new must already have the existing items from old, in the same order
        return [e for e in new.elements if e in old] == old

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.elements == other.elements,
        )
        return all(equalities)


class InspectedSchema(Inspected):
    def __init__(self, schema):
        self.schema = schema
        self.name = None

    @property
    def create_statement(self):
        return "create schema if not exists {};".format(self.quoted_schema)

    @property
    def drop_statement(self):
        return "drop schema if exists {};".format(self.quoted_schema)

    def __eq__(self, other):
        return self.schema == other.schema


class InspectedExtension(Inspected):
    def __init__(self, name, schema, version):
        self.name = name
        self.schema = schema
        self.version = version

    @property
    def drop_statement(self):
        return "drop extension if exists {};".format(self.quoted_name)

    @property
    def create_statement(self):
        return "create extension if not exists {} with schema {} version '{}';".format(
            self.quoted_name, self.quoted_schema, self.version
        )

    @property
    def update_statement(self):
        return "alter extension {} update to version '{}';".format(
            self.quoted_full_name, self.version
        )

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.version == other.version,
        )
        return all(equalities)


class InspectedConstraint(Inspected, TableRelated):
    def __init__(self, name, schema, constraint_type, table_name, definition, index):
        self.name = name
        self.schema = schema
        self.constraint_type = constraint_type
        self.table_name = table_name
        self.definition = definition
        self.index = index

    @property
    def drop_statement(self):
        return "alter table {} drop constraint {};".format(
            self.quoted_full_table_name, self.quoted_name
        )

    @property
    def create_statement(self):
        USING = "alter table {} add constraint {} {} using index {};"
        NOT_USING = "alter table {} add constraint {} {};"
        if self.index:
            return USING.format(
                self.quoted_full_table_name,
                self.quoted_name,
                self.constraint_type,
                self.quoted_name,
            )

        else:
            return NOT_USING.format(
                self.quoted_full_table_name, self.quoted_name, self.definition
            )

    @property
    def quoted_full_name(self):
        return "{}.{}.{}".format(
            quoted_identifier(self.schema),
            quoted_identifier(self.table_name),
            quoted_identifier(self.name),
        )

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.table_name == other.table_name,
            self.definition == other.definition,
            self.index == other.index,
        )
        return all(equalities)


class InspectedPrivilege(Inspected):
    def __init__(self, object_type, schema, name, privilege, target_user):
        self.schema = schema
        self.object_type = object_type
        self.name = name
        self.privilege = privilege.lower()
        self.target_user = target_user

    @property
    def drop_statement(self):
        return "revoke {} on {} {} from {};".format(
            self.privilege, self.object_type, self.quoted_full_name, self.target_user
        )

    @property
    def create_statement(self):
        return "grant {} on {} {} to {};".format(
            self.privilege, self.object_type, self.quoted_full_name, self.target_user
        )

    def __eq__(self, other):
        equalities = (
            self.schema == other.schema,
            self.object_type == other.object_type,
            self.name == other.name,
            self.privilege == other.privilege,
            self.target_user == other.target_user,
        )
        return all(equalities)

    @property
    def key(self):
        return self.object_type, self.quoted_full_name, self.target_user, self.privilege


class PostgreSQL(DBInspector):
    def __init__(self, c, include_internal=False):
        def processed(q):
            if not include_internal:
                q = q.replace("-- SKIP_INTERNAL", "")
            q = text(q)
            return q

        self.ALL_RELATIONS_QUERY = processed(ALL_RELATIONS_QUERY)
        self.INDEXES_QUERY = processed(INDEXES_QUERY)
        self.SEQUENCES_QUERY = processed(SEQUENCES_QUERY)
        self.CONSTRAINTS_QUERY = processed(CONSTRAINTS_QUERY)
        self.FUNCTIONS_QUERY = processed(FUNCTIONS_QUERY)
        self.EXTENSIONS_QUERY = processed(EXTENSIONS_QUERY)
        self.ENUMS_QUERY = processed(ENUMS_QUERY)
        self.DEPS_QUERY = processed(DEPS_QUERY)
        self.SCHEMAS_QUERY = processed(SCHEMAS_QUERY)
        self.PRIVILEGES_QUERY = processed(PRIVILEGES_QUERY)
        self.TRIGGERS_QUERY = processed(TRIGGERS_QUERY)
        super(PostgreSQL, self).__init__(c, include_internal)

    def load_all(self):
        self.load_schemas()
        self.load_all_relations()
        self.load_functions()
        self.selectables = od()
        self.selectables.update(self.relations)
        self.selectables.update(self.functions)
        self.load_deps()
        self.load_deps_all()
        self.load_privileges()
        self.load_triggers()

    def load_schemas(self):
        q = self.c.execute(self.SCHEMAS_QUERY)
        schemas = [InspectedSchema(schema=each.schema) for each in q]
        self.schemas = od((schema.schema, schema) for schema in schemas)

    def load_privileges(self):
        q = self.c.execute(self.PRIVILEGES_QUERY)
        privileges = [
            InspectedPrivilege(
                object_type=i.object_type,
                schema=i.schema,
                name=i.name,
                privilege=i.privilege,
                target_user=i.user,
            )
            for i in q
        ]
        self.privileges = od((i.key, i) for i in privileges)

    def load_deps(self):
        q = self.c.execute(self.DEPS_QUERY)
        for dep in q:
            x = quoted_identifier(dep.name, dep.schema)
            x_dependent_on = quoted_identifier(
                dep.name_dependent_on, dep.schema_dependent_on
            )
            self.selectables[x].dependent_on.append(x_dependent_on)
            self.selectables[x].dependent_on.sort()
            self.selectables[x_dependent_on].dependents.append(x)
            self.selectables[x_dependent_on].dependents.sort()

    def load_deps_all(self):
        def get_related_for_item(item, att):
            related = [self.selectables[_] for _ in getattr(item, att)]
            return [item.signature] + [
                _ for d in related for _ in get_related_for_item(d, att)
            ]

        for k, x in self.selectables.items():
            d_all = get_related_for_item(x, "dependent_on")[1:]
            d_all.sort()
            x.dependent_on_all = d_all
            d_all = get_related_for_item(x, "dependents")[1:]
            d_all.sort()
            x.dependents_all = d_all

    def load_all_relations(self):
        self.tables = od()
        self.views = od()
        self.materialized_views = od()
        self.composite_types = od()
        q = self.c.execute(self.ENUMS_QUERY)
        enumlist = [
            InspectedEnum(name=i.name, schema=i.schema, elements=i.elements) for i in q
        ]
        self.enums = od((i.quoted_full_name, i) for i in enumlist)
        q = self.c.execute(self.ALL_RELATIONS_QUERY)
        for _, g in groupby(q, lambda x: (x.relationtype, x.schema, x.name)):
            clist = list(g)
            f = clist[0]

            def get_enum(name, schema):
                if not name and not schema:
                    return None

                quoted_full_name = "{}.{}".format(
                    quoted_identifier(schema), quoted_identifier(name)
                )
                return self.enums[quoted_full_name]

            columns = [
                ColumnInfo(
                    name=c.attname,
                    dbtype=c.datatype,
                    dbtypestr=c.datatypestring,
                    pytype=self.to_pytype(c.datatype),
                    default=c.defaultdef,
                    not_null=c.not_null,
                    is_enum=c.is_enum,
                    enum=get_enum(c.enum_name, c.enum_schema),
                )
                for c in clist
                if c.position_number
            ]
            s = InspectedSelectable(
                name=f.name,
                schema=f.schema,
                columns=od((c.name, c) for c in columns),
                relationtype=f.relationtype,
                definition=f.definition,
                comment=f.comment,
            )
            RELATIONTYPES = {
                "r": "tables",
                "v": "views",
                "m": "materialized_views",
                "c": "composite_types",
            }
            att = getattr(self, RELATIONTYPES[f.relationtype])
            att[s.quoted_full_name] = s
        self.relations = od()
        for x in (self.tables, self.views, self.materialized_views):
            self.relations.update(x)
        q = self.c.execute(self.INDEXES_QUERY)
        indexlist = [
            InspectedIndex(
                name=i.name,
                schema=i.schema,
                definition=i.definition,
                table_name=i.table_name,
            )
            for i in q
        ]
        self.indexes = od((i.quoted_full_name, i) for i in indexlist)
        q = self.c.execute(self.SEQUENCES_QUERY)
        sequencelist = [InspectedSequence(name=i.name, schema=i.schema) for i in q]
        self.sequences = od((i.quoted_full_name, i) for i in sequencelist)
        q = self.c.execute(self.CONSTRAINTS_QUERY)
        constraintlist = [
            InspectedConstraint(
                name=i.name,
                schema=i.schema,
                constraint_type=i.constraint_type,
                table_name=i.table_name,
                definition=i.definition,
                index=i.index,
            )
            for i in q
        ]
        self.constraints = od((i.quoted_full_name, i) for i in constraintlist)
        q = self.c.execute(self.EXTENSIONS_QUERY)
        extensionlist = [
            InspectedExtension(name=i.name, schema=i.schema, version=i.version)
            for i in q
        ]
        # extension names are unique per-database rather than per-schema like other things (even though extensions are assigned to a particular schema)
        self.extensions = od((i.name, i) for i in extensionlist)
        # add indexes and constraints to each table
        for each in self.indexes.values():
            t = each.quoted_full_table_name
            n = each.quoted_full_name
            self.relations[t].indexes[n] = each
        for each in self.constraints.values():
            t = each.quoted_full_table_name
            n = each.quoted_full_name
            self.relations[t].constraints[n] = each

    def load_functions(self):
        self.functions = od()
        q = self.c.execute(self.FUNCTIONS_QUERY)
        for _, g in groupby(q, lambda x: (x.schema, x.name, x.identity_arguments)):
            clist = list(g)
            f = clist[0]
            outs = [c for c in clist if c.parameter_mode == "OUT"]
            columns = [
                ColumnInfo(
                    name=c.parameter_name,
                    dbtype=c.data_type,
                    pytype=self.to_pytype(c.data_type),
                )
                for c in outs
            ]
            if outs:
                columns = [
                    ColumnInfo(
                        name=c.parameter_name,
                        dbtype=c.data_type,
                        pytype=self.to_pytype(c.data_type),
                    )
                    for c in outs
                ]
            else:
                columns = [
                    ColumnInfo(
                        name=f.name,
                        dbtype=f.data_type,
                        pytype=self.to_pytype(f.returntype),
                        default=f.parameter_default,
                    )
                ]
            plist = [
                ColumnInfo(
                    name=c.parameter_name,
                    dbtype=c.data_type,
                    pytype=self.to_pytype(c.data_type),
                    default=c.parameter_default,
                )
                for c in clist
                if c.parameter_mode == "IN"
            ]
            s = InspectedFunction(
                schema=f.schema,
                name=f.name,
                columns=od((c.name, c) for c in columns),
                inputs=plist,
                identity_arguments=f.identity_arguments,
                result_string=f.result_string,
                language=f.language,
                definition=f.definition,
                strictness=f.strictness,
                security_type=f.security_type,
                volatility=f.volatility,
                full_definition=f.full_definition,
                comment=f.comment,
            )

            identity_arguments = "({})".format(s.identity_arguments)
            self.functions[s.quoted_full_name + identity_arguments] = s

    def load_triggers(self):
        q = self.c.execute(self.TRIGGERS_QUERY)
        triggers = [InspectedTrigger(i.name, i.schema, i.full_definition) for i in q]  # type: list[InspectedTrigger]
        self.triggers = od((t.signature, t) for t in triggers)

    def one_schema(self, schema):
        props = "schemas relations tables views functions selectables sequences constraints indexes enums extensions privileges"
        for prop in props.split():
            att = getattr(self, prop)
            filtered = {k: v for k, v in att.items() if v.schema == schema}
            setattr(self, prop, filtered)

    def __eq__(self, other):
        """
        :type other: PostgreSQL
        :rtype: bool
        """
        return (
            type(self) == type(other)
            and self.schemas == other.schemas
            and self.relations == other.relations
            and self.sequences == other.sequences
            and self.enums == other.enums
            and self.constraints == other.constraints
            and self.extensions == other.extensions
            and self.functions == other.functions
            and self.triggers == other.triggers
        )
