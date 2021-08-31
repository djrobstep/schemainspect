from __future__ import absolute_import, division, print_function, unicode_literals

import inspect
import sys
from collections import OrderedDict as od
from itertools import groupby

from sqlalchemy import text

from ..inspected import ColumnInfo, Inspected
from ..inspected import InspectedSelectable as BaseInspectedSelectable
from ..inspected import TableRelated
from ..inspector import DBInspector
from ..misc import quoted_identifier, resource_text

CREATE_TABLE = """create {}table {} ({}
){}{};
"""
CREATE_TABLE_SUBCLASS = """create {}table {} partition of {} {};
"""
CREATE_FUNCTION_FORMAT = """create or replace function {signature}
returns {result_string} as
$${definition}$$
language {language} {volatility} {strictness} {security_type};"""
ALL_RELATIONS_QUERY = resource_text("sql/relations.sql")
ALL_RELATIONS_QUERY_9 = resource_text("sql/relations9.sql")
SCHEMAS_QUERY = resource_text("sql/schemas.sql")
INDEXES_QUERY = resource_text("sql/indexes.sql")
SEQUENCES_QUERY = resource_text("sql/sequences.sql")
CONSTRAINTS_QUERY = resource_text("sql/constraints.sql")
FUNCTIONS_QUERY = resource_text("sql/functions.sql")
TYPES_QUERY = resource_text("sql/types.sql")
DOMAINS_QUERY = resource_text("sql/domains.sql")
EXTENSIONS_QUERY = resource_text("sql/extensions.sql")
ENUMS_QUERY = resource_text("sql/enums.sql")
DEPS_QUERY = resource_text("sql/deps.sql")
PRIVILEGES_QUERY = resource_text("sql/privileges.sql")
TRIGGERS_QUERY = resource_text("sql/triggers.sql")
COLLATIONS_QUERY = resource_text("sql/collations.sql")
COLLATIONS_QUERY_9 = resource_text("sql/collations9.sql")
RLSPOLICIES_QUERY = resource_text("sql/rlspolicies.sql")


class InspectedSelectable(BaseInspectedSelectable):
    def has_compatible_columns(self, other):
        def names_and_types(cols):
            return [(k, c.dbtype) for k, c in cols.items()]

        items = names_and_types(self.columns)

        if self.relationtype != "f":
            old_arg_count = len(other.columns)
            items = items[:old_arg_count]

        return items == names_and_types(other.columns)

    def can_replace(self, other):
        if not (self.relationtype in ("v", "f") or self.is_table):
            return False

        if self.signature != other.signature:
            return False

        if self.relationtype != other.relationtype:
            return False

        return self.has_compatible_columns(other)

    @property
    def persistence_modifier(self):
        if self.persistence == "t":
            return "temporary "
        elif self.persistence == "u":
            return "unlogged "
        else:
            return ""

    @property
    def is_unlogged(self):
        return self.persistence == "u"

    @property
    def create_statement(self):
        n = self.quoted_full_name
        if self.relationtype in ("r", "p"):

            if not self.is_partitioning_child_table:
                colspec = ",\n".join(
                    "    " + c.creation_clause for c in self.columns.values()
                )
                if colspec:
                    colspec = "\n" + colspec

                if self.partition_def:
                    partition_key = " partition by " + self.partition_def
                    inherits_clause = ""
                elif self.parent_table:
                    inherits_clause = " inherits ({})".format(self.parent_table)
                    partition_key = ""
                else:
                    partition_key = ""
                    inherits_clause = ""

                create_statement = CREATE_TABLE.format(
                    self.persistence_modifier,
                    n,
                    colspec,
                    partition_key,
                    inherits_clause,
                )
            else:
                create_statement = CREATE_TABLE_SUBCLASS.format(
                    self.persistence_modifier, n, self.parent_table, self.partition_def
                )
        elif self.relationtype == "v":
            create_statement = "create or replace view {} as {}\n".format(
                n, self.definition
            )
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
        if self.relationtype in ("r", "p"):
            drop_statement = "drop table {};".format(n)
        elif self.relationtype == "v":
            drop_statement = "drop view if exists {};".format(n)
        elif self.relationtype == "m":
            drop_statement = "drop materialized view if exists {};".format(n)
        elif self.relationtype == "c":
            drop_statement = "drop type {};".format(n)
        else:
            raise NotImplementedError  # pragma: no cover

        return drop_statement

    def alter_table_statement(self, clause):
        if self.is_alterable:
            alter = "alter table {} {};".format(self.quoted_full_name, clause)
        else:
            raise NotImplementedError  # pragma: no cover

        return alter

    @property
    def is_partitioned(self):
        return self.relationtype == "p"

    @property
    def is_inheritance_child_table(self):
        return bool(self.parent_table) and not self.partition_def

    @property
    def is_table(self):
        return self.relationtype in ("p", "r")

    @property
    def is_alterable(self):
        return self.is_table and (
            not self.parent_table or self.is_inheritance_child_table
        )

    @property
    def contains_data(self):
        return bool(
            self.relationtype == "r" and (self.parent_table or not self.partition_def)
        )

    # for back-compat only
    @property
    def is_child_table(self):
        return self.is_partitioning_child_table

    @property
    def is_partitioning_child_table(self):
        return bool(
            self.relationtype == "r" and self.parent_table and self.partition_def
        )

    @property
    def uses_partitioning(self):
        return self.is_partitioning_child_table or self.is_partitioned

    @property
    def attach_statement(self):
        if self.parent_table:
            if self.partition_def:
                return "alter table {} attach partition {} {};".format(
                    self.quoted_full_name, self.parent_table, self.partition_spec
                )
            else:
                return "alter table {} inherit {}".format(
                    self.quoted_full_name, self.parent_table
                )

    @property
    def detach_statement(self):
        if self.parent_table:
            if self.partition_def:
                return "alter table {} detach partition {};".format(
                    self.parent_table, self.quoted_full_name
                )
            else:
                return "alter table {} no inherit {}".format(
                    self.quoted_full_name, self.parent_table
                )

    def attach_detach_statements(self, before):
        slist = []
        if self.parent_table != before.parent_table:
            if before.parent_table:
                slist.append(before.detach_statement)
            if self.parent_table:
                slist.append(self.attach_statement)
        return slist

    @property
    def alter_rls_clause(self):
        keyword = "enable" if self.rowsecurity else "disable"
        return "{} row level security".format(keyword)

    @property
    def alter_rls_statement(self):
        return self.alter_table_statement(self.alter_rls_clause)

    @property
    def alter_unlogged_statement(self):
        keyword = "unlogged" if self.is_unlogged else "logged"
        return self.alter_table_statement("set {}".format(keyword))


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
        returntype,
        kind,
    ):
        self.identity_arguments = identity_arguments
        self.result_string = result_string
        self.language = language
        self.volatility = volatility
        self.strictness = strictness
        self.security_type = security_type
        self.full_definition = full_definition
        self.returntype = returntype
        self.kind = kind

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
    def returntype_is_table(self):
        return "." in self.returntype

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
    def thing(self):
        kinds = dict(f="function", p="procedure", a="aggregate", w="window function")
        return kinds[self.kind]

    @property
    def drop_statement(self):
        return "drop {} if exists {};".format(self.thing, self.signature)

    def __eq__(self, other):
        return (
            self.signature == other.signature
            and self.result_string == other.result_string
            and self.definition == other.definition
            and self.language == other.language
            and self.volatility == other.volatility
            and self.strictness == other.strictness
            and self.security_type == other.security_type
            and self.kind == other.kind
        )


class InspectedTrigger(Inspected):
    def __init__(
        self, name, schema, table_name, proc_schema, proc_name, enabled, full_definition
    ):
        (
            self.name,
            self.schema,
            self.table_name,
            self.proc_schema,
            self.proc_name,
            self.enabled,
            self.full_definition,
        ) = (name, schema, table_name, proc_schema, proc_name, enabled, full_definition)

        self.dependent_on = [self.quoted_full_selectable_name]
        self.dependents = []

    @property
    def signature(self):
        return self.quoted_full_name

    @property
    def quoted_full_name(self):
        return "{}.{}.{}".format(
            quoted_identifier(self.schema),
            quoted_identifier(self.table_name),
            quoted_identifier(self.name),
        )

    @property
    def quoted_full_selectable_name(self):
        return "{}.{}".format(
            quoted_identifier(self.schema), quoted_identifier(self.table_name)
        )

    @property
    def drop_statement(self):
        return 'drop trigger if exists "{}" on "{}"."{}";'.format(
            self.name, self.schema, self.table_name
        )

    @property
    def create_statement(self):
        return self.full_definition + ";"

    def __eq__(self, other):
        """
        :type other: InspectedTrigger
        :rtype: bool
        """
        return (
            self.name == other.name
            and self.schema == other.schema
            and self.table_name == other.table_name
            and self.proc_schema == other.proc_schema
            and self.proc_name == other.proc_name
            and self.enabled == other.enabled
            and self.full_definition == other.full_definition
        )


class InspectedIndex(Inspected, TableRelated):
    def __init__(
        self,
        name,
        schema,
        table_name,
        key_columns,
        key_options,
        num_att,
        is_unique,
        is_pk,
        is_exclusion,
        is_immediate,
        is_clustered,
        key_collations,
        key_expressions,
        partial_predicate,
        algorithm,
        definition=None,
        constraint=None,
        index_columns=None,
        included_columns=None,
    ):
        self.name = name
        self.schema = schema
        self.definition = definition
        self.table_name = table_name
        self.key_columns = key_columns
        self.key_options = key_options
        self.num_att = num_att
        self.is_unique = is_unique
        self.is_pk = is_pk
        self.is_exclusion = is_exclusion
        self.is_immediate = is_immediate
        self.is_clustered = is_clustered
        self.key_collations = key_collations
        self.key_expressions = key_expressions
        self.partial_predicate = partial_predicate
        self.algorithm = algorithm
        self.constraint = constraint
        self.index_columns = index_columns
        self.included_columns = included_columns

    @property
    def drop_statement(self):
        return "drop index if exists {};".format(self.quoted_full_name)

    @property
    def create_statement(self):
        return "{};".format(self.definition)

    def __eq__(self, other):
        """
        :type other: InspectedIndex
        :rtype: bool
        """
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.table_name == other.table_name,
            self.key_columns == other.key_columns,
            self.included_columns == other.included_columns,
            self.key_options == other.key_options,
            self.num_att == other.num_att,
            self.is_unique == other.is_unique,
            self.is_pk == other.is_pk,
            self.is_exclusion == other.is_exclusion,
            self.is_immediate == other.is_immediate,
            self.is_clustered == other.is_clustered,
            self.key_collations == other.key_collations,
            self.key_expressions == other.key_expressions,
            self.partial_predicate == other.partial_predicate,
            self.algorithm == other.algorithm
            # self.constraint == other.constraint
        )
        return all(equalities)


class InspectedSequence(Inspected):
    def __init__(self, name, schema, table_name=None, column_name=None):
        self.name = name
        self.schema = schema
        self.table_name = table_name
        self.column_name = column_name

    @property
    def drop_statement(self):
        return "drop sequence if exists {};".format(self.quoted_full_name)

    @property
    def create_statement(self):
        return "create sequence {};".format(self.quoted_full_name)

    @property
    def create_statement_with_ownership(self):
        t_col_name = self.quoted_table_and_column_name

        if self.table_name and self.column_name:
            return "create sequence {} owned by {};".format(
                self.quoted_full_name, t_col_name
            )
        else:
            return "create sequence {};".format(self.quoted_full_name)

    @property
    def alter_ownership_statement(self):
        t_col_name = self.quoted_table_and_column_name

        if t_col_name is not None:
            return "alter sequence {} owned by {};".format(
                self.quoted_full_name, t_col_name
            )
        else:
            return "alter sequence {} owned by none;".format(self.quoted_full_name)

    @property
    def quoted_full_table_name(self):
        if self.table_name is not None:
            return quoted_identifier(self.table_name, self.schema)

    @property
    def quoted_table_and_column_name(self):
        if self.column_name is not None and self.table_name is not None:
            return (
                self.quoted_full_table_name + "." + quoted_identifier(self.column_name)
            )

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.quoted_table_and_column_name == other.quoted_table_and_column_name,
        )
        return all(equalities)


class InspectedCollation(Inspected):
    def __init__(self, name, schema, provider, encoding, lc_collate, lc_ctype, version):
        self.name = name
        self.schema = schema
        self.provider = provider
        self.lc_collate = lc_collate
        self.lc_ctype = lc_ctype
        self.encoding = encoding
        self.version = version

    @property
    def locale(self):
        return self.lc_collate

    @property
    def drop_statement(self):
        return "drop collation if exists {};".format(self.quoted_full_name)

    @property
    def create_statement(self):
        return "create collation if not exists {} (provider = '{}', locale = '{}');".format(
            self.quoted_full_name, self.provider, self.locale
        )

    def __eq__(self, other):

        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.provider == other.provider,
            self.locale == other.locale,
        )
        return all(equalities)


class InspectedEnum(Inspected):
    def __init__(self, name, schema, elements, pg_version=None):
        self.name = name
        self.schema = schema
        self.elements = elements
        self.pg_version = pg_version
        self.dependents = []
        self.dependent_on = []

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

    def alter_rename_statement(self, new_name):
        name = new_name

        return "alter type {} rename to {};".format(
            self.quoted_full_name, quoted_identifier(name)
        )

    def drop_statement_with_rename(self, new_name):
        name = new_name
        new_name = quoted_identifier(name, self.schema)
        return "drop type {};".format(new_name)

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
                    s = "alter type {} add value '{}' before '{}';".format(
                        self.quoted_full_name, c, old[0]
                    )
                else:
                    s = "alter type {} add value '{}' after '{}';".format(
                        self.quoted_full_name, c, previous
                    )
                statements.append(s)
            previous = c
        return statements

    def can_be_changed_to(self, new, when_within_transaction=False):
        old = self.elements

        if when_within_transaction and self.pg_version and self.pg_version < 12:
            return False

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

    @property
    def quoted_full_name(self):
        return self.quoted_name

    @property
    def quoted_name(self):
        return quoted_identifier(self.schema)

    def __eq__(self, other):
        return self.schema == other.schema


class InspectedType(Inspected):
    def __init__(self, name, schema, columns):
        self.name = name
        self.schema = schema
        self.columns = columns

    @property
    def drop_statement(self):
        return "drop type {};".format(self.signature)

    @property
    def create_statement(self):
        sql = "create type {} as (\n".format(self.signature)

        indent = " " * 4
        typespec = [
            "{}{} {}".format(indent, quoted_identifier(name), _type)
            for name, _type in self.columns.items()
        ]

        sql += ",\n".join(typespec)
        sql += "\n);"
        return sql

    def __eq__(self, other):
        return (
            self.schema == other.schema
            and self.name == other.name
            and self.columns == other.columns
        )


class InspectedDomain(Inspected):
    def __init__(
        self,
        name,
        schema,
        data_type,
        collation,
        constraint_name,
        not_null,
        default,
        check,
    ):
        self.name = name
        self.schema = schema
        self.data_type = data_type
        self.collation = collation
        self.constraint_name = constraint_name
        self.not_null = not_null
        self.default = default
        self.check = check

    @property
    def drop_statement(self):
        return "drop domain {};".format(self.signature)

    @property
    def create_statement(self):
        T = """\
create domain {name}
as {_type}
{collation}{default}{nullable}{check}
"""

        sql = T.format(
            name=self.signature,
            _type=self.data_type,
            collation=self.collation_clause,
            default=self.default_clause,
            check=self.check_clause,
            nullable=self.nullable_clause,
        )

        return sql

    @property
    def check_clause(self):
        if self.check:
            return "{}\n".format(self.check)

        return ""

    @property
    def collation_clause(self):
        if self.collation:
            return "collation {}\n".format(self.collation)

        return ""

    @property
    def default_clause(self):
        if self.default:
            return "default {}\n".format(self.default)

        return ""

    @property
    def nullable_clause(self):
        if self.not_null:
            return "not null\n"
        else:
            return "null\n"

    equality_attributes = (
        "schema name data_type collation default constraint_name not_null check".split()
    )

    def __eq__(self, other):
        try:
            return all(
                [
                    getattr(self, a) == getattr(other, a)
                    for a in self.equality_attributes
                ]
            )
        except AttributeError:
            return False


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
        return "alter extension {} update to '{}';".format(
            self.quoted_name, self.version
        )

    def alter_statements(self, other=None):
        return [self.update_statement]

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.version == other.version,
        )
        return all(equalities)


class InspectedConstraint(Inspected, TableRelated):
    def __init__(
        self,
        name,
        schema,
        constraint_type,
        table_name,
        definition,
        index,
        is_fk=False,
        is_deferrable=False,
        initially_deferred=False,
    ):
        self.name = name
        self.schema = schema
        self.constraint_type = constraint_type
        self.table_name = table_name
        self.definition = definition
        self.index = index
        self.is_fk = is_fk

        self.quoted_full_foreign_table_name = None
        self.fk_columns_local = None
        self.fk_columns_foreign = None

        self.is_deferrable = is_deferrable
        self.initially_deferred = initially_deferred

    @property
    def drop_statement(self):
        return "alter table {} drop constraint {};".format(
            self.quoted_full_table_name, self.quoted_name
        )

    @property
    def deferrable_subclause(self):
        # [ DEFERRABLE | NOT DEFERRABLE ] [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]

        if not self.is_deferrable:
            return ""

        else:
            clause = " DEFERRABLE"

            if self.initially_deferred:
                clause += " INITIALLY DEFERRED"

            return clause

    @property
    def create_statement(self):
        if self.index:
            using_clause = "{} using index {}{}".format(
                self.constraint_type, self.quoted_name, self.deferrable_subclause
            )
        else:
            using_clause = self.definition

        USING = "alter table {} add constraint {} {};"

        return USING.format(self.quoted_full_table_name, self.quoted_name, using_clause)

    @property
    def quoted_full_name(self):
        return "{}.{}.{}".format(
            quoted_identifier(self.schema),
            quoted_identifier(self.table_name),
            quoted_identifier(self.name),
        )

    @property
    def quoted_full_table_name(self):
        return "{}.{}".format(
            quoted_identifier(self.schema), quoted_identifier(self.table_name)
        )

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.table_name == other.table_name,
            self.definition == other.definition,
            self.index == other.index,
            self.is_deferrable == other.is_deferrable,
            self.initially_deferred == other.initially_deferred,
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
    def quoted_target_user(self):
        return quoted_identifier(self.target_user)

    @property
    def drop_statement(self):
        return "revoke {} on {} {} from {};".format(
            self.privilege,
            self.object_type,
            self.quoted_full_name,
            self.quoted_target_user,
        )

    @property
    def create_statement(self):
        return "grant {} on {} {} to {};".format(
            self.privilege,
            self.object_type,
            self.quoted_full_name,
            self.quoted_target_user,
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


RLS_POLICY_CREATE = """create policy {name}
on {table_name}
as {permissiveness}
for {commandtype_keyword}
to {roleslist}{qual_clause}{withcheck_clause};
"""

COMMANDTYPES = {"*": "all", "r": "select", "a": "insert", "w": "update", "d": "delete"}


class InspectedRowPolicy(Inspected, TableRelated):
    def __init__(
        self, name, schema, table_name, commandtype, permissive, roles, qual, withcheck
    ):
        self.name = name
        self.schema = schema
        self.table_name = table_name
        self.commandtype = commandtype
        self.permissive = permissive
        self.roles = roles
        self.qual = qual
        self.withcheck = withcheck

    @property
    def permissiveness(self):
        return "permissive" if self.permissive else "restrictive"

    @property
    def commandtype_keyword(self):
        return COMMANDTYPES[self.commandtype]

    @property
    def key(self):
        return "{}.{}".format(self.quoted_full_table_name, self.quoted_name)

    @property
    def create_statement(self):
        if self.qual:
            qual_clause = "\nusing ({})".format(self.qual)
        else:
            qual_clause = ""

        if self.withcheck:
            withcheck_clause = "\nwith check {}".format(self.withcheck)
        else:
            withcheck_clause = ""

        roleslist = ", ".join(self.roles)

        return RLS_POLICY_CREATE.format(
            name=self.quoted_name,
            table_name=self.quoted_full_table_name,
            permissiveness=self.permissiveness,
            commandtype_keyword=self.commandtype_keyword,
            roleslist=roleslist,
            qual_clause=qual_clause,
            withcheck_clause=withcheck_clause,
        )

    @property
    def drop_statement(self):
        return "drop policy {} on {};".format(
            self.quoted_name, self.quoted_full_table_name
        )

    def __eq__(self, other):
        equalities = (
            self.name == self.name,
            self.schema == other.schema,
            self.permissiveness == other.permissiveness,
            self.commandtype == other.commandtype,
            self.permissive == other.permissive,
            self.roles == other.roles,
            self.qual == other.qual,
            self.withcheck == other.withcheck,
        )
        return all(equalities)


PROPS = "schemas relations tables views functions selectables sequences constraints indexes enums extensions privileges collations triggers"


class PostgreSQL(DBInspector):
    def __init__(self, c, include_internal=False):
        pg_version = c.dialect.server_version_info[0]
        self.pg_version = pg_version

        def processed(q):
            if not include_internal:
                q = q.replace("-- SKIP_INTERNAL", "")
            if self.pg_version >= 11:
                q = q.replace("-- 11_AND_LATER", "")
            else:
                q = q.replace("-- 10_AND_EARLIER", "")
            q = text(q)
            return q

        if pg_version <= 9:
            self.ALL_RELATIONS_QUERY = processed(ALL_RELATIONS_QUERY_9)
            self.COLLATIONS_QUERY = processed(COLLATIONS_QUERY_9)
            self.RLSPOLICIES_QUERY = None
        else:
            all_relations_query = ALL_RELATIONS_QUERY

            if pg_version >= 12:
                replace = "-- 12_ONLY"
            else:
                replace = "-- PRE_12"

            all_relations_query = all_relations_query.replace(replace, "")
            self.ALL_RELATIONS_QUERY = processed(all_relations_query)
            self.COLLATIONS_QUERY = processed(COLLATIONS_QUERY)
            self.RLSPOLICIES_QUERY = processed(RLSPOLICIES_QUERY)

        self.INDEXES_QUERY = processed(INDEXES_QUERY)
        self.SEQUENCES_QUERY = processed(SEQUENCES_QUERY)
        self.CONSTRAINTS_QUERY = processed(CONSTRAINTS_QUERY)
        self.FUNCTIONS_QUERY = processed(FUNCTIONS_QUERY)
        self.TYPES_QUERY = processed(TYPES_QUERY)
        self.DOMAINS_QUERY = processed(DOMAINS_QUERY)
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

        self.load_privileges()
        self.load_triggers()
        self.load_collations()
        self.load_rlspolicies()
        self.load_types()
        self.load_domains()

        self.load_deps()
        self.load_deps_all()

    def load_schemas(self):
        q = self.c.execute(self.SCHEMAS_QUERY)
        schemas = [InspectedSchema(schema=each.schema) for each in q]
        self.schemas = od((schema.schema, schema) for schema in schemas)

    def load_rlspolicies(self):
        if self.pg_version <= 9:
            self.rlspolicies = od()
            return

        q = self.c.execute(self.RLSPOLICIES_QUERY)

        rlspolicies = [
            InspectedRowPolicy(
                name=p.name,
                schema=p.schema,
                table_name=p.table_name,
                commandtype=p.commandtype,
                permissive=p.permissive,
                roles=p.roles,
                qual=p.qual,
                withcheck=p.withcheck,
            )
            for p in q
        ]

        self.rlspolicies = od((p.key, p) for p in rlspolicies)

    def load_collations(self):
        q = self.c.execute(self.COLLATIONS_QUERY)
        collations = [
            InspectedCollation(
                schema=i.schema,
                name=i.name,
                provider=i.provider,
                encoding=i.encoding,
                lc_collate=i.lc_collate,
                lc_ctype=i.lc_ctype,
                version=i.version,
            )
            for i in q
        ]
        self.collations = od((i.quoted_full_name, i) for i in collations)

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

        self.deps = list(q)

        for dep in self.deps:
            x = quoted_identifier(dep.name, dep.schema, dep.identity_arguments)
            x_dependent_on = quoted_identifier(
                dep.name_dependent_on,
                dep.schema_dependent_on,
                dep.identity_arguments_dependent_on,
            )
            self.selectables[x].dependent_on.append(x_dependent_on)
            self.selectables[x].dependent_on.sort()
            self.selectables[x_dependent_on].dependents.append(x)
            self.selectables[x_dependent_on].dependents.sort()

        for k, t in self.triggers.items():
            for dep_name in t.dependent_on:
                try:
                    dependency = self.selectables[dep_name]
                except KeyError:
                    continue
                dependency.dependents.append(k)

        for k, r in self.relations.items():
            for kc, c in r.columns.items():
                if c.is_enum:
                    e_sig = c.enum.signature

                    if e_sig in self.enums:
                        r.dependent_on.append(e_sig)
                        c.enum.dependents.append(k)

            if r.parent_table:
                pt = self.relations[r.parent_table]
                r.dependent_on.append(r.parent_table)
                pt.dependents.append(r.signature)

    def get_dependency_by_signature(self, signature):
        things = [self.selectables, self.enums, self.triggers]

        for thing in things:
            try:
                return thing[signature]
            except KeyError:
                continue

    def load_deps_all(self):
        def get_related_for_item(item, att):
            related = [self.get_dependency_by_signature(_) for _ in getattr(item, att)]
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

    def dependency_order(
        self,
        drop_order=False,
        selectables=True,
        triggers=True,
        enums=True,
        include_fk_deps=False,
    ):
        if sys.version_info < (3, 0):
            raise NotImplementedError

        from schemainspect import TopologicalSorter

        graph, things = {}, {}

        if enums:
            things.update(self.enums)
        if selectables:
            things.update(self.selectables)
        if triggers:
            things.update(self.triggers)

        for k, x in things.items():
            dependent_on = list(x.dependent_on)

            if k in self.tables and x.parent_table:
                dependent_on.append(x.parent_table)

            graph[k] = list(x.dependent_on)

        if include_fk_deps:
            fk_deps = {}

            for k, x in self.constraints.items():
                if x.is_fk:
                    t, other_t = (
                        x.quoted_full_table_name,
                        x.quoted_full_foreign_table_name,
                    )
                    fk_deps[t] = [other_t]

            graph.update(fk_deps)

        ts = TopologicalSorter(graph)

        ordering = []

        ts.prepare()

        while ts.is_active():
            items = ts.get_ready()

            itemslist = list(items)

            # itemslist.sort()
            ordering += itemslist
            ts.done(*items)

        if drop_order:
            ordering.reverse()
        return ordering

    @property
    def partitioned_tables(self):
        return od((k, v) for k, v in self.tables.items() if v.is_partitioned)

    @property
    def alterable_tables(self):  # ordinary tables and parent tables
        return od((k, v) for k, v in self.tables.items() if v.is_alterable)

    @property
    def data_tables(self):  # ordinary tables and child tables
        return od((k, v) for k, v in self.tables.items() if v.contains_data)

    @property
    def partitioning_child_tables(self):
        return od(
            (k, v) for k, v in self.tables.items() if v.is_partitioning_child_table
        )

    @property
    def tables_using_partitioning(self):
        return od((k, v) for k, v in self.tables.items() if v.uses_partitioning)

    @property
    def tables_not_using_partitioning(self):
        return od((k, v) for k, v in self.tables.items() if not v.uses_partitioning)

    def load_all_relations(self):
        self.tables = od()
        self.views = od()
        self.materialized_views = od()
        self.composite_types = od()

        q = self.c.execute(self.ENUMS_QUERY)
        enumlist = [
            InspectedEnum(
                name=i.name,
                schema=i.schema,
                elements=i.elements,
                pg_version=self.pg_version,
            )
            for i in q
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
                    collation=c.collation,
                    is_identity=c.is_identity,
                    is_identity_always=c.is_identity_always,
                    is_generated=c.is_generated,
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
                parent_table=f.parent_table,
                partition_def=f.partition_def,
                rowsecurity=f.rowsecurity,
                forcerowsecurity=f.forcerowsecurity,
                persistence=f.persistence,
            )
            RELATIONTYPES = {
                "r": "tables",
                "v": "views",
                "m": "materialized_views",
                "c": "composite_types",
                "p": "tables",
            }
            att = getattr(self, RELATIONTYPES[f.relationtype])
            att[s.quoted_full_name] = s

        for k, t in self.tables.items():
            if t.is_inheritance_child_table:
                parent_table = self.tables[t.parent_table]
                for cname, c in t.columns.items():
                    if cname in parent_table.columns:
                        c.is_inherited = True

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
                key_columns=i.key_columns,
                index_columns=i.index_columns,
                included_columns=i.included_columns,
                key_options=i.key_options,
                num_att=i.num_att,
                is_unique=i.is_unique,
                is_pk=i.is_pk,
                is_exclusion=i.is_exclusion,
                is_immediate=i.is_immediate,
                is_clustered=i.is_clustered,
                key_collations=i.key_collations,
                key_expressions=i.key_expressions,
                partial_predicate=i.partial_predicate,
                algorithm=i.algorithm,
            )
            for i in q
        ]
        self.indexes = od((i.quoted_full_name, i) for i in indexlist)
        q = self.c.execute(self.SEQUENCES_QUERY)

        sequencelist = [
            InspectedSequence(
                name=i.name,
                schema=i.schema,
                table_name=i.table_name,
                column_name=i.column_name,
            )
            for i in q
        ]
        self.sequences = od((i.quoted_full_name, i) for i in sequencelist)
        q = self.c.execute(self.CONSTRAINTS_QUERY)

        constraintlist = []

        for i in q:
            constraint = InspectedConstraint(
                name=i.name,
                schema=i.schema,
                constraint_type=i.constraint_type,
                table_name=i.table_name,
                definition=i.definition,
                index=i.index,
                is_fk=i.is_fk,
                is_deferrable=i.is_deferrable,
                initially_deferred=i.initially_deferred,
            )
            if constraint.index:
                index_name = quoted_identifier(i.index, schema=i.schema)
                index = self.indexes[index_name]
                index.constraint = constraint
                constraint.index = index

            if constraint.is_fk:
                constraint.quoted_full_foreign_table_name = quoted_identifier(
                    i.foreign_table_name, schema=i.foreign_table_schema
                )
                constraint.fk_columns_foreign = i.fk_columns_foreign
                constraint.fk_columns_local = i.fk_columns_local

            constraintlist.append(constraint)

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
                returntype=f.returntype,
                kind=f.kind,
            )

            identity_arguments = "({})".format(s.identity_arguments)
            self.functions[s.quoted_full_name + identity_arguments] = s

    def load_triggers(self):
        q = self.c.execute(self.TRIGGERS_QUERY)
        triggers = [
            InspectedTrigger(
                i.name,
                i.schema,
                i.table_name,
                i.proc_schema,
                i.proc_name,
                i.enabled,
                i.full_definition,
            )
            for i in q
        ]  # type: list[InspectedTrigger]
        self.triggers = od((t.signature, t) for t in triggers)

    def load_types(self):
        q = self.c.execute(self.TYPES_QUERY)

        def col(defn):
            return defn["attribute"], defn["type"]

        types = [
            InspectedType(i.name, i.schema, dict(col(_) for _ in i.columns)) for i in q
        ]  # type: list[InspectedType]
        self.types = od((t.signature, t) for t in types)

    def load_domains(self):
        q = self.c.execute(self.DOMAINS_QUERY)

        def col(defn):
            return defn["attribute"], defn["type"]

        domains = [
            InspectedDomain(
                i.name,
                i.schema,
                i.data_type,
                i.collation,
                i.constraint_name,
                i.not_null,
                i.default,
                i.check,
            )
            for i in q
        ]  # type: list[InspectedType]
        self.domains = od((t.signature, t) for t in domains)

    def filter_schema(self, schema=None, exclude_schema=None):
        if schema and exclude_schema:
            raise ValueError("Can only have schema or exclude schema, not both")

        def equal_to_schema(x):
            return x.schema == schema

        def not_equal_to_exclude_schema(x):
            return x.schema != exclude_schema

        if schema:
            comparator = equal_to_schema
        elif exclude_schema:
            comparator = not_equal_to_exclude_schema
        else:
            raise ValueError("schema or exclude_schema must be not be none")

        for prop in PROPS.split():
            att = getattr(self, prop)
            filtered = {k: v for k, v in att.items() if comparator(v)}
            setattr(self, prop, filtered)

    def _as_dicts(self):
        def obj_to_d(x):
            if isinstance(x, dict):
                return {k: obj_to_d(v) for k, v in x.items()}

            elif isinstance(x, (ColumnInfo, Inspected)):
                return {
                    k: obj_to_d(getattr(x, k))
                    for k in dir(x)
                    if not k.startswith("_") and not inspect.ismethod(getattr(x, k))
                }
            else:

                return str(x)

        d = {}

        for prop in PROPS.split():
            att = getattr(self, prop)

            _d = {k: obj_to_d(v) for k, v in att.items()}

            d[prop] = _d

        return d

    def one_schema(self, schema):
        self.filter_schema(schema=schema)

    def exclude_schema(self, schema):
        self.filter_schema(exclude_schema=schema)

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
            and self.collations == other.collations
            and self.rlspolicies == other.rlspolicies
        )
