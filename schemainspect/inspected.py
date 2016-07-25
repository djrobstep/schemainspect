from .misc import AutoRepr, quoted_identifier


class Inspected(AutoRepr):
    @property
    def quoted_full_name(self):
        return '{}.{}'.format(
            quoted_identifier(self.schema), quoted_identifier(self.name))

    @property
    def unquoted_full_name(self):
        return '{}.{}'.format(self.schema, self.name)

    @property
    def quoted_name(self):
        return quoted_identifier(self.name)

    @property
    def quoted_schema(self):
        return quoted_identifier(self.schema)


class ColumnInfo(AutoRepr):
    def __init__(self,
                 name,
                 dbtype,
                 pytype,
                 default=None,
                 not_null=False,
                 dbtypestr=None):

        self.name = name or ''
        self.dbtype = dbtype
        self.dbtypestr = dbtypestr or dbtype
        self.pytype = pytype
        self.default = default or None
        self.not_null = not_null

    def __eq__(self, other):
        return self.name == other.name \
            and self.dbtype == other.dbtype \
            and self.dbtypestr == other.dbtypestr \
            and self.pytype == other.pytype \
            and self.default == other.default \
            and self.not_null == other.not_null

    def __hash__(self):
        s = '{},{},{},{}'.format(self.name, self.dbtype, self.pytype,
                                 self.default, self.not_null)
        return hash(s)

    @property
    def quoted_name(self):
        return quoted_identifier(self.name)

    @property
    def creation_sql(self):
        x = '{} {}'.format(self.quoted_name, self.dbtypestr)
        if self.not_null:
            x += ' not null'
        if self.default:
            x += ' default {}'.format(self.default)
        return x


class InspectedSelectable(Inspected):
    def __init__(self,
                 name,
                 schema,
                 columns,
                 inputs=None,
                 definition=None,
                 drop_statement=None,
                 create_statement=None,
                 relationtype='unknown'):
        self.name = name
        self.schema = schema
        self.inputs = inputs or []
        self.columns = columns
        self.definition = definition
        self.relationtype = relationtype
        self._create_statement = create_statement
        self._drop_statement = drop_statement

    def __eq__(self, other):
        equalities = \
            self.relationtype == other.relationtype, \
            self.name == other.name, \
            self.schema == other.schema, \
            self.columns == other.columns, \
            self.inputs == other.inputs, \
            self.definition == other.definition
        return all(equalities)
