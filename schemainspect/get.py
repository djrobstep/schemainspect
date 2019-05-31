from .inspector import NullInspector
from .misc import connection_from_s_or_c
from .pg import PostgreSQL
from .rs import Redshift

SUPPORTED = {"postgresql": PostgreSQL, "redshift": Redshift}


def get_inspector(x, schema=None, dialect=None):
    if x is None:
        return NullInspector()

    c = connection_from_s_or_c(x)
    dialect = dialect or c.dialect.name
    try:
        ic = SUPPORTED[dialect]
    except KeyError:
        raise NotImplementedError

    inspected = ic(c)
    if schema:
        inspected.one_schema(schema)
    return inspected
