from .inspector import NullInspector
from .misc import connection_from_s_or_c
from .pg import PostgreSQL

SUPPORTED = {"postgresql": PostgreSQL}


def get_inspector(x, schema=None, exclude_schema=None):
    if schema and exclude_schema:
        raise ValueError("Cannot provide both schema and exclude_schema")
    if x is None:
        return NullInspector()

    c = connection_from_s_or_c(x)
    try:
        ic = SUPPORTED[c.dialect.name]
    except KeyError:
        raise NotImplementedError

    inspected = ic(c)
    if schema:
        inspected.one_schema(schema)
    elif exclude_schema:
        inspected.exclude_schema(exclude_schema)
    return inspected
