from .misc import connection_from_s_or_c
from .pg import PostgreSQL

SUPPORTED = {'postgresql': PostgreSQL}


def get_inspector(x):
    c = connection_from_s_or_c(x)

    try:
        return SUPPORTED[c.dialect.name](c)
    except KeyError:
        raise NotImplementedError
