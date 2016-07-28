from .misc import connection_from_s_or_c
from .pg import PostgreSQL

SUPPORTED = {'postgresql': PostgreSQL}


def get_inspector(x):
    c = connection_from_s_or_c(x)

    try:
        ic = SUPPORTED[c.dialect.name]
    except KeyError:
        raise NotImplementedError

    return ic(c)
