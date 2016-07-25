from __future__ import (absolute_import, division, print_function,
                        unicode_literals)


def to_pytype(sqla_dialect, typename):
    try:
        sqla_obj = sqla_dialect.ischema_names[typename]()
    except KeyError:
        return type(None)

    try:
        return sqla_obj.python_type
    except (NotImplementedError):
        return type(sqla_obj)


class DBInspector(object):
    def __init__(self, c):
        self.c = c
        self.engine = self.c.engine
        self.dialect = self.engine.dialect
        self.load_all()

    def to_pytype(self, typename):
        return to_pytype(self.dialect, typename)
