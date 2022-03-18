from collections import OrderedDict as od


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
    def __init__(self, c, include_internal=False):
        self.c = c
        try:
            self.engine = self.c.engine
            self.dialect = self.engine.dialect
        except AttributeError:
            self.engine = None
            self.dialect = "postgresql"
        self.include_internal = include_internal
        self.load_all()

    def to_pytype(self, typename):
        if self.engine:
            return to_pytype(self.dialect, typename)


class NullInspector(DBInspector):
    def __init__(self):
        pass

    def __getattr__(self, name):
        return od()
