from sqlbag import S

from schemainspect import get_inspector


CREATE = """
create table t (id integer not null primary key);
"""


def test_repr(db):
    with S(db) as s:
        s.execute(CREATE)
        i = get_inspector(s)

        table = i.tables['"public"."t"']
        assert repr(table).startswith("InspectedSelectable(")

        c = table.constraints['"public"."t"."t_pkey"']
        assert repr(c).startswith("InspectedConstraint(")
        assert "constraint=..." in repr(c)
