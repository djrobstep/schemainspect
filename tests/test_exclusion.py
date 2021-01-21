from sqlbag import S

from schemainspect import get_inspector

CREATE = """
create table t (
    id integer not null primary key,
    starts_at timestamp not null,
    ends_at timestamp not null,
    exclude using gist (tsrange(starts_at, ends_at) with &&) -- disallow overlapping time intervals
);
"""


def test_exclusion_constraint(db):
    """
    Test that Exclusion constraints are parsed, and that SQL for exclusion
    constraints is generated correctly:
    - EXCLUDE USING ... is generated
    - No explicit index creation
    """
    with S(db) as s:
        s.execute(CREATE)

        i = get_inspector(s)
        constraints_keys = list(i.constraints.keys())
        assert constraints_keys == [
            '"public"."t"."t_pkey"',
            '"public"."t"."t_tsrange_excl"',
        ]
        ex_constr = i.constraints['"public"."t"."t_tsrange_excl"']
        assert ex_constr.constraint_type == "EXCLUDE"

        indexes_keys = list(i.indexes.keys())
        assert indexes_keys == [
            '"public"."t_pkey"',
            '"public"."t_tsrange_excl"',
        ]
        ex_index = i.indexes['"public"."t_tsrange_excl"']
        assert ex_index.constraint == ex_constr

        assert (
            ex_constr.create_statement
            == 'alter table "public"."t" add constraint "t_tsrange_excl" EXCLUDE USING gist (tsrange(starts_at, ends_at) WITH &&);'
        )
        assert (
            ex_index.create_statement
            == "select 1; -- CREATE INDEX t_tsrange_excl ON public.t USING gist (tsrange(starts_at, ends_at));"
        )
