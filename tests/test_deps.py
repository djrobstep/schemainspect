
from sqlbag import S
from schemainspect import get_inspector

CREATES = """
    create extension pg_trgm;

    create or replace function "public"."fff"(t text)
    returns TABLE(score decimal) as
    $$ select similarity('aaa', 'aaab')::decimal $$
    language SQL VOLATILE CALLED ON NULL INPUT SECURITY INVOKER;

    create view vvv as select similarity('aaa', 'aaab')::decimal as x;

    create view depends_on_fff as select * from fff('t');

    create or replace function "public"."depends_on_vvv"(t text)
    returns TABLE(score decimal) as
    $$ select * from vvv $$
    language SQL VOLATILE CALLED ON NULL INPUT SECURITY INVOKER;

    create view doubledep as
    select * from depends_on_vvv('x')
    union
    select * from depends_on_fff;
"""


def test_relationships(db):
    # commented-out dependencies are the dependencies that aren't tracked directly by postgres
    with S(db) as s:
        s.execute(CREATES)
    with S(db) as s:
        i = get_inspector(s)
        dependencies_by_name = {
            k: v.dependent_on for k, v in i.selectables.items() if v.dependent_on
        }
        assert (
            dependencies_by_name
            == {
                # '"public"."depends_on_vvv"(t text)': [
                #     '"public"."vvv"'
                # ],
                '"public"."depends_on_fff"': ['"public"."fff"(t text)'],
                '"public"."doubledep"': [
                    '"public"."depends_on_fff"', '"public"."depends_on_vvv"(t text)'
                ],
            }
        )
        dependents_by_name = {
            k: v.dependents for k, v in i.selectables.items() if v.dependents
        }
        assert (
            dependents_by_name
            == {
                # '"public"."vvv"': ['"public"."depends_on_vvv"(t text)'],
                '"public"."fff"(t text)': ['"public"."depends_on_fff"'],
                '"public"."depends_on_fff"': ['"public"."doubledep"'],
                '"public"."depends_on_vvv"(t text)': ['"public"."doubledep"'],
            }
        )
        # testing recursive deps
        dependencies_by_name = {
            k: v.dependent_on_all
            for k, v in i.selectables.items()
            if v.dependent_on_all
        }
        assert (
            dependencies_by_name
            == {
                # '"public"."depends_on_vvv"(t text)': [
                #     '"public"."vvv"'
                # ],
                '"public"."depends_on_fff"': ['"public"."fff"(t text)'],
                '"public"."doubledep"': [
                    '"public"."depends_on_fff"',
                    '"public"."depends_on_vvv"(t text)',
                    '"public"."fff"(t text)',
                ],
                # '"public"."vvv"'
            }
        )
        dependents_by_name = {
            k: v.dependents_all for k, v in i.selectables.items() if v.dependents_all
        }
        assert (
            dependents_by_name
            == {
                # '"public"."vvv"': ['"public"."depends_on_vvv"(t text)', '"public"."doubledep"'],
                '"public"."fff"(t text)': [
                    '"public"."depends_on_fff"', '"public"."doubledep"'
                ],
                '"public"."depends_on_fff"': ['"public"."doubledep"'],
                '"public"."depends_on_vvv"(t text)': ['"public"."doubledep"'],
            }
        )
