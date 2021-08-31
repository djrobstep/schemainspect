import sys

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

CREATES_FK = """
create schema other;

create type emptype as enum('a', 'b', 'c');


CREATE TABLE other.emp (
    id bigint primary key,
    empname text,
    category emptype
);

create table salary (
    emp_id bigint unique references other.emp(id),
    salary bigint not null
);

create view empview as (
    select
        *
    from other.emp
        join salary on
            emp.id = salary.emp_id
);

CREATE FUNCTION emp() RETURNS trigger AS $emp_stamp$
    BEGIN
        -- Who works for us when they must pay for it?
        IF NEW.salary < 0 THEN
            RAISE EXCEPTION '% cannot have a negative salary', NEW.empname;
        END IF;

        -- Remember who changed the payroll
        NEW.last_user := current_user;
        RETURN NEW;
    END;
$emp_stamp$ LANGUAGE plpgsql;

CREATE TRIGGER emp_stamp BEFORE INSERT OR UPDATE ON other.emp
    FOR EACH ROW EXECUTE FUNCTION emp();

"""


def test_dep_order(db):
    with S(db) as s:
        i = get_inspector(s)

        if i.pg_version <= 10:
            return

        # s.execute(CREATES)
        s.execute(CREATES_FK)

        i = get_inspector(s)

        # dependency_order doesn't work in py2
        if sys.version_info < (3, 0):
            return
        create_order = i.dependency_order(
            include_fk_deps=True,
        )

        drop_order = i.dependency_order(
            drop_order=True,
            include_fk_deps=True,
        )

        for x in drop_order:
            thing = i.get_dependency_by_signature(x)

            drop = thing.drop_statement
            s.execute(drop)

        for x in create_order:
            thing = i.get_dependency_by_signature(x)

            create = thing.create_statement
            s.execute(create)


def test_fk_info(db):
    with S(db) as s:
        i = get_inspector(s)

        if i.pg_version <= 10:
            return

        s.execute(CREATES_FK)

        i = get_inspector(s)

        fk = i.constraints['"public"."salary"."salary_emp_id_fkey"']

        assert fk.is_fk is True
        assert fk.quoted_full_foreign_table_name == '"other"."emp"'
