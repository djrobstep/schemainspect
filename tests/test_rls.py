from sqlbag import S

from schemainspect import get_inspector

schemainspect_test_role = "schemainspect_test_role"


def create_role(s, rolename):
    role = s.execute(
        """
SELECT 1 FROM pg_roles WHERE rolname=:rolename
    """,
        dict(rolename=rolename),
    )

    role_exists = bool(list(role))

    if not role_exists:
        s.execute(
            """
            create role {rolename};
        """.format(
                rolename=rolename
            )
        )


def test_rls(db):
    with S(db) as s:
        i = get_inspector(s)

        if i.pg_version <= 9:
            return

        s.execute(
            """
CREATE TABLE t(id uuid, a text, b decimal);
        """
        )

        i = get_inspector(s)
        t = i.tables['"public"."t"']

        assert t.rowsecurity is False
        assert (
            t.alter_rls_statement
            == 'alter table "public"."t" disable row level security;'
        )

        t.rowsecurity = True
        s.execute(t.alter_rls_statement)
        i = get_inspector(s)
        t = i.tables['"public"."t"']
        assert t.rowsecurity is True
        assert (
            t.alter_rls_statement
            == 'alter table "public"."t" enable row level security;'
        )

        create_role(s, schemainspect_test_role)

        s.execute(
            """

CREATE TABLE accounts (manager text, company text, contact_email text);

ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

CREATE POLICY account_managers ON accounts TO {schemainspect_test_role}
    USING (manager = (CURRENT_USER)::text);

create policy "insert_gamer"
on accounts
as permissive
for insert
to {schemainspect_test_role}
with check (manager = (CURRENT_USER)::text);

        """.format(
                schemainspect_test_role=schemainspect_test_role
            )
        )
        i = get_inspector(s)

        pname = '"public"."accounts"."account_managers"'
        t = i.rlspolicies[pname]
        assert t.name == "account_managers"
        assert t.schema == "public"
        assert t.table_name == "accounts"
        assert t.commandtype == "*"
        assert t.permissive is True
        assert t.roles == ["schemainspect_test_role"]
        assert t.qual == "(manager = (CURRENT_USER)::text)"
        assert t.withcheck is None

        assert (
            t.create_statement
            == """create policy "account_managers"
on "public"."accounts"
as permissive
for all
to schemainspect_test_role
using ((manager = (CURRENT_USER)::text));
"""
        )

        assert (
            t.drop_statement == 'drop policy "account_managers" on "public"."accounts";'
        )

        s.execute(t.drop_statement)
        s.execute(t.create_statement)
        i = get_inspector(s)
        t = i.rlspolicies[pname]
        assert t.name == "account_managers"
        assert t.schema == "public"
        assert t.table_name == "accounts"
        assert t.commandtype == "*"
        assert t.permissive is True
        assert t.roles == ["schemainspect_test_role"]
        assert t.qual == "(manager = (CURRENT_USER)::text)"
        assert t.withcheck is None

        pname = '"public"."accounts"."insert_gamer"'
        t = i.rlspolicies[pname]
        assert t.name == "insert_gamer"
        assert t.schema == "public"
        assert t.table_name == "accounts"
        assert t.commandtype == "a"
        assert t.permissive is True
        assert t.roles == ["schemainspect_test_role"]
        assert t.withcheck == "(manager = (CURRENT_USER)::text)"
        assert t.qual is None

        assert (
            t.create_statement
            == """create policy "insert_gamer"
on "public"."accounts"
as permissive
for insert
to schemainspect_test_role
with check ((manager = (CURRENT_USER)::text));
"""
        )

        assert t.drop_statement == 'drop policy "insert_gamer" on "public"."accounts";'
