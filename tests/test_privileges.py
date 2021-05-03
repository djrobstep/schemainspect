from schemainspect.pg.obj import InspectedPrivilege


def test_inspected_privilege():
    a = InspectedPrivilege("table", "public", "test_table", "select", "test_user")
    a2 = InspectedPrivilege("table", "public", "test_table", "select", "test_user")
    b = InspectedPrivilege(
        "function", "schema", "test_function", "execute", "test_user"
    )
    b2 = InspectedPrivilege(
        "function", "schema", "test_function", "modify", "test_user"
    )
    assert a == a2
    assert a == a
    assert a != b
    assert b != b2
    assert (
        b2.create_statement
        == 'grant modify on function "schema"."test_function" to "test_user";'
    )
    assert (
        b.drop_statement
        == """DO
$$
    BEGIN
        IF (SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'schema'
              AND table_name = 'test_function'
        ) THEN
            REVOKE execute on function "schema"."test_function" from "test_user";
        END IF;
    END
$$;"""
    )
    assert a.key == ("table", '"public"."test_table"', "test_user", "select")
