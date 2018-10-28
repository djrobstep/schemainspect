import pytest

from sqlbag import temporary_database


@pytest.yield_fixture()
def db():
    with temporary_database(host='localhost') as dburi:
        yield dburi
