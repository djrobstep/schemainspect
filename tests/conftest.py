import pytest

from sqlbag import temporary_database


@pytest.yield_fixture(scope='module')
def db():
    with temporary_database('postgresql') as dburi:
        yield dburi
