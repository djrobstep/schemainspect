import pytest
from sqlbag import temporary_database


@pytest.yield_fixture()
def db():
    with temporary_database(host="localhost") as dburi:
        yield dburi


def pytest_addoption(parser):
    parser.addoption(
        "--timescale", action="store_true", help="Test with Timescale extension"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "timescale: mark timescale specific tests")


def pytest_collection_modifyitems(config, items):
    skip_timescale = pytest.mark.skip(reason="need --timescale option to run")
    if not config.getoption("--timescale", default=False):
        # no option given in cli: skip the timescale tests
        for item in items:
            if "timescale" in item.keywords:
                item.add_marker(skip_timescale)
