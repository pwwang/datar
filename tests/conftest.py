from datar import options


def pytest_sessionstart(session):
    # Load no plugins
    options(backends=[None])
