import asyncio

import pytest
from streamz_postgres.loaders import PostgresLoader, retry


def test_loader(pg):
    loader = PostgresLoader(**pg)
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(loader.execute("select 1;"))

    assert res[0][0] == 1


def test_loader_dict(pg):
    loader = PostgresLoader(**pg)
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(loader.execute("select 1 as a;"))

    assert dict(res[0]) == {"a": 1}


def test_retry():
    counter = 0

    @retry(wait=0.1, count=3)
    def _():
        nonlocal counter
        if counter < 3:
            counter += 1
            raise Exception

    _()


def test_retry_fail():
    counter = 0

    @retry(wait=0.1, count=3)
    def _():
        nonlocal counter
        if counter < 4:
            counter += 1
            raise Exception

    with pytest.raises(Exception):
        _()
