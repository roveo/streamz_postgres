import asyncio

from streamz import Stream
from streamz.utils_test import wait_for
from streamz_postgres.sources import from_postgres_cdc, from_postgres_increment
from streamz_postgres.tests import Writer

Stream.register_api(staticmethod)(from_postgres_cdc)
Stream.register_api(staticmethod)(from_postgres_increment)


def test_cdc(pg):
    table = "cdc"
    src = Stream.from_postgres_cdc(table, pg, polling_interval=1, limit=3)
    L = src.sink_to_list()

    w = Writer(src.strategy.loader.connection, table)
    w.create_table()
    w.insert(10)
    src.start()

    wait_for(lambda: len(L) == 10, 2, period=0.1)

    w.update(5)
    wait_for(lambda: len(L) == 15, 2, period=0.1)

    w.update(8)
    wait_for(lambda: len(L) == 23, 2, period=0.1)


def test_backfill(pg):
    table = "backfill"
    src = Stream.from_postgres_cdc(table, pg, polling_interval=1, limit=3)
    L = src.sink_to_list()

    w = Writer(src.strategy.loader.connection, table)
    w.create_table()
    w.insert(10)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(src._do_backfill())

    assert len(L) == 10


def test_increment(pg):
    table = "inc"
    src = Stream.from_postgres_increment(table, pg, polling_interval=1)
    L = src.sink_to_list()

    w = Writer(src.strategy.loader.connection, table)
    w.create_table()
    w.insert(10)
    src.start()

    wait_for(lambda: len(L) == 10, 2, period=0.1)

    w.update(5)  # updates are not captured
    w.insert(8)
    wait_for(lambda: len(L) == 18, 2, period=0.1)

    w.insert(2)
    wait_for(lambda: len(L) == 20, 2, period=0.1)
