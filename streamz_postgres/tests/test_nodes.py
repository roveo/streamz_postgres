from streamz import Stream
from streamz.utils_test import wait_for
from streamz_postgres.nodes import postgres_lookup
from streamz_postgres.tests import Writer, uuid

Stream.register_api()(postgres_lookup)


def test_lookup_single(pg):
    table = uuid()
    src = Stream.from_postgres_increment(table, pg)
    L = src.postgres_lookup(table, "x", pg, target="lookup").sink_to_list()

    writer = Writer(src.strategy.loader.connection, table)
    writer.create_table()
    writer.insert(10)
    src.start()

    wait_for(lambda: len(L) == 10, 1, period=0.1)

    for row in L:
        lookup = row.pop("lookup")
        assert lookup == {"id": 1, "x": 1}


def test_lookup_partition(pg):
    table = uuid()
    src = Stream.from_postgres_increment(table, pg)
    L = (
        src.partition(3, timeout=0.001)
        .postgres_lookup(table, "x", pg, target="lookup")
        .flatten()
        .sink_to_list()
    )

    writer = Writer(src.strategy.loader.connection, table)
    writer.create_table()
    writer.insert(10)
    src.start()

    wait_for(lambda: len(L) == 10, 1, period=0.1)

    for row in L:
        lookup = row.pop("lookup")
        assert lookup == {"id": 1, "x": 1}
