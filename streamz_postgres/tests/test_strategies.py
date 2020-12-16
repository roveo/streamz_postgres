from streamz_postgres.loaders import PostgresLoader
from streamz_postgres.strategies import XminIncrement
from streamz_postgres.tests import Writer, async_generator_to_list


def test_xmin_backfill(pg):
    table = ("public", "xmin_backfill")
    loader = PostgresLoader(**pg)
    writer = Writer(loader.connection, table[1])
    writer.create_table()
    writer.insert(10)

    strategy = XminIncrement(table, loader)

    res = async_generator_to_list(strategy.backfill())

    assert len(res) == 10
    assert strategy.xmin_start != -1
    assert res[0]["snapshot_xmin"] is not None

    writer.update(3)
    writer.insert(3)

    # start from the correct snapshot after backfill
    ex = async_generator_to_list(strategy.execute())
    assert len(ex) == 6


def test_xmin_wraparound(pg):
    table = ("public", "xmin_wraparound")
    loader = PostgresLoader(**pg)
    writer = Writer(loader.connection, table[1])
    writer.create_table()
    writer.insert(10)

    strategy = XminIncrement(table, loader)
    strategy.xmin_start = 8 ** 32 - 10
    strategy.xmin_end = 1

    assert async_generator_to_list(strategy.execute()) == []
    assert strategy.xmin_start == 0
    assert strategy.xmin_end > strategy.xmin_start
    assert len(async_generator_to_list(strategy.execute())) == 10
