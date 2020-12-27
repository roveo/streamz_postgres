import asyncio
import csv
import io
import time

import psycopg2
from psycopg2 import sql
from psycopg2.extras import LogicalReplicationConnection
from streamz import Stream
from streamz.utils_test import wait_for
from streamz_postgres.cdc.sources import Format, from_postgres_cdc
from streamz_postgres.pg.database import Database
from streamz_postgres.tests import Writer
from streamz_postgres.tests.mock import MockClient

Stream.register_api(staticmethod)(from_postgres_cdc)


def debug(*args, **kwargs):
    import pdb

    pdb.set_trace()
    pass


def test_proto(pg):
    client = MockClient("streaming", **pg)
    client.create_table()
    client.start()
    time.sleep(2)

    with psycopg2.connect(**pg) as conn, conn.cursor() as cur:
        cur.execute("CREATE PUBLICATION test FOR ALL TABLES;")
        conn.commit()

    connection = psycopg2.connect(**pg, connection_factory=LogicalReplicationConnection)
    cursor = connection.cursor()

    cursor.create_replication_slot("test", output_plugin="pgoutput")
    options = dict(proto_version=1, publication_names="test")
    slot_name, consistent_point, snapshot_name, output_plugin = cursor.fetchone()

    with psycopg2.connect(**pg) as conn, conn.cursor() as cur:
        cur.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; "
            f"SET TRANSACTION SNAPSHOT '{snapshot_name}';"
        )
        fp = io.StringIO()
        cur.copy_expert(
            sql.SQL("COPY (SELECT * FROM streaming) TO STDOUT WITH CSV;"), fp
        )
        conn.commit()

    fp.seek(0)
    db = Database(**pg)
    reader = csv.reader(fp)
    loop = asyncio.get_event_loop()

    for row in reader:
        res = loop.run_until_complete(db.convert_row(client.oid, row))
        import pdb

        pdb.set_trace()

    client.stop()
    client.join()


def test_source(pg):
    writer = Writer(psycopg2.connect(**pg), "streaming")
    writer.create_table()

    source = Stream.from_postgres_cdc(pg, "test", ["test"], format=Format.PROTO)

    with writer.connection.cursor() as _cur:
        _cur.execute("create publication test for table streaming;")
        writer.connection.commit()

    writer.insert(10)

    L = source.sink_to_list()
    source.start()

    writer.update(5)
    writer.insert(2)
    writer.connection.close()

    time.sleep(1.5)
    import pdb

    pdb.set_trace()
    pass
