import functools
import random
import string
import time
from datetime import datetime
from threading import Thread, Event

import pytz
from psycopg2 import connect, sql


def or_none(f):
    @functools.wraps(f)
    def inner():
        if random.choice([False, False, False, False, True]):
            return None
        return f()

    return inner


@or_none
def generate_bool():
    return random.choice([False, True])


@or_none
def generate_float():
    return random.random()


@or_none
def generate_ts():
    return datetime.now()


@or_none
def generate_tz():
    return datetime.now().replace(tzinfo=pytz.timezone("UTC"))


def generate_str(maxlen):
    @or_none
    def inner():
        return "".join(
            random.choices(
                string.ascii_uppercase + string.digits, k=random.randint(0, maxlen)
            )
        )

    return inner


field_mapping = {
    "v": generate_str(128),
    "t": generate_str(10000),
    "ts": generate_ts,
    "tz": generate_tz,
    "b": generate_bool,
    "f": generate_float,
}


class MockClient(Thread):
    def __init__(self, table, weights=(0.6, 0.3, 0.1), interval=0.1, **kwargs):
        super().__init__()
        self.table = table
        self.connection = connect(**kwargs)
        self.weights = weights
        self.interval = interval
        self.inserts = 0
        self.updates = 0
        self.deletes = 0
        self.stopped = Event()
        self.oid = None

    def run(self):
        while not self.stopped.is_set():
            self.tick()
            time.sleep(self.interval)

    def stop(self):
        self.stopped.set()

    def bootstrap(self, n=1000):
        for _ in range(n):
            self.tick()

    def tick(self):
        (action,) = random.choices(
            ["insert", "update", "delete"], weights=self.weights, k=1
        )
        getattr(self, action)()

    def insert(self):
        values = {k: sql.Literal(v()) for k, v in field_mapping.items()}
        self.execute(
            sql.SQL(
                "INSERT INTO {table} (v, t, ts, tz, b, f) "
                "VALUES ({v}, {t}, {ts}, {tz}, {b}, {f});"
            ).format(**values, **self.params),
        )
        self.inserts += 1

    def update(self):
        field = random.choice("v t ts tz b f".split())
        val = field_mapping[field]()
        pk = self.random_pk()
        if pk is not None:
            self.execute(
                sql.SQL(
                    "UPDATE {table} SET {field} = {val} " "WHERE pk = {pk};"
                ).format(
                    pk=sql.Literal(pk),
                    field=sql.Identifier(field),
                    val=sql.Literal(val),
                    **self.params
                ),
            )
            self.updates += 1

    def delete(self):
        pk = self.random_pk()
        if pk is not None:
            self.execute(
                sql.SQL("DELETE FROM {table} WHERE pk = {pk};").format(
                    pk=sql.Literal(pk), **self.params
                )
            )
            self.deletes += 1

    def random_pk(self):
        with self.connection.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT pk FROM {table} ORDER BY random() LIMIT 1;").format(
                    **self.params
                )
            )
            res = cur.fetchone()
            if res is not None:
                return res[0]

    def execute(self, query):
        with self.connection.cursor() as cur:
            cur.execute(query)
            self.connection.commit()

    def create_table(self):
        query = sql.SQL(
            "CREATE TABLE {table} ( "
            "pk serial not null primary key, "
            "v varchar(128), "
            "t text, "
            "ts timestamp, "
            "tz timestamptz, "
            "b boolean, "
            "f float "
            ");"
        ).format(**self.params)
        self.execute(query)
        self.oid = self.get_oid()

    @property
    def params(self):
        return dict(table=sql.Identifier(self.table))

    def get_oid(self):
        with self.connection.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT oid FROM pg_class WHERE relname = {table};").format(
                    table=sql.Literal(self.table)
                )
            )
            (oid,) = cur.fetchone()
            return oid
