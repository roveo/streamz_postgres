import logging

from psycopg2.sql import SQL, Identifier, Literal

from streamz_postgres.loaders import PostgresLoader

logger = logging.getLogger(__name__)


class Strategy:
    def __init__(self, table, loader: PostgresLoader):
        self.table = table
        self.loader = loader

    async def execute(self):
        raise NotImplementedError


class XminIncrement(Strategy):
    def __init__(
        self,
        table,
        loader: PostgresLoader,
        initial_xmin=-1,
        pk="id",
        limit=1000,
    ):
        super().__init__(table, loader)
        self.limit = limit
        self.pk = pk
        self.xmin_start = int(initial_xmin)
        self.xmin_end = None

    async def execute(self):
        snapshot = await self.snapshot()
        if snapshot == self.xmin_start:  # no new transactions
            return
        self.xmin_end = snapshot
        if snapshot < self.xmin_start:  # handle txid wraparound
            self.xmin_end = 2 ** 32
        state = -1
        while True:
            res = await self.loader.execute(self.load(state))
            for row in res:
                state = max(state, row[self.pk])
                yield row
            if len(res) < self.limit:
                break
        self.xmin_start = self.xmin_end
        if snapshot < self.xmin_start:
            self.xmin_start = 0

    async def backfill(self):
        """Faster initial load."""
        snapshot = await self.snapshot()
        state = -1
        query = SQL(
            "select *, xmin, {xmin_end} as snapshot_xmin "
            "from {table} "
            "where {pk} > {state} "
            "order by {pk} limit {limit};"
        )
        while True:
            res = await self.loader.execute(
                query.format(state=Literal(state), **self.params)
            )
            for row in res:
                state = max(state, row[self.pk])
                if int(row["xmin"]) < snapshot:
                    yield row
            if len(res) < self.limit:
                break
        self.xmin_start = snapshot  # start from loaded snapshot

    @property
    def params(self):
        return dict(
            table=Identifier(*self.table),
            xmin_start=Literal(self.xmin_start),
            xmin_end=Literal(self.xmin_end),
            limit=Literal(self.limit),
            pk=Identifier(self.pk),
        )

    async def snapshot(self):
        return (
            await self.loader.execute(
                SQL("select txid_snapshot_xmin(txid_current_snapshot()) as snapshot;")
            )
        )[0]["snapshot"]

    def load(self, from_pk):
        return SQL(
            "select *, xmin, {xmin_end} as snapshot_xmin "
            "from {table} "
            "where {pk} > {state} "
            "and xmin::varchar::int >= {xmin_start} "
            "and xmin::varchar::int < {xmin_end} "
            "order by {pk} limit {limit};"
        ).format(**self.params, state=Literal(from_pk))


class Increment(Strategy):
    def __init__(
        self,
        table,
        loader: PostgresLoader,
        column: str = "id",
        initial_value=-1,
        limit: int = 10000,
    ):
        super().__init__(table, loader)
        self.column = column
        self.limit = limit
        self.state = initial_value

    async def execute(self):
        results = await self.loader.execute(self.query())
        while len(results) > 0:
            for row in results:
                yield row
                self.state = max(self.state, row[self.column])
            results = await self.loader.execute(self.query())

    def query(self):
        return SQL(
            "select *, xmin, now() as etl_ts "
            "from {table} where {column} > {state} "
            "order by {column} limit {limit};"
        ).format(
            table=Identifier(*self.table),
            column=Identifier(self.column),
            state=Literal(self.state),
            limit=Literal(self.limit),
        )
