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
        snapshot = await self.loader.execute(self.snapshot())
        if snapshot[0]["snapshot"] == self.xmin_start:
            return
        count = await self.loader.execute(self.count(snapshot))
        if count[0]["count"] == 0:
            return
        for query in self.load(count):
            results = await self.loader.execute(query)
            for row in results:
                yield row

    @property
    def params(self):
        return dict(
            table=Identifier(*self.table),
            xmin_start=Literal(self.xmin_start),
            xmin_end=Literal(self.xmin_end),
            limit=Literal(self.limit),
            pk=Identifier(self.pk),
        )

    @staticmethod
    def snapshot():
        return SQL("select txid_snapshot_xmin(txid_current_snapshot()) as snapshot;")

    def count(self, snapshot):
        xmin = snapshot[0]["snapshot"]
        self.xmin_end = int(xmin)
        return SQL(
            "select count(*) from {table} "
            "where xmin::varchar::int >= {xmin_start} "
            "and xmin::varchar::int < {xmin_end};"
        ).format(**self.params)

    def load(self, count):
        cnt = count[0]["count"] or 0
        if cnt == 0:
            return
        params = self.params
        params["offset"] = Literal(0)
        for page in range(cnt // self.limit + 1):
            params["offset"] = Literal(page * self.limit)
            yield SQL(
                "select *, xmin, {xmin_end} as snapshot_xmin, now() as etl_ts "
                "from {table} "
                "where xmin::varchar::int >= {xmin_start} "
                "and xmin::varchar::int < {xmin_end} "
                "order by {pk} limit {limit} offset {offset};"
            ).format(**params)
        self.xmin_start = self.xmin_end


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
