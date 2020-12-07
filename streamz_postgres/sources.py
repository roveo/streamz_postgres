import logging
from typing import Tuple, Union

from streamz import Source
from tornado import gen

from streamz_postgres.loaders import PostgresLoader
from streamz_postgres.strategies import Increment, Strategy, XminIncrement

logger = logging.getLogger(__name__)


class PostgresSource(Source):
    def __init__(
        self,
        strategy: Strategy,
        polling_interval: Union[int, Tuple[int, int]] = 60,
        **kwargs,
    ):
        super().__init__(ensure_io_loop=True, **kwargs)
        self.strategy = strategy
        self.table = strategy.table
        if isinstance(polling_interval, int):
            polling_interval = (polling_interval, polling_interval)
        self.polling_interval = polling_interval
        self.stopped = True

    def start(self):
        self.stopped = False
        self.loop.add_callback(self.do_poll)

    async def do_poll(self):
        dmin, dmax = self.polling_interval
        delay = 0
        while not self.stopped:
            if delay > 0:
                logger.info(f"{self.table}: sleeping for {round(delay, 1)} sec.")
                await gen.sleep(delay)

            logger.info(f"{self.table}: polling")
            rows = 0
            async for row in self.strategy.execute():
                await self.emit(row, asynchronous=True)
                rows += 1
                if rows % 100000 == 0:
                    logger.info(f"{self.table}: emitted {rows} rows and counting")
            if rows == 0:
                logger.info(f"{self.table}: no new rows")
                delay = dmax
            else:
                logger.info(f"{self.table}: emitted {rows} rows")
                delay = max(dmin, dmax * (50 / rows))
                delay = min(delay, dmax)

    @staticmethod
    def convert_table(table):
        if isinstance(table, tuple) and len(table) == 2:
            return table
        if isinstance(table, str) and "." in table:
            return tuple(table.split("."))
        else:
            return ("public", table)


class from_postgres_cdc(PostgresSource):
    def __init__(
        self,
        table,
        connection_params,
        limit=1000,
        initial_xmin=-1,
        pk="id",
        polling_interval=60,
        retry_count=0,
        retry_wait=10,
        backfill=False,
        **kwargs,
    ):
        self._backfill = backfill
        loader = PostgresLoader(
            **connection_params, retry_count=retry_count, retry_wait=retry_wait
        )
        strategy = XminIncrement(
            self.convert_table(table),
            loader,
            initial_xmin=initial_xmin,
            pk=pk,
            limit=limit,
        )
        super().__init__(
            strategy=strategy,
            polling_interval=polling_interval,
            **kwargs,
        )

    async def do_poll(self):
        if self._backfill:
            logger.info(f"{self.table}: performing backfill...")
            await self._do_backfill()
        await super().do_poll()

    async def _do_backfill(self):
        rows = 0
        async for row in self.strategy.backfill():
            await self.emit(row, asynchronous=True)
            rows += 1
            if rows % 100000 == 0:
                logger.info(f"{self.table}: backfill is at {rows} rows and counting")
        logger.info(f"{self.table}: finished backfilling {rows} rows")


class from_postgres_increment(PostgresSource):
    def __init__(
        self,
        table,
        connection_params,
        limit=1000,
        initial_value=-1,
        column="id",
        polling_interval=60,
        retry_count=0,
        retry_wait=10,
        **kwargs,
    ):
        loader = PostgresLoader(
            **connection_params, retry_count=retry_count, retry_wait=retry_wait
        )
        strategy = Increment(
            self.convert_table(table),
            loader,
            column=column,
            initial_value=initial_value,
            limit=limit,
        )
        super().__init__(
            strategy=strategy,
            polling_interval=polling_interval,
            **kwargs,
        )
