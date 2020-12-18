import functools
import logging
import time

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.sql import SQL, Composed, Identifier, Literal
from tornado.ioloop import IOLoop

logger = logging.getLogger(__name__)


executor = None


def retry(count=3, wait=5, exc=Exception, callback=None):
    def wrapper(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            retries_left = count + 1
            err = None
            while retries_left > 0:
                try:
                    return fn(*args, **kwargs)
                except exc as e:
                    retries_left -= 1
                    if callable(callback):
                        callback()
                    err = e
                    time.sleep(wait)
                    continue
            raise err

        return wrapped

    return wrapper


class Loader:

    cursor_kwargs: dict = {}
    retry_exception_cls = Exception

    def __init__(self, retry_count=0, retry_wait=10, **kwargs):
        """A class that actually makes async queries to a database.
        Handles retries for you.
        """
        self.connection_kwargs = kwargs
        self._connection = None

        self.query = retry(
            count=retry_count,
            wait=retry_wait,
            exc=self.retry_exception_cls,
            callback=self.refresh,
        )(self.query)

    def get_connection(self):
        raise NotImplementedError

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.get_connection()
        return self._connection

    def refresh(self):
        if self._connection is not None:
            try:
                self._connection.close()
            except self.retry_exception_cls:
                pass
            self._connection = None

    def query(self, sql):
        with self.connection.cursor(**self.cursor_kwargs) as cur:
            _sql = sql
            if isinstance(sql, (SQL, Composed)):
                _sql = sql.as_string(cur)
            logger.debug(_sql)
            cur.execute(_sql)
            return cur.fetchall()

    async def execute(self, sql):
        return await IOLoop.current().run_in_executor(executor, self.query, sql)


class PostgresLoader(Loader):

    cursor_kwargs = dict(cursor_factory=DictCursor)
    retry_exception_cls = psycopg2.Error

    def get_connection(self):
        return psycopg2.connect(**self.connection_kwargs)

    async def get_tables(self):
        sql = (
            "select table_schema, table_name "
            "from information_schema.tables "
            "where table_schema not in ('information_schema', 'pg_catalog') "
            "order by table_schema, table_name;"
        )
        return await self.execute(sql)

    async def lookup(self, table, key, values):
        sql = SQL("select * from {table} where {key} in {values};").format(
            table=Identifier(table), key=Identifier(key), values=Literal(values)
        )
        return await self.execute(sql)
