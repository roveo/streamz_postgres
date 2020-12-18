import asyncio

from psycopg2.extras import DictRow
from streamz import Stream

from streamz_postgres.loaders import PostgresLoader


class postgres_lookup(Stream):
    """Look up values in a Postgres table. Accepts both individual records
    (``DictRow`` or ``dict``) and batches of records (output of ``partition`` node).
    Will emit data in the same form as input, either individual dicts or batches of
    dict.

    Parameters
    ----------
    table: str
        Table name to look up in.
    field: str
        Input field to look up by.
    client_params: dict
        ``psycopg2`` connection parameters.
    key: str
        Target field by which lookup will be performed. For example, if
        ``table="users"``, ``field="user_id`` and ``key="id"``, the node will look up
        records in table ``users``, where ``id`` equals to ``user_id`` field in input
        item. Defaults to ``"id"``.
    target: str
        Looked up values will be written to the output dict at this key. Defaults to the
        value of ``table`` parameter.
    """

    def __init__(
        self,
        upstream,
        table,
        field,
        client_params,
        key="id",
        target=None,
        retry_count=0,
        retry_wait=1,
        **kwargs
    ):
        super().__init__(upstream, **kwargs)
        self._table = table
        self._key = key
        self._field = field
        self._target = target if target is not None else table
        self._loader = PostgresLoader(
            retry_count=retry_count, retry_wait=retry_wait, **client_params
        )

    def _lookup(self, future, emit_batches):
        async def cb(xs, metadata=None):
            values = tuple(x[self._field] for x in xs)
            looked_up = await self._loader.lookup(self._table, self._key, values)
            mapping = {row[self._key]: dict(row) for row in looked_up}
            result = []
            for x in xs:
                result_row = dict(x)
                result_row[self._target] = mapping[result_row[self._field]]
                result.append(result_row)
            if not emit_batches:
                result = result[0]
            else:
                result = tuple(result)
            await self.emit(result, metadata=metadata, asynchronous=True)
            self._release_refs(metadata)
            future.set_result(None)

        return cb

    def update(self, x, who=None, metadata=None):
        if not isinstance(x, (tuple, DictRow, dict)):
            raise ValueError(
                "Input to postgres_lookup must be psycopg2.extras.DictRow, dict "
                "or the output of partition node containing them."
            )
        _x = x
        emit_batches = True
        if isinstance(x, (dict, DictRow)):
            _x = (x,)
            emit_batches = False
        future = asyncio.Future()
        self._retain_refs(metadata)
        self.loop.add_callback(
            self._lookup(future, emit_batches), _x, metadata=metadata
        )
        return future
