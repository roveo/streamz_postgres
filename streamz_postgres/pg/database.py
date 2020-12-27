from streamz_postgres.loaders import PostgresLoader
from streamz_postgres.pg.data_types_info import DataTypesInfo
from streamz_postgres.pg.schema_info import SchemaInfo


class Database:
    """Fast and easy access to info about a database. Tables, data types etc."""

    def __init__(self, **client_params):
        loader = PostgresLoader(**client_params)
        self.data_types = DataTypesInfo(loader)
        self.schema = SchemaInfo(loader)

    async def convert_row(self, relation_id: int, row: list):
        columns = await self.schema[relation_id]
        result = {}
        for val, (typ, name) in zip(row, columns):
            result[name] = self.data_types[typ].convert(val)
        return result
