from streamz_postgres.pg.base_info import BaseInfo
from psycopg2.sql import SQL


class DataTypesInfo(BaseInfo):
    """Container of data types available in a particular database."""

    known_types = {}

    async def refresh(self):
        """Refresh the container."""
        res = await self.loader.execute(SQL("SELECT oid, typename FROM pg_type;"))
        return {oid: name for oid, name in res}

    @classmethod
    def register_data_type(cls, data_type):
        cls.known_types[data_type.name] = data_type

    def convert(self, typname, val):
        t = self.known_types.get(typname)
        if t is None:
            return val
        return t.convert(val)
