from collections import defaultdict
from streamz_postgres.pg.base_info import BaseInfo


class SchemaInfo(BaseInfo):
    """Information about relations in a database: tables and their columns."""

    async def refresh(self):
        res = await self.loader.execute(
            "SELECT attrelid, atttypid, attname FROM pg_attribute "
            "ORDER BY attrelid, attnum;"
        )
        info = defaultdict(lambda: [])
        for rel, typ, name in res:
            info[rel].append((typ, name))
        return info
