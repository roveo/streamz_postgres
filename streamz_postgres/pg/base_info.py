class BaseInfo:
    def __init__(self, loader):
        self.loader = loader
        self.info = {}

    async def __getitem__(self, oid: int):
        if oid not in self.info:
            self.info = await self.refresh()
        return self.info[oid]

    async def refresh(self):
        raise NotImplementedError
