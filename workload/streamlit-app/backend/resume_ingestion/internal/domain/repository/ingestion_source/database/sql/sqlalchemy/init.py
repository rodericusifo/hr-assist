from sqlalchemy.ext.asyncio import AsyncEngine

from .get_or_create import (
    get_or_create,
)

class SqlAlchemyIngestionSourceRepository:
    get_or_create = get_or_create

    def __init__(self, engine: AsyncEngine):
        self._engine = engine
