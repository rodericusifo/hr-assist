from sqlalchemy.ext.asyncio import AsyncEngine

from .store_records import store_records

class SQLAlchemyResumeRepository:
    store_records = store_records

    def __init__(self, engine: AsyncEngine):
        self._engine = engine
