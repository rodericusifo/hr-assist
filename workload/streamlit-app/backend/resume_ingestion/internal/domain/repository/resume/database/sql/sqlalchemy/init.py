from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine

from backend.resume_ingestion.internal.domain.repository.resume.database.sql.sqlalchemy.store_records import (
    StoreRecordsMixin,
)

class SQLAlchemyResumeRepository(StoreRecordsMixin):
    def __init__(self, engine: AsyncEngine):
        self._engine = engine

