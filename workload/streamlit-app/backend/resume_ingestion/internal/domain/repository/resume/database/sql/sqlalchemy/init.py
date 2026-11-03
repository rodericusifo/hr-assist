from __future__ import annotations

from sqlalchemy import Engine

from backend.resume_ingestion.internal.domain.repository.resume.database.sql.sqlalchemy.store_records import (
    StoreRecordsMixin,
)


class SQLAlchemyDatabaseResumeRepository(StoreRecordsMixin):
    def __init__(self, engine: Engine):
        self._engine = engine
