from sqlalchemy.ext.asyncio import AsyncEngine

from backend.resume_ingestion.internal.domain.repository.ingestion_source.database.sql.sqlalchemy.manage_ingestion_source import (
    ManageIngestionRunsMixin,
)

class SqlAlchemyIngestionSourceRepository(ManageIngestionRunsMixin):
    def __init__(self, engine: AsyncEngine):
        self._engine = engine
