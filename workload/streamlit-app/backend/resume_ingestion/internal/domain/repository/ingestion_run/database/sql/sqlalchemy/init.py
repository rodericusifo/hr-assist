from sqlalchemy.ext.asyncio import AsyncEngine


from backend.resume_ingestion.internal.domain.repository.ingestion_run.database.sql.sqlalchemy.manage_ingestion_run import (
    ManageIngestionRunMixin,
)

class SqlAlchemyIngestionRunRepository(ManageIngestionRunMixin):
    def __init__(self, engine: AsyncEngine):
        self._engine = engine
