from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .init import (
        SqlAlchemyIngestionRunRepository,
    )

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.resume_ingestion.internal.domain.model.database.sql.sqlalchemy.ingestion_run import (
    IngestionRun,
)

async def set_status(
    self: SqlAlchemyIngestionRunRepository,
    xid: str,
    status: str,
    error: str | None,
) -> None:
    async with AsyncSession(self._engine) as session:
        result = await session.execute(
            select(IngestionRun).filter_by(xid=xid)
        )
        run = result.scalar_one_or_none()
        if run:
            run.status = status
            run.error = error
            run.updated_at = datetime.now(timezone.utc)
            await session.commit()
