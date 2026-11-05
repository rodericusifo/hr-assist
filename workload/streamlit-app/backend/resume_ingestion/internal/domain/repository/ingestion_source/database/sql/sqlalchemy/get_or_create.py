from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .init import (
        SqlAlchemyIngestionSourceRepository,
    )

from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.resume_ingestion.internal.domain.model.database.sql.sqlalchemy.ingestion_source import (
    IngestionSource,
)

async def get_or_create(
    self: SqlAlchemyIngestionSourceRepository,
    provider: str,
    file_path: str,
) -> int:
    async with AsyncSession(self._engine) as session:
        # Try to find existing source
        result = await session.execute(
            select(IngestionSource).filter_by(
                provider=provider, file_path=file_path
            )
        )
        source = result.scalar_one_or_none()

        if source is None:
            # Create new source
            source = IngestionSource(
                xid=str(uuid4()),
                provider=provider,
                file_path=file_path,
            )
            session.add(source)
            await session.commit()
            await session.refresh(source)

        return source.id
