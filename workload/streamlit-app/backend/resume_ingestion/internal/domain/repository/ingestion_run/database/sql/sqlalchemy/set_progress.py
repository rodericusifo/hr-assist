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

async def set_progress(
    self: SqlAlchemyIngestionRunRepository,
    xid: str,
    projection: dict,
) -> None:
    progress_values = {
        "extract": (0.1, "Extracting resumes"),
        "transform_clean": (0.3, "Cleaning records"),
        "transform_enrich": (0.5, "Enriching records"),
        "transform_chunk": (0.7, "Creating knowledge chunks"),
        "load": (0.9, "Loading knowledge base"),
        "complete": (1.0, "Ingestion complete"),
        "error": (0.0, "Workflow failed"),
    }
    stage = projection.get("stage", "starting")
    value, message = progress_values.get(
        stage,
        (0.0, "Starting workflow"),
    )

    async with AsyncSession(self._engine) as session:
        result = await session.execute(
            select(IngestionRun).filter_by(xid=xid)
        )
        run = result.scalar_one_or_none()
        if run:
            run.progress_stage = stage
            run.progress_value = value
            run.progress_message = message

            # COALESCE: only update if new value provided
            new_last_processed = projection.get("last_processed_record_id")
            if new_last_processed is not None:
                run.last_processed_record_id = new_last_processed

            run.updated_at = datetime.now(timezone.utc)
            await session.commit()