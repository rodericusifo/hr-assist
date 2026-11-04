from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.resume_ingestion.internal.domain.model.database.sql.sqlalchemy.ingestion_run import (
    IngestionRun,
)

if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.repository.ingestion_run.database.sql.sqlalchemy.init import (
        SqlAlchemyIngestionRunRepository,
    )

class ManageIngestionRunMixin:
    async def get_or_create(
        self: SqlAlchemyIngestionRunRepository,
        source_id: int,
    ) -> tuple[str, str | None, bool]:
        """Get or create the current ingestion run.

        Returns (xid, last_processed_record_id, is_retry).
        - New run: (xid, None, False)
        - Failed retry: (xid, None, True) — pass state=None to workflow so
          LangGraph restores state from checkpoint.
        - Succeeded/running: (xid, last_processed_record_id, False) — seed
          workflow state so it resumes after the last processed record.
        """
        async with AsyncSession(self._engine) as session:
            # Find current run for this source
            result = await session.execute(
                select(IngestionRun).filter_by(
                    ingestion_source_id=source_id, is_current=True
                )
            )
            current_run = result.scalar_one_or_none()

            if current_run is None:
                # No current run - create new one
                new_run = IngestionRun(
                    xid=str(uuid4()),
                    ingestion_source_id=source_id,
                    last_processed_record_id=None,
                    status="running",
                    is_current=True,
                )
                session.add(new_run)
                await session.commit()
                await session.refresh(new_run)
                return new_run.xid, None, False

            elif current_run.status == "failed":
                # Reset failed run to running state
                # LangGraph checkpoint will restore workflow state when resuming
                current_run.status = "running"
                current_run.error = None
                current_run.progress_stage = None
                current_run.progress_value = None
                current_run.progress_message = None
                current_run.updated_at = datetime.now(timezone.utc)
                await session.commit()
                await session.refresh(current_run)
                return current_run.xid, None, True

            elif current_run.status == "succeeded":
                # Mark current as not current, create new run
                # Carry forward last_processed_record_id so workflow resumes after it
                resume_record_id = current_run.last_processed_record_id
                current_run.is_current = False
                current_run.updated_at = datetime.now(timezone.utc)

                new_run = IngestionRun(
                    xid=str(uuid4()),
                    ingestion_source_id=source_id,
                    last_processed_record_id=resume_record_id,
                    status="running",
                    is_current=True,
                )
                session.add(new_run)
                await session.commit()
                await session.refresh(new_run)
                return (
                    new_run.xid,
                    str(resume_record_id) if resume_record_id else None,
                    False,
                )

            elif current_run.status == "running":
                # Use existing running run, resume from last processed position
                resume_record_id = current_run.last_processed_record_id
                return (
                    current_run.xid,
                    str(resume_record_id) if resume_record_id else None,
                    False,
                )

            else:
                raise RuntimeError(
                    f"Unsupported ingestion run status: {current_run.status!r}"
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
