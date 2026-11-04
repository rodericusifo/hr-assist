
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.init import (
        LanggraphResumeService,
    )

import asyncio
from pathlib import Path
from typing import AsyncGenerator
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.init import (
    IngestResumeRecordsToKnowledgeBaseWorkflow,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from backend.resume_ingestion.internal.pkg.config.config_database_sql import (
    database_sql_connection_string,
)
from backend.resume_ingestion.internal.pkg.constant.dialect import Dialect

class IngestResumeRecordsToKnowledgeBaseMixin:
    async def ingest_resume_records_to_knowledge_base(
        self: LanggraphResumeService,
        provider: str,
        file_path: str,
        max_records_to_extract: int,
    ) -> AsyncGenerator[dict, None]:
        if max_records_to_extract < 1:
            raise ValueError("The number of records to ingest must be at least 1")

        connection_string = database_sql_connection_string(Dialect.POSTGRES)
        source_key = (
            str(Path(file_path).resolve())
            if provider == "file"
            else file_path.strip()
        )
        lifecycle_key = (provider, source_key)
        lock_key = f"{lifecycle_key[0]}:{lifecycle_key[1]}"
        lock_engine = create_async_engine(
            database_sql_connection_string(Dialect.POSTGRES).replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        )
        try:
            async with lock_engine.begin() as lock_connection:
                await lock_connection.execute(
                    text("SELECT pg_advisory_lock(hashtextextended(:key, 0))"),
                    {"key": lock_key},
                )
                try:
                    source_id = await self._sql_database_ingestion_source_repository.get_or_create(
                        provider,
                        source_key,
                    )
                    xid, last_processed_record_id, is_retry = await self._sql_database_ingestion_run_repository.get_or_create(
                        source_id,
                    )
                    # Build state inside lock session
                    # Always provide initial state with provider and file_path.
                    # For retries, LangGraph restores additional state from checkpoint.
                    # For new/resumed runs, we explicitly set last_processed_record_id.
                    state = (
                        None
                        if is_retry
                        else IngestResumeRecordsToKnowledgeBaseWorkflowState(
                            provider=provider,
                            file_path=file_path,
                            last_processed_record_id=last_processed_record_id,
                            max_records_to_extract=max_records_to_extract,
                        )
                    )
                    context = IngestResumeRecordsToKnowledgeBaseWorkflowContext(
                        llm=self._llm,
                        chunk_size=1200,
                        chunk_overlap=180,
                    )
                    await self._sql_database_ingestion_run_repository.set_status(
                        xid,
                        "running",
                        None,
                    )
                    async with AsyncPostgresSaver.from_conn_string(
                        connection_string
                    ) as checkpointer:
                        try:
                            workflow = IngestResumeRecordsToKnowledgeBaseWorkflow(
                                vector_database_resume_repository=(
                                    self._vector_database_resume_repository
                                ),
                                sql_database_resume_repository=(
                                    self._sql_database_resume_repository
                                ),
                            ).compile(checkpointer=checkpointer)
                            # For retries, pass None to restore full state from checkpoint.
                            # For new/resumed runs, provide initial state.
                            stream = await workflow.astream_events(
                                state,
                                config={"configurable": {"thread_id": xid}},
                                context=context,
                                version="v3",
                            )

                            workflow_failed = False
                            last_snapshot = None
                            async for snapshot in stream.values:
                                workflow_failed = (
                                    workflow_failed
                                    or self._snapshot_has_error(snapshot)
                                )
                                last_snapshot = snapshot
                                projection = self._ui_projection(snapshot)
                                await self._sql_database_ingestion_run_repository.set_progress(
                                    xid,
                                    projection,
                                )
                                yield {
                                    "event": "progress",
                                    "data": projection,
                                }
                        except (GeneratorExit, asyncio.CancelledError):
                            await self._sql_database_ingestion_run_repository.set_status(
                                xid,
                                "failed",
                                "Ingestion was interrupted before completion",
                            )
                            raise
                        except Exception as error:
                            await self._sql_database_ingestion_run_repository.set_status(
                                xid,
                                "failed",
                                str(error),
                            )
                            raise
                        if workflow_failed:
                            await self._sql_database_ingestion_run_repository.set_status(
                                xid,
                                "failed",
                                "Workflow completed with an error",
                            )
                        else:
                            await self._sql_database_ingestion_run_repository.set_progress(
                                xid,
                                {"stage": "complete"},
                            )
                            await self._sql_database_ingestion_run_repository.set_status(
                                xid,
                                "succeeded",
                                None,
                            )
                            yield {
                                "event": "complete",
                                "data": self._ui_projection(
                                    last_snapshot,
                                    status="succeeded",
                                ),
                            }
                except (GeneratorExit, asyncio.CancelledError):
                    raise
                except Exception as error:
                    await self._sql_database_ingestion_run_repository.set_status(
                        xid,
                        "failed",
                        str(error),
                    )
                    raise
                finally:
                    if not lock_connection.closed:
                        await lock_connection.execute(
                            text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                            {"key": lock_key},
                        )
        finally:
            await lock_engine.dispose()

    @classmethod
    def _ui_projection(
        cls,
        snapshot: object,
        *,
        status: str = "running",
    ) -> dict:
        def records(key: str) -> list[dict]:
            value = getattr(snapshot, key, None)
            if value is None and isinstance(snapshot, dict):
                value = snapshot.get(key)
            return value if isinstance(value, list) else []

        extracted = records("extracted_records")
        cleaned = records("transform_cleaned_records")
        enriched = records("transform_enriched_records")
        chunked = records("transform_chunked_records")
        error = getattr(snapshot, "error", None)
        if error is None and isinstance(snapshot, dict):
            error = snapshot.get("error")

        if error is not None:
            error_data = (
                error.model_dump()
                if hasattr(error, "model_dump")
                else error
            )
            stage = "error"
            status = "failed"
        elif chunked:
            stage = "load"
        elif enriched:
            stage = "transform_chunk"
        elif cleaned:
            stage = "transform_enrich"
        elif extracted:
            stage = "transform_clean"
        else:
            stage = "extract"

        last_processed_record_id = getattr(
            snapshot, "last_processed_record_id", None
        )
        if last_processed_record_id is None and isinstance(snapshot, dict):
            last_processed_record_id = snapshot.get("last_processed_record_id")

        return {
            "stage": stage,
            "status": status,
            "extracted_count": len(extracted),
            "cleaned_count": len(cleaned),
            "enriched_count": len(enriched),
            "chunked_count": len(chunked),
            "last_processed_record_id": last_processed_record_id,
            "error": error_data if error is not None else None,
        }

    @staticmethod
    def _snapshot_has_error(snapshot: object) -> bool:
        if hasattr(snapshot, "error"):
            return getattr(snapshot, "error") is not None
        if isinstance(snapshot, dict):
            return snapshot.get("error") is not None
        return False
