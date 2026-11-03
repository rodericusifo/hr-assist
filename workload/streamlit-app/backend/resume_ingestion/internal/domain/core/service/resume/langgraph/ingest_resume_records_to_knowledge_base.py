
from __future__ import annotations
import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, AsyncGenerator
from uuid import uuid4
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.init import (
        LanggraphResumeService,
    )

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.init import (
    IngestResumeRecordsToKnowledgeBaseWorkflow,
)
from backend.resume_ingestion.internal.pkg.config.config_database_sql import (
    database_sql_connection_string,
    database_sql_pool,
)
from backend.resume_ingestion.internal.pkg.constant.dialect import Dialect

class IngestResumeRecordsToKnowledgeBaseMixin:
    async def ingest_resume_records_to_knowledge_base(
        self: LanggraphResumeService,
        provider: str,
        file_path: str,
        max_records_to_extract: int = 10,
    ) -> AsyncGenerator[dict, None]:
        if max_records_to_extract < 1:
            raise ValueError("The number of records to ingest must be at least 1")
        provider = provider.strip().lower()
        state = IngestResumeRecordsToKnowledgeBaseWorkflowState(
            provider=provider,
            file_path=file_path,
        )
        context = IngestResumeRecordsToKnowledgeBaseWorkflowContext(
            llm=self._llm,
            max_records_to_extract=max_records_to_extract,
            chunk_size=1200,
            chunk_overlap=180
        )

        connection_string = database_sql_connection_string(Dialect.POSTGRES)
        source_key = (
            str(Path(file_path).resolve())
            if provider == "file"
            else file_path.strip()
        )
        lifecycle_key = (provider, source_key)
        lock_key = f"{lifecycle_key[0]}:{lifecycle_key[1]}"
        async with database_sql_pool(Dialect.POSTGRES) as pool:
            async with pool.connection() as lock_connection:
                await lock_connection.execute(
                    "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
                    (lock_key,),
                )
                try:
                    thread_id, source_id = await self._get_or_create_run(
                        pool,
                        lifecycle_key,
                    )
                    state.starting_record_id = await self._get_last_processed_record_id(
                        pool,
                        source_id,
                    )
                    await self._set_run_status(
                        pool,
                        thread_id,
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
                            stream = await workflow.astream_events(
                                state,
                                config={"configurable": {"thread_id": thread_id}},
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
                                await self._set_run_progress(
                                    pool,
                                    thread_id,
                                    projection,
                                )
                                yield {
                                    "event": "progress",
                                    "data": projection,
                                }
                        except (GeneratorExit, asyncio.CancelledError):
                            await self._set_run_status(
                                pool,
                                thread_id,
                                "failed",
                                "Ingestion was interrupted before completion",
                            )
                            raise
                        except Exception as error:
                            await self._set_run_status(
                                pool,
                                thread_id,
                                "failed",
                                str(error),
                            )
                            raise
                        if workflow_failed:
                            await self._set_run_status(
                                pool,
                                thread_id,
                                "failed",
                                "Workflow completed with an error",
                            )
                        else:
                            await self._set_run_progress(
                                pool,
                                thread_id,
                                {"stage": "complete"},
                            )
                            await self._set_run_status(
                                pool,
                                thread_id,
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
                    await self._set_run_status(
                        pool,
                        thread_id,
                        "failed",
                        str(error),
                    )
                    raise
                finally:
                    if not lock_connection.closed:
                        await lock_connection.execute(
                            "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                            (lock_key,),
                        )

    @staticmethod
    async def _get_or_create_run(
        pool: AsyncConnectionPool,
        lifecycle_key: tuple[str, str],
    ) -> tuple[str, int]:
        provider, file_path = lifecycle_key
        initial_thread_id = f"thread-{uuid4()}"
        async with pool.connection() as connection:
            await connection.execute(
                """
                INSERT INTO ingestion_sources (xid, provider, file_path)
                VALUES (%s, %s, %s)
                ON CONFLICT (provider, file_path)
                DO NOTHING
                """,
                (uuid4(), provider, file_path),
            )
            cursor = await connection.execute(
                """
                SELECT id
                FROM ingestion_sources
                WHERE provider = %s AND file_path = %s
                """,
                (provider, file_path),
            )
            source_row = await cursor.fetchone()
            if source_row is None:
                raise RuntimeError("Failed to load resume ingestion run state")
            source_id = int(source_row[0])
            cursor = await connection.execute(
                """
                SELECT
                    thread_id,
                    status,
                    COALESCE(last_processed_record_id, starting_record_id)
                        AS resume_record_id
                FROM ingestion_runs
                WHERE ingestion_source_id = %s AND is_current
                """,
                (source_id,),
            )
            row = await cursor.fetchone()
            if row is not None and row[2] is None:
                cursor = await connection.execute(
                    """
                    SELECT COALESCE(
                        last_processed_record_id,
                        starting_record_id
                    )
                    FROM ingestion_runs
                    WHERE ingestion_source_id = %s
                      AND is_current = FALSE
                      AND (
                          last_processed_record_id IS NOT NULL
                          OR starting_record_id IS NOT NULL
                      )
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 1
                    """,
                    (source_id,),
                )
                previous_cursor = await cursor.fetchone()
                if previous_cursor is not None:
                    row = (row[0], row[1], previous_cursor[0])
            if row is None:
                cursor = await connection.execute(
                    """
                    INSERT INTO ingestion_runs
                        (
                            ingestion_source_id,
                            xid,
                            thread_id,
                            starting_record_id,
                            is_current
                        )
                    VALUES (%s, %s, %s, NULL, TRUE)
                    RETURNING thread_id
                    """,
                    (source_id, uuid4(), initial_thread_id),
                )
                row = await cursor.fetchone()
            elif row[1] == "failed":
                await connection.execute(
                    """
                    UPDATE ingestion_runs
                    SET status = 'running',
                        error = NULL,
                        starting_record_id = COALESCE(
                            starting_record_id,
                            %s
                        ),
                        progress_stage = NULL,
                        progress_value = NULL,
                        progress_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE ingestion_source_id = %s
                      AND is_current
                    """,
                    (row[2], source_id),
                )
            elif row[1] == "succeeded":
                await connection.execute(
                    """
                    UPDATE ingestion_runs
                    SET is_current = FALSE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE ingestion_source_id = %s
                      AND is_current
                    """,
                    (source_id,),
                )
                cursor = await connection.execute(
                    """
                    INSERT INTO ingestion_runs
                        (
                            ingestion_source_id,
                            xid,
                            thread_id,
                            starting_record_id,
                            last_processed_record_id,
                            status,
                            is_current
                        )
                    VALUES (%s, %s, %s, %s, %s, 'running', TRUE)
                    RETURNING thread_id
                    """,
                    (source_id, uuid4(), initial_thread_id, row[2], None),
                )
                row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("Failed to create ingestion run")
            await connection.commit()
            return str(row[0]), source_id

    @staticmethod
    async def _set_run_status(
        pool: AsyncConnectionPool,
        thread_id: str,
        status: str,
        error: str | None,
    ) -> None:
        async with pool.connection() as connection:
            await connection.execute(
                """
                UPDATE ingestion_runs
                SET status = %s,
                    error = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE thread_id = %s
                """,
                (status, error, thread_id),
            )
            await connection.commit()

    @staticmethod
    async def _set_run_progress(
        pool: AsyncConnectionPool,
        thread_id: str,
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
        async with pool.connection() as connection:
            await connection.execute(
                """
                UPDATE ingestion_runs
                SET progress_stage = %s,
                    progress_value = %s,
                    progress_message = %s,
                    last_processed_record_id = COALESCE(
                        %s,
                        last_processed_record_id
                    ),
                    updated_at = CURRENT_TIMESTAMP
                WHERE thread_id = %s
                """,
                (
                    stage,
                    value,
                    message,
                    projection.get("last_processed_record_id"),
                    thread_id,
                ),
            )
            await connection.commit()

    @staticmethod
    async def _get_last_processed_record_id(
        pool: AsyncConnectionPool,
        source_id: int,
    ) -> str | None:
        async with pool.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT starting_record_id
                FROM ingestion_runs
                WHERE ingestion_source_id = %s AND is_current
                """,
                (source_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("Failed to load resume ingestion cursor")
            await connection.commit()
            return str(row[0]) if row[0] is not None else None

    @staticmethod
    def _last_processed_record_id(snapshot: object) -> str | None:
        records = getattr(snapshot, "extracted_records", None)
        if records is None and isinstance(snapshot, dict):
            records = snapshot.get("extracted_records")
        if not isinstance(records, list) or not records:
            return None
        last_record = records[-1]
        if not isinstance(last_record, dict) or last_record.get("ID") is None:
            return None
        return str(last_record["ID"])

    @staticmethod
    def _chunk_records(snapshot: object) -> list[dict]:
        records = getattr(snapshot, "transform_chunked_records", None)
        if records is None and isinstance(snapshot, dict):
            records = snapshot.get("transform_chunked_records")
        return records if isinstance(records, list) else []

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
