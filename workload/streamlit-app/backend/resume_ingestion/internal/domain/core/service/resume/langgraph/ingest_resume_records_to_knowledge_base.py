
from __future__ import annotations
from pathlib import Path
from typing import TYPE_CHECKING, AsyncGenerator
from uuid import uuid4
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.init import (
        LanggraphResumeService,
    )

import streamlit as st
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg import AsyncConnection

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.init import (
    IngestResumeRecordsToKnowledgeBaseWorkflow,
)

class IngestResumeRecordsToKnowledgeBaseMixin:
    async def ingest_resume_records_to_knowledge_base(self: LanggraphResumeService, provider: str, file_path: str) -> AsyncGenerator[dict, None]:
        state = IngestResumeRecordsToKnowledgeBaseWorkflowState(
            provider=provider,
            file_path=file_path,
        )
        context = IngestResumeRecordsToKnowledgeBaseWorkflowContext(
            llm=self._llm,
            max_records_to_extract=10,
            chunk_size=1200,
            chunk_overlap=180
        )

        connection_string = self._database_connection_string()
        source_key = (
            str(Path(file_path).resolve())
            if provider == "file"
            else file_path.strip()
        )
        lifecycle_key = (provider, source_key)
        lock_connection = await AsyncConnection.connect(connection_string)
        lock_key = f"{lifecycle_key[0]}:{lifecycle_key[1]}"
        try:
            await lock_connection.execute(
                "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
                (lock_key,),
            )
            thread_id, source_id = await self._get_or_create_thread_id(
                connection_string,
                lifecycle_key,
            )
            state.last_processed_record_id = await self._get_last_processed_record_id(
                connection_string,
                source_id,
            )
            await self._set_thread_status(
                connection_string,
                thread_id,
                "running",
                None,
            )
            async with AsyncPostgresSaver.from_conn_string(connection_string) as checkpointer:
                workflow = IngestResumeRecordsToKnowledgeBaseWorkflow(
                    self._vector_database_resume_repository
                ).compile(
                    checkpointer=checkpointer
                )
                try:
                    stream = await workflow.astream_events(
                        state,
                        config={"configurable": {"thread_id": thread_id}},
                        context=context,
                        version="v3",
                    )

                    workflow_failed = False
                    last_snapshot = None
                    async for snapshot in stream.values:
                        workflow_failed = workflow_failed or self._snapshot_has_error(snapshot)
                        last_snapshot = snapshot
                        yield {
                            "event": "values",
                            "data": snapshot.dict(),
                        }
                except Exception as error:
                    await self._set_thread_status(
                        connection_string,
                        thread_id,
                        "failed",
                        str(error),
                    )
                    raise
                if workflow_failed:
                    await self._set_thread_status(
                        connection_string,
                        thread_id,
                        "failed",
                        "Workflow completed with an error",
                    )
                else:
                    await self._rotate_thread_id(
                        connection_string,
                        thread_id,
                        source_id,
                        self._last_processed_record_id(last_snapshot),
                    )
                    await self._set_thread_status(
                        connection_string,
                        thread_id,
                        "succeeded",
                        None,
                    )
        finally:
            await lock_connection.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                (lock_key,),
            )
            await lock_connection.close()

    @staticmethod
    async def _get_or_create_thread_id(
        connection_string: str,
        lifecycle_key: tuple[str, str],
    ) -> tuple[str, int]:
        provider, file_path = lifecycle_key
        initial_thread_id = str(uuid4())
        async with await AsyncConnection.connect(connection_string) as connection:
            await connection.execute(
                """
                INSERT INTO ingestion_sources (provider, file_path)
                VALUES (%s, %s)
                ON CONFLICT (provider, file_path)
                DO NOTHING
                """,
                (provider, file_path),
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
                raise RuntimeError("Failed to load resume ingestion thread state")
            source_id = int(source_row[0])
            cursor = await connection.execute(
                """
                SELECT id
                FROM ingestion_threads
                WHERE ingestion_source_id = %s AND is_current
                """,
                (source_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                cursor = await connection.execute(
                    """
                    INSERT INTO ingestion_threads
                        (ingestion_source_id, id, is_current)
                    VALUES (%s, %s, TRUE)
                    RETURNING id
                    """,
                    (source_id, initial_thread_id),
                )
                row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("Failed to create ingestion thread")
            await connection.commit()
            return str(row[0]), source_id

    @staticmethod
    async def _set_thread_status(
        connection_string: str,
        thread_id: str,
        status: str,
        error: str | None,
    ) -> None:
        async with await AsyncConnection.connect(connection_string) as connection:
            await connection.execute(
                """
                UPDATE ingestion_threads
                SET status = %s,
                    error = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (status, error, thread_id),
            )
            await connection.commit()

    @staticmethod
    async def _get_last_processed_record_id(
        connection_string: str,
        source_id: int,
    ) -> str | None:
        async with await AsyncConnection.connect(connection_string) as connection:
            cursor = await connection.execute(
                """
                SELECT last_processed_record_id
                FROM ingestion_threads
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
    async def _rotate_thread_id(
        connection_string: str,
        current_thread_id: str,
        source_id: int,
        last_processed_record_id: str | None,
    ) -> None:
        next_thread_id = str(uuid4())
        async with await AsyncConnection.connect(connection_string) as connection:
            cursor = await connection.execute(
                """
                UPDATE ingestion_threads
                SET is_current = FALSE,
                    status = 'succeeded',
                    last_processed_record_id = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                  AND ingestion_source_id = %s
                  AND is_current
                """,
                (
                    last_processed_record_id,
                    current_thread_id,
                    source_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    "Resume ingestion thread state changed before rotation"
                )
            await connection.execute(
                """
                INSERT INTO ingestion_threads
                    (
                        ingestion_source_id,
                        id,
                        is_current,
                        last_processed_record_id,
                        status
                    )
                VALUES (%s, %s, TRUE, %s, 'succeeded')
                """,
                (source_id, next_thread_id, last_processed_record_id),
            )
            await connection.commit()

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

    @staticmethod
    def _snapshot_has_error(snapshot: object) -> bool:
        if hasattr(snapshot, "error"):
            return getattr(snapshot, "error") is not None
        if isinstance(snapshot, dict):
            return snapshot.get("error") is not None
        return False

    @staticmethod
    def _database_connection_string() -> str:
        dsn = st.secrets.get("PRIMARY_DATABASE_SQL_DSN")
        if not dsn:
            raise RuntimeError(
                "Configure PRIMARY_DATABASE_SQL_DSN in .streamlit/secrets.toml"
            )
        return str(dsn)