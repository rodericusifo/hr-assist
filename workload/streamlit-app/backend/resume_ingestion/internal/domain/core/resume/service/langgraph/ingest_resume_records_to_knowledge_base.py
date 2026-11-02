
from __future__ import annotations
from pathlib import Path
from typing import TYPE_CHECKING, AsyncGenerator
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.init import ResumeService

import streamlit as st
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)
from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.workflow.ingest_resume_records_to_knowledge_base.init import (
    IngestResumeRecordsToKnowledgeBaseWorkflow,
)

class IngestResumeRecordsToKnowledgeBaseMixin:
    async def ingest_resume_records_to_knowledge_base(self: ResumeService, provider: str, file_path: str) -> AsyncGenerator[dict, None]:
        state = IngestResumeRecordsToKnowledgeBaseWorkflowState(
            provider=provider,
            file_path=file_path
        )
        context = IngestResumeRecordsToKnowledgeBaseWorkflowContext(
            llm=self._llm,
            max_records_to_extract=10,
            chunk_size=1200,
            chunk_overlap=180
        )

        connection_string = self._checkpoint_connection_string()
        thread_id = f"{provider}:{Path(file_path).resolve()}"
        async with AsyncPostgresSaver.from_conn_string(connection_string) as checkpointer:
            await checkpointer.setup()
            workflow = IngestResumeRecordsToKnowledgeBaseWorkflow().compile(
                checkpointer=checkpointer
            )
            stream = await workflow.astream_events(
                state,
                config={"configurable": {"thread_id": thread_id}},
                context=context,
                version="v3",
            )

            async for snapshot in stream.values:
                yield {
                    "event": "values",
                    "data": snapshot.dict(),
                }

    @staticmethod
    def _checkpoint_connection_string() -> str:
        dsn = st.secrets.get("CHECKPOINT_DATABASE_DSN")
        if not dsn:
            raise RuntimeError(
                "Configure CHECKPOINT_DATABASE_DSN in .streamlit/secrets.toml"
            )
        return str(dsn)