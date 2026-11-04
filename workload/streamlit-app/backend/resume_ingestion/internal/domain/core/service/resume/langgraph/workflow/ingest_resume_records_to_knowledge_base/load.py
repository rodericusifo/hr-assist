from __future__ import annotations
from typing import TYPE_CHECKING
from typing import Any
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.init import IngestResumeRecordsToKnowledgeBaseWorkflow

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)


class LoadMixin:
    async def load_to_vector_db(
        self: IngestResumeRecordsToKnowledgeBaseWorkflow,
        state: IngestResumeRecordsToKnowledgeBaseWorkflowState,
    ) -> dict[str, Any]:
        if self._vector_database_resume_repository is None:
            raise RuntimeError(
                "A resume vector database repository is required to load chunks"
            )
        if state.transform_chunked_records is None:
            raise ValueError("Cannot load before chunking resume records")

        await self._vector_database_resume_repository.store_records(
            state.transform_chunked_records
        )
        return LoadMixin._last_processed_record_update(state)

    async def load_to_sql_db(
        self: IngestResumeRecordsToKnowledgeBaseWorkflow,
        state: IngestResumeRecordsToKnowledgeBaseWorkflowState,
    ) -> dict[str, Any]:
        if self._sql_database_resume_repository is None:
            raise RuntimeError(
                "A resume SQL database repository is required to load records"
            )
        if state.transform_enriched_records is None:
            raise ValueError("Cannot load before enriching resume records")

        await self._sql_database_resume_repository.store_records(
            state.transform_enriched_records
        )
        return {}

    @staticmethod
    def _last_processed_record_update(
        state: IngestResumeRecordsToKnowledgeBaseWorkflowState,
    ) -> dict[str, str]:
        extracted_records = state.extracted_records or []
        if extracted_records:
            last_record_id = extracted_records[-1].get("ID")
            if last_record_id is not None:
                return {"last_processed_record_id": str(last_record_id)}
        return {}
