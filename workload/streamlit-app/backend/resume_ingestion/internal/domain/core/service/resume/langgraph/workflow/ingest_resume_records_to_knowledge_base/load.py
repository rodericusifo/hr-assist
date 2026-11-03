from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.init import IngestResumeRecordsToKnowledgeBaseWorkflow

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)


class LoadMixin:
    def load(
        self: IngestResumeRecordsToKnowledgeBaseWorkflow,
        state: IngestResumeRecordsToKnowledgeBaseWorkflowState,
    ) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
        if self._vector_database_resume_repository is None:
            raise RuntimeError(
                "A resume vector database repository is required to load chunks"
            )
        if state.transform_chunked_records is None:
            raise ValueError("Cannot load before chunking resume records")

        self._vector_database_resume_repository.embed_and_store_records(
            state.transform_chunked_records
        )
        extracted_records = state.extracted_records or []
        if extracted_records:
            last_record_id = extracted_records[-1].get("ID")
            if last_record_id is not None:
                state.last_processed_record_id = str(last_record_id)
        return state
