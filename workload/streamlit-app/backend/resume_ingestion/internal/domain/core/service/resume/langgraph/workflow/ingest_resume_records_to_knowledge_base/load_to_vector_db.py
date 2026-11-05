from __future__ import annotations
from typing import TYPE_CHECKING
from typing import Any
if TYPE_CHECKING:
    from .init import IngestResumeRecordsToKnowledgeBaseWorkflow

from .schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)

def load_to_vector_db(
    self: IngestResumeRecordsToKnowledgeBaseWorkflow,
    state: IngestResumeRecordsToKnowledgeBaseWorkflowState,
) -> dict[str, Any]:
    if self._vector_resume_repository is None:
        raise RuntimeError(
            "A resume vector database repository is required to load chunks"
        )
    if state.transform_chunked_records is None:
        raise ValueError("Cannot load before chunking resume records")

    self._vector_resume_repository.embed_and_store_records(
        state.transform_chunked_records
    )
    return _last_processed_record_update(state)

def _last_processed_record_update(
    state: IngestResumeRecordsToKnowledgeBaseWorkflowState,
) -> dict[str, str]:
    extracted_records = state.extracted_records or []
    if extracted_records:
        last_record_id = extracted_records[-1].get("ID")
        if last_record_id is not None:
            return {"last_processed_record_id": str(last_record_id)}
    return {}
