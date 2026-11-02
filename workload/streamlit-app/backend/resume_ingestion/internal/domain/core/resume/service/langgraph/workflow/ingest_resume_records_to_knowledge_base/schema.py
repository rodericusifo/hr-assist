from typing import Any

from pydantic import BaseModel

from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.schema import (
    LLM
)

class Error(BaseModel):
    node: str
    message: str

class IngestResumeRecordsToKnowledgeBaseWorkflowContext(BaseModel):
    llm: LLM | None = None
    max_records_to_extract: int | None = None
    chunk_size: int | None = None
    chunk_overlap: int | None = None

class IngestResumeRecordsToKnowledgeBaseWorkflowState(BaseModel):
    provider: str | None = None
    file_path: str | None = None
    extracted_records: list[dict[str, Any]] | None = None
    transform_cleaned_records: list[dict[str, Any]] | None = None
    transform_enriched_records: list[dict[str, Any]] | None = None
    transform_chunked_records: list[dict[str, Any]] | None = None
    error: Error | None = None