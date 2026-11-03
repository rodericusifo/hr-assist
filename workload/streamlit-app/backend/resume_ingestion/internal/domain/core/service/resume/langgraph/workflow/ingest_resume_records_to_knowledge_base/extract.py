from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.init import IngestResumeRecordsToKnowledgeBaseWorkflow

import pandas as pd
from langgraph.runtime import Runtime

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
)

class ExtractMixin:
    def extract(self: IngestResumeRecordsToKnowledgeBaseWorkflow, state: IngestResumeRecordsToKnowledgeBaseWorkflowState, runtime: Runtime[IngestResumeRecordsToKnowledgeBaseWorkflowContext]) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
        """Extract resume records from the configured provider."""
        provider = state.provider
        file_path = state.file_path
        last_processed_record_id = state.last_processed_record_id

        if not file_path:
            raise ValueError("Resume file path is required")

        match provider:
            case "file" | "url":
                records = pd.read_csv(file_path, encoding="utf-8-sig").sort_values(
                    "ID"
                )
                if last_processed_record_id:
                    extracted_records = (
                        records
                        .query(f"ID > {last_processed_record_id}")
                        .head(runtime.context.max_records_to_extract)
                        .to_dict(orient="records")
                    )
                else:
                    extracted_records = (
                        records
                        .head(runtime.context.max_records_to_extract)
                        .to_dict(orient="records")
                    )
            case _:
                raise ValueError(f"Unsupported provider: {provider}")

        return {
            "extracted_records": extracted_records
        }