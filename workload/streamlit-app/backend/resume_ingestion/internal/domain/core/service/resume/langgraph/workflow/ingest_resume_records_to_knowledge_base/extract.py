from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .init import IngestResumeRecordsToKnowledgeBaseWorkflow

import pandas as pd
from langgraph.runtime import Runtime

from .schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
)

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
            if last_processed_record_id is not None:
                try:
                    starting_value = float(last_processed_record_id)
                    records = records[
                        pd.to_numeric(records["ID"], errors="coerce")
                        > starting_value
                    ]
                except ValueError:
                    records = records[
                        records["ID"].astype(str) > last_processed_record_id
                    ]
            extracted_records = (
                records
                .head(state.max_records_to_extract)
                .to_dict(orient="records")
            )
        case _:
            raise ValueError(f"Unsupported provider: {provider}")

    return {
        "extracted_records": extracted_records
    }
