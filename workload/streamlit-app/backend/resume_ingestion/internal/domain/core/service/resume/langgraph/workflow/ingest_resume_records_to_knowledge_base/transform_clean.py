from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .init import IngestResumeRecordsToKnowledgeBaseWorkflow

import re
from collections.abc import Mapping
import pandas as pd

from .schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
)

def transform_clean(self: IngestResumeRecordsToKnowledgeBaseWorkflow, state: IngestResumeRecordsToKnowledgeBaseWorkflowState) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
    """Clean extracted resume records by removing duplicates and invalid entries."""
    if state.extracted_records is None:
        raise ValueError("Cannot transform clean before extracting resume records")

    records = state.extracted_records
    if not isinstance(records, list):
        raise ValueError(
            "Extracted data must contain a list of record"
        )

    existing_records = state.transform_cleaned_records or []
    processed_ids = {
        _clean_value(record.get("ID"))
        for record in existing_records
        if isinstance(record, Mapping) and _clean_value(record.get("ID"))
    }
    unprocessed_records = [
        record
        for record in records
        if isinstance(record, Mapping)
        and _clean_value(record.get("ID")) not in processed_ids
    ]

    frame = pd.DataFrame(
        [dict(record) for record in unprocessed_records]
    )
    frame = frame.map(_clean_value)

    required_fields = ("ID", "Resume_str", "Category")
    for field in required_fields:
        if field not in frame:
            frame[field] = ""

    frame = frame[frame[list(required_fields)].ne("").all(axis=1)]
    frame = frame.drop_duplicates(subset="ID", keep="first")

    new_records = frame.to_dict(orient="records")

    return {
        "transform_cleaned_records": existing_records + new_records
    }

def _clean_value(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()
