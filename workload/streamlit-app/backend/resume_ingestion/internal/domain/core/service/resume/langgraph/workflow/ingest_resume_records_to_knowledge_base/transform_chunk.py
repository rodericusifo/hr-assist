from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .init import IngestResumeRecordsToKnowledgeBaseWorkflow

import hashlib
import re
from collections.abc import Mapping
from datetime import datetime, timezone

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langgraph.runtime import Runtime

from .schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
)

def transform_chunk(self: IngestResumeRecordsToKnowledgeBaseWorkflow, state: IngestResumeRecordsToKnowledgeBaseWorkflowState, runtime: Runtime[IngestResumeRecordsToKnowledgeBaseWorkflowContext]) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
    """Split enriched resume text into overlapping chunks with searchable metadata."""
    if state.transform_enriched_records is None:
        raise ValueError("Cannot chunk before enriching resume records")

    records = state.transform_enriched_records
    if not isinstance(records, list):
        raise ValueError("Enriched data must contain a list of record")
    if runtime.context.chunk_size is None or runtime.context.chunk_overlap is None:
        raise ValueError("Chunk size and overlap are required to chunk resume records")
    if runtime.context.chunk_size <= runtime.context.chunk_overlap:
        raise ValueError("Chunk size must be greater than chunk overlap")

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=runtime.context.chunk_size,
        chunk_overlap=runtime.context.chunk_overlap,
    )
    existing_chunks = state.transform_chunked_records or []
    processed_resume_ids = {
        _resume_id_from_chunk_id(chunk.get("ID"))
        for chunk in existing_chunks
        if isinstance(chunk, Mapping)
        and _resume_id_from_chunk_id(chunk.get("ID"))
    }
    new_records = [
        record
        for record in records
        if isinstance(record, Mapping)
        and _clean_value(record.get("ID")) not in processed_resume_ids
    ]

    processed_at = datetime.now(timezone.utc).isoformat()
    new_chunks = []
    for record in new_records:
        resume_id = _clean_value(record.get("ID"))
        resume_text = _clean_value(record.get("Resume_str"))
        parsed_metadata = record.get("metadata")
        if not isinstance(parsed_metadata, Mapping):
            parsed_metadata = {}

        base_metadata = {
            "name": parsed_metadata.get("name"),
            "contact": parsed_metadata.get("contact", {}),
            "has_summary": bool(parsed_metadata.get("summary")),
            "companies_worked": [
                experience.get("company")
                for experience in parsed_metadata.get("experience", [])
                if isinstance(experience, Mapping) and experience.get("company")
            ],
            "degrees_held": [
                education.get("degree")
                for education in parsed_metadata.get("education", [])
                if isinstance(education, Mapping) and education.get("degree")
            ],
            "skills": parsed_metadata.get("skills", []),
            "category": record.get("Category"),
        }

        chunks = splitter.split_text(resume_text)
        for chunk_index, chunk in enumerate(chunks):
            new_chunks.append(
                {
                    "ID": f"{resume_id}_{chunk_index:04d}",
                    "chunk": chunk,
                    "metadata": {
                        **base_metadata,
                        "resume_id": resume_id,
                        "source": "Resume_str",
                        "chunk_index": chunk_index,
                        "chunk_hash": hashlib.sha256(
                            chunk.encode("utf-8")
                        ).hexdigest(),
                        "is_final_chunk": chunk_index == len(chunks) - 1,
                        "processed_at": processed_at,
                    },
                }
            )

    return {"transform_chunked_records": existing_chunks + new_chunks}

def _resume_id_from_chunk_id(chunk_id: object) -> str:
    normalized_id = str(chunk_id or "").strip()
    match = re.match(r"^(.*)_\d{4}$", normalized_id)
    return match.group(1) if match else ""

def _clean_value(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()
