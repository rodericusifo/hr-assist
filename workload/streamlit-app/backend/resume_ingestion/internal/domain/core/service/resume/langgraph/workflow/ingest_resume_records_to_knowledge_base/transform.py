from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.init import IngestResumeRecordsToKnowledgeBaseWorkflow

import hashlib
import json
import re
import time
import httpx
from collections.abc import Mapping
from datetime import datetime, timezone

import pandas as pd
from openai import APIError
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langgraph.runtime import Runtime
from pydantic import ValidationError

from backend.resume_ingestion.internal.domain.model.workflow.parsed_resume import (
    ParsedResume,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.workflow.ingest_resume_records_to_knowledge_base.schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
)

class TransformMixin:
    llm_parser_structured: BaseChatModel | None = None

    PARSE_PROMPT_TEMPLATE = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "Extract structured information from this resume.\n"
                "Return ONLY one valid JSON object matching the schema exactly.\n"
                "Do not use markdown fences. Do not add extra keys.\n"
                "Use null, [], or {{}} when information is missing.",
            ),
            ("human", "Resume:\n{resume_text}"),
        ]
    )
    RETRY_PARSE_PROMPT_TEMPLATE = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "Your previous response was invalid.\n"
                "Validation error: {error}\n\n"
                "Return ONLY corrected JSON matching the schema exactly.\n"
                "Do not use markdown fences. Do not add extra keys.",
            ),
            ("human", "Resume:\n{resume_text}"),
        ]
    )
    MAX_PARSE_ATTEMPTS_PER_RECORD = 3
    RETRY_BACKOFF_SECONDS = 2.0

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
            self._clean_value(record.get("ID"))
            for record in existing_records
            if isinstance(record, Mapping) and self._clean_value(record.get("ID"))
        }
        unprocessed_records = [
            record
            for record in records
            if isinstance(record, Mapping)
            and self._clean_value(record.get("ID")) not in processed_ids
        ]

        frame = pd.DataFrame(
            [dict(record) for record in unprocessed_records]
        )
        frame = frame.map(self._clean_value)

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

    def transform_enrich(self: IngestResumeRecordsToKnowledgeBaseWorkflow, state: IngestResumeRecordsToKnowledgeBaseWorkflowState, runtime: Runtime[IngestResumeRecordsToKnowledgeBaseWorkflowContext]) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
        """Transform extracted resume records into structured data using an LLM parser."""
        if runtime.context.llm.parser is None:
            raise RuntimeError("An LLM parser is required to parse resumes")
        TransformMixin.llm_parser_structured = (
            runtime.context.llm.parser.with_structured_output(
                ParsedResume,
                method="function_calling",
            )
        )

        if state.transform_cleaned_records is None:
            raise ValueError("Cannot transform enrich before cleaning resume records")

        records = state.transform_cleaned_records
        if not isinstance(records, list):
            raise ValueError(
                "Cleaned data must contain a list of record"
            )

        existing_records = state.transform_enriched_records or []
        processed_ids = {
            self._clean_value(record.get("ID"))
            for record in existing_records
            if isinstance(record, Mapping) and self._clean_value(record.get("ID"))
        }
        unprocessed_records = [
            record
            for record in records
            if isinstance(record, Mapping)
            and self._clean_value(record.get("ID")) not in processed_ids
        ]

        parsed_records = []
        for record in unprocessed_records:
            resume_id = record["ID"]
            resume_text = record["Resume_str"]
            enriched_record = {
                **record,
                "metadata": self._parse_record(resume_text, resume_id),
            }
            parsed_records.append(enriched_record)

        return {
            "transform_enriched_records": existing_records + parsed_records
        }

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
            self._resume_id_from_chunk_id(chunk.get("ID"))
            for chunk in existing_chunks
            if isinstance(chunk, Mapping)
            and self._resume_id_from_chunk_id(chunk.get("ID"))
        }
        new_records = [
            record
            for record in records
            if isinstance(record, Mapping)
            and self._clean_value(record.get("ID")) not in processed_resume_ids
        ]

        processed_at = datetime.now(timezone.utc).isoformat()
        new_chunks = []
        for record in new_records:
            resume_id = self._clean_value(record.get("ID"))
            resume_text = self._clean_value(record.get("Resume_str"))
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

    @staticmethod
    def _resume_id_from_chunk_id(chunk_id: object) -> str:
        normalized_id = str(chunk_id or "").strip()
        match = re.match(r"^(.*)_\d{4}$", normalized_id)
        return match.group(1) if match else ""

    @staticmethod
    def _parse_record(resume_text: str, resume_id: str) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
        """Parse a single resume record using the LLM parser, with retries on failure."""
        prompt = TransformMixin.PARSE_PROMPT_TEMPLATE.invoke({"resume_text": resume_text})
        last_error = "there are no parsing errors yet"

        for attempt in range(1, TransformMixin.MAX_PARSE_ATTEMPTS_PER_RECORD + 1):
            try:
                response = TransformMixin.llm_parser_structured.invoke(prompt)
                return TransformMixin._coerce_parsed(response)
            except httpx.RequestError as error:
                last_error = f"LLM network error: {error}"
                if attempt < TransformMixin.MAX_PARSE_ATTEMPTS_PER_RECORD:
                    time.sleep(TransformMixin.RETRY_BACKOFF_SECONDS * attempt)
            except APIError as error:
                last_error = f"LLM provider error: {error}"
                if attempt < TransformMixin.MAX_PARSE_ATTEMPTS_PER_RECORD:
                    time.sleep(TransformMixin.RETRY_BACKOFF_SECONDS * attempt)
            except json.JSONDecodeError as error:
                last_error = f"invalid JSON: {error.msg} at position {error.pos}"
            except ValidationError as error:
                last_error = f"schema mismatch: {error}"
            except ValueError as error:
                last_error = str(error)
            prompt = TransformMixin.RETRY_PARSE_PROMPT_TEMPLATE.invoke(
                {"resume_text": resume_text, "error": last_error}
            )

        raise ValueError(
            f"LLM failed to produce valid resume JSON for ID {resume_id} "
            f"after {TransformMixin.MAX_PARSE_ATTEMPTS_PER_RECORD} attempts: {last_error}"
        )

    @staticmethod
    def _coerce_parsed(response: object) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
        if isinstance(response, ParsedResume):
            return response.model_dump()
        if isinstance(response, dict):
            return ParsedResume.model_validate(response).model_dump()
        if isinstance(response, str):
            return ParsedResume.model_validate(
                json.loads(TransformMixin._extract_json(response))
            ).model_dump()
        raise ValueError("LLM returned an unexpected response type")

    @staticmethod
    def _extract_json(content: str) -> str:
        fenced_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", content, re.DOTALL)
        if fenced_match:
            return fenced_match.group(1)

        start = content.find("{")
        if start == -1:
            raise json.JSONDecodeError("No JSON object found", content, 0)

        decoder = json.JSONDecoder()
        _, end = decoder.raw_decode(content[start:])
        return content[start : start + end]

    @staticmethod
    def _clean_value(value: object) -> str:
        if value is None:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()
