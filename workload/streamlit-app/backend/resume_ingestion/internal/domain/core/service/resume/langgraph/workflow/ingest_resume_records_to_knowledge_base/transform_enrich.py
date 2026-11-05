from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .init import IngestResumeRecordsToKnowledgeBaseWorkflow

import json
import re
import time
import httpx
from collections.abc import Mapping

from openai import APIError
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.language_models.chat_models import BaseChatModel
from langgraph.runtime import Runtime
from pydantic import ValidationError
from langsmith import Client

from backend.resume_ingestion.internal.domain.model.workflow.parsed_resume import (
    ParsedResume,
)
from .schema import (
    IngestResumeRecordsToKnowledgeBaseWorkflowState,
    IngestResumeRecordsToKnowledgeBaseWorkflowContext,
)

langsmith_client = Client()

llm_parser_structured: BaseChatModel | None = None

# PARSE_PROMPT_TEMPLATE = ChatPromptTemplate.from_messages(
#     [
#         (
#             "system",
#             "Extract structured information from this resume.\n"
#             "Return ONLY one valid JSON object matching the schema exactly.\n"
#             "Do not use markdown fences. Do not add extra keys.\n"
#             "Use null, [], or {{}} when information is missing.",
#         ),
#         ("human", "Resume:\n{resume_text}"),
#     ]
# )
# RETRY_PARSE_PROMPT_TEMPLATE = ChatPromptTemplate.from_messages(
#     [
#         (
#             "system",
#             "Your previous response was invalid.\n"
#             "Validation error: {error}\n\n"
#             "Return ONLY corrected JSON matching the schema exactly.\n"
#             "Do not use markdown fences. Do not add extra keys.",
#         ),
#         ("human", "Resume:\n{resume_text}"),
#     ]
# )
RESUME_PARSER_PROMPT = langsmith_client.pull_prompt(
    "resume-parser",
)
RESUME_PARSER_RETRY_PROMPT = langsmith_client.pull_prompt(
    "resume-parser-retry",
)
MAX_PARSE_ATTEMPTS_PER_RECORD = 3
RETRY_BACKOFF_SECONDS = 2.0

def transform_enrich(self: IngestResumeRecordsToKnowledgeBaseWorkflow, state: IngestResumeRecordsToKnowledgeBaseWorkflowState, runtime: Runtime[IngestResumeRecordsToKnowledgeBaseWorkflowContext]) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
    """Transform extracted resume records into structured data using an LLM parser."""
    if state.transform_cleaned_records is None:
        raise ValueError("Cannot transform enrich before cleaning resume records")

    if state.parsing_mode:
        if runtime.context.llm.parser is None:
            raise RuntimeError("An LLM parser is required to parse resumes")
        global llm_parser_structured
        llm_parser_structured = (
            runtime.context.llm.parser.with_structured_output(
                ParsedResume,
                method="function_calling",
            )
        )

    records = state.transform_cleaned_records
    if not isinstance(records, list):
        raise ValueError(
            "Cleaned data must contain a list of record"
        )

    existing_records = state.transform_enriched_records or []
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

    parsed_records = []
    for record in unprocessed_records:
        resume_id = record["ID"]
        if state.parsing_mode:
            resume_text = record["Resume_str"]
            metadata = _parse_record(resume_text, resume_id)
        else:
            metadata = {}
        enriched_record = {
            **record,
            "metadata": metadata,
        }
        parsed_records.append(enriched_record)

    return {
        "transform_enriched_records": existing_records + parsed_records
    }

def _parse_record(resume_text: str, resume_id: str) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
    """Parse a single resume record using the LLM parser, with retries on failure."""
    prompt = RESUME_PARSER_PROMPT.invoke({"resume_text": resume_text})
    last_error = "there are no parsing errors yet"

    for attempt in range(1, MAX_PARSE_ATTEMPTS_PER_RECORD + 1):
        try:
            response = llm_parser_structured.invoke(prompt)
            return _coerce_parsed(response)
        except httpx.RequestError as error:
            last_error = f"LLM network error: {error}"
            if attempt < MAX_PARSE_ATTEMPTS_PER_RECORD:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
        except APIError as error:
            last_error = f"LLM provider error: {error}"
            if attempt < MAX_PARSE_ATTEMPTS_PER_RECORD:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
        except json.JSONDecodeError as error:
            last_error = f"invalid JSON: {error.msg} at position {error.pos}"
        except ValidationError as error:
            last_error = f"schema mismatch: {error}"
        except ValueError as error:
            last_error = str(error)
        prompt = RESUME_PARSER_RETRY_PROMPT.invoke(
            {"resume_text": resume_text, "error": last_error}
        )

    raise ValueError(
        f"LLM failed to produce valid resume JSON for ID {resume_id} "
        f"after {MAX_PARSE_ATTEMPTS_PER_RECORD} attempts: {last_error}"
    )

def _coerce_parsed(response: object) -> IngestResumeRecordsToKnowledgeBaseWorkflowState:
    if isinstance(response, ParsedResume):
        return response.model_dump()
    if isinstance(response, dict):
        return ParsedResume.model_validate(response).model_dump()
    if isinstance(response, str):
        return ParsedResume.model_validate(
            json.loads(_extract_json(response))
        ).model_dump()
    raise ValueError("LLM returned an unexpected response type")

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

def _clean_value(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()
