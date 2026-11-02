from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.schema import (
    LLM
)
from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.ingest_resume_records_to_knowledge_base import IngestResumeRecordsToKnowledgeBaseMixin

class LanggraphResumeService(IngestResumeRecordsToKnowledgeBaseMixin):
    def __init__(self, llm: LLM | None = None):
        self._llm = llm
