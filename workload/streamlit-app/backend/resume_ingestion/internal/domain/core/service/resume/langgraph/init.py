from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.schema import (
    LLM
)
from backend.resume_ingestion.internal.pkg.interface.resume import (
    ISQLDatabaseResumeRepository,
    IVectorDatabaseResumeRepository,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.ingest_resume_records_to_knowledge_base import IngestResumeRecordsToKnowledgeBaseMixin

class LanggraphResumeService(IngestResumeRecordsToKnowledgeBaseMixin):
    def __init__(
        self,
        llm: LLM | None = None,
        vector_database_resume_repository: IVectorDatabaseResumeRepository | None = None,
        sql_database_resume_repository: ISQLDatabaseResumeRepository | None = None,
    ):
        self._llm = llm
        self._vector_database_resume_repository = vector_database_resume_repository
        self._sql_database_resume_repository = sql_database_resume_repository
