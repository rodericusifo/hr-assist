from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.schema import (
    LLM
)
from backend.resume_ingestion.internal.pkg.interface.resume import (
    IResumeRepository,
)
from backend.resume_ingestion.internal.pkg.interface.ingestion_run import (
    IIngestionRunRepository
)
from backend.resume_ingestion.internal.pkg.interface.ingestion_source import (
    IIngestionSourceRepository
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.ingest_resume_records_to_knowledge_base import IngestResumeRecordsToKnowledgeBaseMixin

class LanggraphResumeService(IngestResumeRecordsToKnowledgeBaseMixin):
    def __init__(
        self,
        llm: LLM | None = None,
        vector_database_resume_repository: IResumeRepository | None = None,
        sql_database_resume_repository: IResumeRepository | None = None,
        sql_database_ingestion_run_repository: IIngestionRunRepository | None = None,
        sql_database_ingestion_source_repository: IIngestionSourceRepository | None = None,
    ):
        self._llm = llm
        self._vector_database_resume_repository = vector_database_resume_repository
        self._sql_database_resume_repository = sql_database_resume_repository
        self._sql_database_ingestion_run_repository = sql_database_ingestion_run_repository
        self._sql_database_ingestion_source_repository = sql_database_ingestion_source_repository