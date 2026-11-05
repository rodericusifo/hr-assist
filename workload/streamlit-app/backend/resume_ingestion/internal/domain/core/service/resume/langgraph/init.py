from backend.resume_ingestion.internal.pkg.interface.resume import (
    ISQLResumeRepository,
    IVectorResumeRepository
)
from backend.resume_ingestion.internal.pkg.interface.ingestion_run import (
    ISQLIngestionRunRepository
)
from backend.resume_ingestion.internal.pkg.interface.ingestion_source import (
    ISQLIngestionSourceRepository
)
from .ingest_resume_records_to_knowledge_base import ingest_resume_records_to_knowledge_base
from .schema import (
    LLM
)

class LanggraphResumeService:
    ingest_resume_records_to_knowledge_base = ingest_resume_records_to_knowledge_base

    def __init__(
        self,
        llm: LLM | None = None,
        vector_resume_repository: IVectorResumeRepository | None = None,
        sql_resume_repository: ISQLResumeRepository | None = None,
        sql_ingestion_run_repository: ISQLIngestionRunRepository | None = None,
        sql_ingestion_source_repository: ISQLIngestionSourceRepository | None = None,
    ):
        self._llm = llm
        self._vector_resume_repository = vector_resume_repository
        self._sql_resume_repository = sql_resume_repository
        self._sql_ingestion_run_repository = sql_ingestion_run_repository
        self._sql_ingestion_source_repository = sql_ingestion_source_repository