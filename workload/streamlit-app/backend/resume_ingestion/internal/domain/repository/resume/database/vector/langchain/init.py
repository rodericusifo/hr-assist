from langchain_core.vectorstores import VectorStore

from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.store_records import (
    StoreRecordsMixin,
)

class LangchainVectorResumeRepository(StoreRecordsMixin):
    def __init__(
        self,
        vector_store: VectorStore,
    ) -> None:
        self._vector_store = vector_store
