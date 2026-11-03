from langchain_qdrant import QdrantVectorStore

from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.qdrant.embed_and_store_records import (
    EmbedAndStoreRecordsMixin,
)

class QdrantLangchainVectorDatabaseResumeRepository(EmbedAndStoreRecordsMixin):
    def __init__(
        self,
        vector_store: QdrantVectorStore,
    ) -> None:
        self._vector_store = vector_store
