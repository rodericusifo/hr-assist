from langchain_core.vectorstores import VectorStore

from .embed_and_store_records import embed_and_store_records

class LangchainVectorResumeRepository:
    embed_and_store_records = embed_and_store_records

    def __init__(
        self,
        vector_store: VectorStore,
    ) -> None:
        self._vector_store = vector_store
