from __future__ import annotations

from typing import Any

from langchain_core.documents import Document
from langchain_qdrant import QdrantVectorStore

from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.qdrant.schema import (
    LLM,
)

class QdrantLangchainVectorDatabaseResumeRepository:
    def __init__(
        self,
        llm: LLM,
        vector_store: QdrantVectorStore | None = None,
    ) -> None:
        self._llm = llm
        self._vector_store = vector_store
        self._vector_store.embeddings = self._llm.embedder


    def embed_and_store_records(self, records: list[dict[str, Any]]) -> None:
        documents = [
            Document(
                id=str(record["ID"]),
                page_content=str(record.get("chunk", "")),
                metadata=record.get("metadata", {}),
            )
            for record in records
            if record.get("ID") is not None
        ]
        if not documents:
            return
        self._vector_store.add_documents(
            documents,
            ids=[document.id for document in documents],
        )
