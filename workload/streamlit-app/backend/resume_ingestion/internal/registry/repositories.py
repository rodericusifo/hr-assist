from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.registry.init import DomainRegistry

import streamlit as st
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient

from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.qdrant.schema import (
    LLM,
)
from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.qdrant.init import (
    QdrantLangchainVectorDatabaseResumeRepository,
)


class Repositories:
    def registerRepositories(self: DomainRegistry) -> None:
        llm = LLM(
            embedder=OpenAIEmbeddings(
                model="openai/text-embedding-3-small",
                api_key=st.secrets["OPENROUTER_API_KEY"],
                base_url=st.secrets["OPENROUTER_API_BASE"],
            ),
        )
        qdrant_langchain_vector_database_resume_repository = QdrantLangchainVectorDatabaseResumeRepository(
            llm=llm,
            vector_store=QdrantVectorStore(
                client=QdrantClient(url=st.secrets["QDRANT_URL"]),
                collection_name="resumes",
            ),
        )
        self._components["repository.resume.database.vector.langchain.qdrant"] = qdrant_langchain_vector_database_resume_repository
