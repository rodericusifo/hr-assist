from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.registry.init import DomainRegistry

import streamlit as st
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from sqlalchemy import create_engine

from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.qdrant.schema import (
    LLM,
)
from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.qdrant.init import (
    QdrantLangchainVectorDatabaseResumeRepository,
)
from backend.resume_ingestion.internal.domain.repository.resume.database.sql.sqlalchemy.init import (
    SQLAlchemyDatabaseResumeRepository,
)
from backend.resume_ingestion.internal.pkg.config.config_database_sql import (
    database_sql_connection_string,
)
from backend.resume_ingestion.internal.pkg.constant.dialect import Dialect

class RepositoriesMixin:
    def registerRepositories(self: DomainRegistry) -> None:
        llm = LLM(
            embedder=OpenAIEmbeddings(
                model="openai/text-embedding-3-small",
                api_key=st.secrets["OPENROUTER_API_KEY"],
                base_url=st.secrets["OPENROUTER_API_BASE"],
            ),
        )
        qdrant_langchain_vector_database_resume_repository = QdrantLangchainVectorDatabaseResumeRepository(
            vector_store=QdrantVectorStore(
                client=QdrantClient(
                    url=st.secrets["PRIMARY_QDRANT_DATABASE_VECTOR_URL"]
                ),
                collection_name="resumes",
                embedding=llm.embedder,
            ),
        )
        self._components["repository.resume.database.vector.langchain.qdrant"] = qdrant_langchain_vector_database_resume_repository
        self._components["repository.resume.database.sql.sqlalchemy"] = SQLAlchemyDatabaseResumeRepository(
            engine=create_engine(
                database_sql_connection_string(Dialect.POSTGRES).replace(
                    "postgresql://", "postgresql+psycopg://", 1
                )
            )
        )
