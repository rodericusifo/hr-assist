from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .init import DomainRegistry

import streamlit as st
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from sqlalchemy.ext.asyncio import create_async_engine

from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.schema import (
    LLM,
)
from backend.resume_ingestion.internal.domain.repository.resume.database.vector.langchain.init import (
    LangchainVectorResumeRepository,
)
from backend.resume_ingestion.internal.domain.repository.resume.database.sql.sqlalchemy.init import (
    SQLAlchemyResumeRepository,
)
from backend.resume_ingestion.internal.domain.repository.ingestion_run.database.sql.sqlalchemy.init import (
    SqlAlchemyIngestionRunRepository,
)
from backend.resume_ingestion.internal.domain.repository.ingestion_source.database.sql.sqlalchemy.init import (
    SqlAlchemyIngestionSourceRepository,
)
from backend.resume_ingestion.internal.pkg.config.config_database_sql import (
    database_sql_connection_string,
)
from backend.resume_ingestion.internal.pkg.constant.dialect import Dialect

def registerRepositories(self: DomainRegistry) -> None:
    llm = LLM(
        embedder=OpenAIEmbeddings(
            model="openai/text-embedding-3-small",
            api_key=st.secrets["OPENROUTER_API_KEY"],
            base_url=st.secrets["OPENROUTER_API_BASE"],
        ),
    )
    self._components["repository.resume.vector"] = LangchainVectorResumeRepository(
        vector_store=QdrantVectorStore(
            client=QdrantClient(
                url=st.secrets["PRIMARY_QDRANT_DATABASE_VECTOR_URL"],
                api_key=st.secrets["PRIMARY_QDRANT_DATABASE_SQL_API_KEY"],
                prefer_grpc=True,
            ),
            collection_name="resumes",
            embedding=llm.embedder,
        ),
    )
    async_engine = create_async_engine(
        database_sql_connection_string(Dialect.POSTGRES).replace(
            "postgresql://", "postgresql+asyncpg://", 1
        )
    )
    self._components["repository.resume.sql"] = SQLAlchemyResumeRepository(
        engine=async_engine
    )
    self._components["repository.ingestion_run.sql"] = SqlAlchemyIngestionRunRepository(
        engine=async_engine
    )
    self._components["repository.ingestion_source.sql"] = SqlAlchemyIngestionSourceRepository(
        engine=async_engine
    )
