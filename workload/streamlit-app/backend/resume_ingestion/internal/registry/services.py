from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .init import DomainRegistry

import streamlit as st
from langchain_openai import ChatOpenAI

from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.init import (
    LanggraphResumeService,
)
from backend.resume_ingestion.internal.domain.core.service.resume.langgraph.schema import (
    LLM
)

def registerServices(self: DomainRegistry) -> None:
    llm = LLM(
        parser=ChatOpenAI(
            model="openai/gpt-4.1-mini",
            api_key=st.secrets["OPENROUTER_API_KEY"],
            base_url=st.secrets["OPENROUTER_API_BASE"],
            temperature=0.0,
        ),
    )
    vector_resume_repository = self._components["repository.resume.vector"]
    sql_resume_repository = self._components["repository.resume.sql"]
    sql_ingestion_run_repository = self._components["repository.ingestion_run.sql"]
    sql_ingestion_source_repository = self._components["repository.ingestion_source.sql"]
    self._components["service.resume"] = LanggraphResumeService(
        llm=llm,
        vector_resume_repository=vector_resume_repository,
        sql_resume_repository=sql_resume_repository,
        sql_ingestion_run_repository=sql_ingestion_run_repository,
        sql_ingestion_source_repository=sql_ingestion_source_repository,
    )