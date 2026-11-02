from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend.resume_ingestion.internal.registry.init import DomainRegistry

import streamlit as st
from langchain_openai import ChatOpenAI

from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.init import (
    LanggraphResumeService,
)
from backend.resume_ingestion.internal.domain.core.resume.service.langgraph.schema import (
    LLM
)

class Services:
    def registerServices(self: DomainRegistry) -> None:
        llm=LLM(
            parser=ChatOpenAI(
                model="openai/gpt-4.1-mini",
                api_key=st.secrets["OPENROUTER_API_KEY"],
                base_url=st.secrets["OPENROUTER_API_BASE"],
                temperature=0.0,
            )
        )
        langgraph_resume_service = LanggraphResumeService(llm=llm)
        self._components["service.resume.langgraph"] = langgraph_resume_service