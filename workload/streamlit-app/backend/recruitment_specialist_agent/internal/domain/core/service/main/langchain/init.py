from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langsmith import Client
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from langgraph.checkpoint.memory import InMemorySaver
from qdrant_client import QdrantClient
from sqlalchemy import create_engine
import streamlit as st

from backend.recruitment_specialist_agent.internal.domain.core.service.main.langchain.tool.search_resume_vector import search_resume_vector
from backend.recruitment_specialist_agent.internal.pkg.config.config_database_sql import (
    database_sql_connection_string,
)
from backend.recruitment_specialist_agent.internal.pkg.constant.dialect import Dialect
from .run import run

langsmith_client = Client()

class LangchainMainAgentService:
    run = run

    def __init__(self):
        self.llm_chat = ChatOpenAI(
            model="qwen/qwen3.7-flash",
            api_key=st.secrets["OPENROUTER_API_KEY"],
            base_url=st.secrets["OPENROUTER_API_BASE"],
            reasoning_effort="low",
            temperature=0.0,
        )
        self.llm_embbeder = OpenAIEmbeddings(
            model="openai/text-embedding-3-small",
            api_key=st.secrets["OPENROUTER_API_KEY"],
            base_url=st.secrets["OPENROUTER_API_BASE"],
        )
        self.sql_engine = create_engine(
            database_sql_connection_string(Dialect.POSTGRES),
            pool_pre_ping=True,
        )
        self.vectorstore = QdrantVectorStore(
            client=QdrantClient(
                url=st.secrets["PRIMARY_QDRANT_DATABASE_VECTOR_URL"],
                api_key=st.secrets["PRIMARY_QDRANT_DATABASE_SQL_API_KEY"] if "PRIMARY_QDRANT_DATABASE_SQL_API_KEY" in st.secrets else None,
                prefer_grpc=True,
            ),
            collection_name="resumes",
            embedding=self.llm_embbeder,
        )
        self.tools = [
            search_resume_vector,
        ] + SQLDatabaseToolkit(
            db=SQLDatabase(self.sql_engine, include_tables=["resumes"]),
            llm=self.llm_chat
        ).get_tools()
        self.agent = create_agent(
            model=self.llm_chat,
            tools=self.tools,
            system_prompt=langsmith_client.pull_prompt("recruitment-specialist-agent").format(),
            checkpointer=InMemorySaver(),
        )
